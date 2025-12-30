"""
pipeline.py — TikTok (Apify) → score → download winner → brand (ffmpeg) → upload to Google Drive
+ Deduping via a Drive-stored seen.json (so you don't repost the same TikTok video ID twice)

ENV VARS (set as GitHub Actions secrets/env):
- APIFY_TOKEN
- APIFY_ACTOR_ID                  (e.g. GdWCkxBtKWOsKjdch)
- GDRIVE_FOLDER_ID                (the target "instagram" folder ID — ideally inside a Shared Drive)
- GDRIVE_SERVICE_ACCOUNT_JSON     (the full JSON string)
Optional:
- HASHTAG                         (default: oddlysatisfying)
- VIDEOS_PER_RUN                  (default: 50)
- MIN_VIEWS                       (default: 10000)
- MAX_AGE_DAYS                    (default: 7)
- DRY_RUN                         ("1" to stop before downloading/branding/uploading)
"""

import io
import json
import math
import os
import shutil
import subprocess
import time
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Set

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload


# -----------------------
# Config
# -----------------------
APIFY_TOKEN = os.environ["APIFY_TOKEN"]
APIFY_ACTOR_ID = os.environ.get("APIFY_ACTOR_ID", "GdWCkxBtKWOsKjdch")

HASHTAG = os.environ.get("HASHTAG", "oddlysatisfying")
VIDEOS_PER_RUN = int(os.environ.get("VIDEOS_PER_RUN", "50"))
MIN_VIEWS = int(os.environ.get("MIN_VIEWS", "10000"))
MAX_AGE_DAYS = int(os.environ.get("MAX_AGE_DAYS", "7"))

GDRIVE_FOLDER_ID = os.environ["GDRIVE_FOLDER_ID"]
SA_JSON = os.environ["GDRIVE_SERVICE_ACCOUNT_JSON"]

DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

SEEN_FILENAME = "seen.json"
LOGO_PATH = "logo.png"


# -----------------------
# Helpers
# -----------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def hours_since(iso: str) -> float:
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return (now_utc() - dt).total_seconds() / 3600


def dated_mp4_name() -> str:
    return f"{date.today().isoformat()}.mp4"


def require(condition: bool, msg: str) -> None:
    if not condition:
        raise RuntimeError(msg)


# -----------------------
# Apify
# -----------------------
def apify_start_run(actor_id: str, payload: dict) -> str:
    """
    Start an Apify actor run. We use /runs (async create) because it reliably returns Location header.
    """
    resp = requests.post(
        f"https://api.apify.com/v2/acts/{actor_id}/runs",
        params={"token": APIFY_TOKEN},
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    loc = resp.headers.get("Location")
    require(bool(loc), f"No Location header from Apify. status={resp.status_code} body={resp.text[:500]}")
    return loc.rstrip("/").split("/")[-1]


def apify_wait_run(run_id: str) -> dict:
    """
    Poll run status until finished. Return run data if SUCCEEDED, otherwise raise with tail of logs.
    """
    while True:
        run = requests.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}",
            params={"token": APIFY_TOKEN},
            timeout=60,
        ).json()["data"]

        status = run["status"]
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            if status != "SUCCEEDED":
                log = requests.get(
                    f"https://api.apify.com/v2/actor-runs/{run_id}/log",
                    params={"token": APIFY_TOKEN, "offset": 0, "limit": 4000},
                    timeout=60,
                ).text
                raise RuntimeError(
                    f"Apify run did not succeed. status={status}\n"
                    f"statusMessage={run.get('statusMessage')}\n\n"
                    f"Log tail:\n{log[-2000:]}"
                )
            return run

        time.sleep(3)


def apify_get_dataset_items(dataset_id: str) -> List[Dict[str, Any]]:
    r = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items",
        params={"clean": "true", "token": APIFY_TOKEN},
        timeout=120,
    )
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and "items" in data:
        return data["items"]
    if isinstance(data, list):
        return data
    raise TypeError(f"Unexpected dataset payload type: {type(data)}")


# -----------------------
# Drive (Service Account)
# -----------------------
def build_drive_service():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(SA_JSON),
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def drive_find_file_id(service, folder_id: str, filename: str) -> Optional[str]:
    q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    res = service.files().list(
        q=q,
        fields="files(id,name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None


def drive_download_text(service, file_id: str) -> str:
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue().decode("utf-8")


def drive_upload_text(service, folder_id: str, filename: str, text: str, existing_file_id: Optional[str]) -> str:
    media = MediaIoBaseUpload(io.BytesIO(text.encode("utf-8")), mimetype="application/json", resumable=False)
    if existing_file_id:
        service.files().update(
            fileId=existing_file_id,
            media_body=media,
            body={"name": filename},
            supportsAllDrives=True,
        ).execute()
        return existing_file_id

    created = service.files().create(
        body={"name": filename, "parents": [folder_id]},
        media_body=media,
        fields="id",
        supportsAllDrives=True,
    ).execute()
    return created["id"]


def drive_upload_mp4(service, file_path: str, folder_id: str) -> str:
    filename = os.path.basename(file_path)
    media = MediaFileUpload(file_path, mimetype="video/mp4", resumable=True)
    created = service.files().create(
        body={"name": filename, "parents": [folder_id]},
        media_body=media,
        fields="id, name",
        supportsAllDrives=True,
    ).execute()
    return created["id"]


def load_seen_ids(service, folder_id: str) -> (Set[str], Optional[str]):
    """
    Returns (seen_ids_set, file_id_of_seen_json_or_None)
    If seen.json doesn't exist yet, returns empty set and None.
    """
    file_id = drive_find_file_id(service, folder_id, SEEN_FILENAME)
    if not file_id:
        return set(), None

    try:
        raw = drive_download_text(service, file_id)
        arr = json.loads(raw)
        if isinstance(arr, list):
            return set(str(x) for x in arr if x), file_id
        return set(), file_id
    except Exception:
        # If corrupted, start fresh but keep file id so we overwrite.
        return set(), file_id


# -----------------------
# Scoring / selection
# -----------------------
def score_candidates(items: List[Dict[str, Any]], seen_ids: Set[str]) -> List[Dict[str, Any]]:
    """
    Filter + score. Returns scored items.
    Works with fields you confirmed: createTimeISO, playCount, diggCount, shareCount, webVideoUrl, id
    """
    filtered: List[Dict[str, Any]] = []
    max_age_hours = 24 * MAX_AGE_DAYS

    for v in items:
        if not isinstance(v, dict):
            continue

        vid = str(v.get("id", "")).strip()
        if vid and vid in seen_ids:
            continue

        created = v.get("createTimeISO")
        if not created:
            continue

        try:
            age_h = hours_since(created)
        except Exception:
            continue

        if age_h > max_age_hours:
            continue

        views = v.get("playCount", 0) or 0
        if views < MIN_VIEWS:
            continue

        # Score: velocity * engagement, dampened by log views
        likes = v.get("diggCount", 0) or 0
        shares = v.get("shareCount", 0) or 0

        vph = views / max(age_h, 1)
        er = (likes / views) if views else 0
        sr = (shares / views) if views else 0

        v["score"] = math.log10(views + 1) * vph * (er + 2 * sr + 1e-6)
        filtered.append(v)

    return filtered


def pick_winner(scored: List[Dict[str, Any]]) -> Dict[str, Any]:
    require(bool(scored), "No candidates after filtering/scoring. Increase VIDEOS_PER_RUN or lower MIN_VIEWS / MAX_AGE_DAYS.")
    return max(scored, key=lambda x: x.get("score", 0))


# -----------------------
# Download + brand
# -----------------------
def download_mp4_from_run2_dataset(dataset_id_2: str, out_path: str = "input.mp4") -> None:
    """
    Your actor provides a downloadable Apify KVS link in item['mediaUrls'][0] ending in .mp4.
    """
    items = apify_get_dataset_items(dataset_id_2)
    require(bool(items), "Run #2 dataset empty.")

    item = items[0]
    media_urls = item.get("mediaUrls") or []
    if isinstance(media_urls, str):
        media_urls = [media_urls]

    mp4_url = next((u for u in media_urls if isinstance(u, str) and u.lower().endswith(".mp4")), None)
    if not mp4_url and media_urls:
        mp4_url = media_urls[0]

    require(bool(mp4_url), f"No mediaUrls mp4 found. Keys={list(item.keys())}")

    # Apify storage record URLs typically need token
    r = requests.get(mp4_url, params={"token": APIFY_TOKEN}, stream=True, timeout=180)
    r.raise_for_status()

    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)


def brand_with_ffmpeg(input_mp4: str, logo_png: str, output_mp4: str) -> None:
    require(bool(shutil.which("ffmpeg")), "ffmpeg not found on PATH.")
    require(os.path.exists(logo_png), f"Missing logo file: {logo_png}")

    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            input_mp4,
            "-i",
            logo_png,
            "-filter_complex",
            # faint logo bottom-right, ~12% width, 24px margins
            "[1:v]colorchannelmixer=aa=0.18,scale=iw*0.12:-1[logo];"
            "[0:v][logo]overlay=W-w-24:H-h-24:format=auto,format=yuv420p",
            "-c:a",
            "copy",
            output_mp4,
        ],
        check=True,
    )


# -----------------------
# Main
# -----------------------
def main() -> None:
    drive = build_drive_service()

    # Dedup store: Drive file "seen.json" inside your target folder.
    # You do NOT need to create it manually — this script will create it the first time.
    seen_ids, seen_file_id = load_seen_ids(drive, GDRIVE_FOLDER_ID)
    print(f"Seen IDs loaded: {len(seen_ids)}")

    # Run #1: scrape hashtag
    # Your actor uses "hashtags" and "numberOfVideos" (per your Apify UI).
    run1_payload = {
        "hashtags": [HASHTAG],
        "numberOfVideos": VIDEOS_PER_RUN,
    }
    run1_id = apify_start_run(APIFY_ACTOR_ID, run1_payload)
    run1 = apify_wait_run(run1_id)
    dataset1 = run1["defaultDatasetId"]

    items = apify_get_dataset_items(dataset1)
    print(f"Run1 dataset: {dataset1} items: {len(items)}")

    scored = score_candidates(items, seen_ids)
    print(f"Candidates after filter+dedupe: {len(scored)}")
    winner = pick_winner(scored)

    winner_url = winner["webVideoUrl"]
    winner_id = str(winner.get("id", "")).strip()
    print("Winner ID:", winner_id)
    print("Winner URL:", winner_url)
    print("Winner score:", winner.get("score"))
    print("Winner playCount:", winner.get("playCount"))

    if DRY_RUN:
        print("DRY_RUN=1 → stopping before download/brand/upload.")
        return

    # Run #2: download single post
    run2_payload = {
        "postURLs": [winner_url],
        "shouldDownloadVideos": True,
    }
    run2_id = apify_start_run(APIFY_ACTOR_ID, run2_payload)
    run2 = apify_wait_run(run2_id)
    dataset2 = run2["defaultDatasetId"]
    print(f"Run2 dataset: {dataset2}")

    download_mp4_from_run2_dataset(dataset2, "input.mp4")
    require(os.path.exists("input.mp4"), "input.mp4 missing after download step.")

    out_name = dated_mp4_name()
    brand_with_ffmpeg("input.mp4", LOGO_PATH, out_name)
    require(os.path.exists(out_name), f"{out_name} missing after ffmpeg step.")

    # Upload MP4 to Drive
    drive_id = drive_upload_mp4(drive, out_name, GDRIVE_FOLDER_ID)
    print("Uploaded MP4 to Drive. fileId:", drive_id, "name:", out_name)

    # Update seen.json (dedupe) AFTER successful upload
    if winner_id:
        seen_ids.add(winner_id)
        seen_text = json.dumps(sorted(seen_ids), ensure_ascii=False, indent=2)
        seen_file_id = drive_upload_text(drive, GDRIVE_FOLDER_ID, SEEN_FILENAME, seen_text, existing_file_id=seen_file_id)
        print("Updated seen.json fileId:", seen_file_id, "added:", winner_id)
    else:
        print("Warning: winner has no 'id' field; cannot dedupe this run.")


if __name__ == "__main__":
    main()
