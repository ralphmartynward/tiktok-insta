import os, time, json, math, subprocess, shutil
import requests
from datetime import datetime, timezone

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from datetime import date

def dated_filename():
    return f"{date.today().isoformat()}.mp4"  # YYYY-MM-DD.mp4



APIFY_TOKEN = os.environ["APIFY_TOKEN"]
APIFY_ACTOR_ID = os.environ.get("APIFY_ACTOR_ID", "GdWCkxBtKWOsKjdch")
HASHTAG = os.environ.get("HASHTAG", "oddlysatisfying")
VIDEOS_PER_RUN = int(os.environ.get("VIDEOS_PER_RUN", "50"))
GDRIVE_FOLDER_ID = os.environ["GDRIVE_FOLDER_ID"]
SA_JSON = os.environ["GDRIVE_SERVICE_ACCOUNT_JSON"]


def hours_since(iso):
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - dt).total_seconds() / 3600


def apify_start_run(actor_id: str, payload: dict) -> str:
    """Start run and return run_id via Location header (robust)."""
    resp = requests.post(
        f"https://api.apify.com/v2/acts/{actor_id}/runs",
        params={"token": APIFY_TOKEN},
        json=payload,
    )
    resp.raise_for_status()
    loc = resp.headers.get("Location")
    if not loc:
        raise RuntimeError(f"No Location header. Status={resp.status_code}, body={resp.text[:500]}")
    return loc.rstrip("/").split("/")[-1]


def apify_wait_run(run_id: str) -> dict:
    """Wait until run finishes and return run data."""
    while True:
        run = requests.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}",
            params={"token": APIFY_TOKEN},
        ).json()["data"]

        if run["status"] in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            if run["status"] != "SUCCEEDED":
                log = requests.get(
                    f"https://api.apify.com/v2/actor-runs/{run_id}/log",
                    params={"token": APIFY_TOKEN, "offset": 0, "limit": 2000},
                ).text
                raise RuntimeError(f"Run failed: {run['status']} {run.get('statusMessage')}\n{log[-1500:]}")
            return run
        time.sleep(3)


def apify_get_dataset_items(dataset_id: str) -> list:
    r = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items",
        params={"clean": "true", "token": APIFY_TOKEN},
    )
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and "items" in data:
        return data["items"]
    return data


def score_and_pick(items: list) -> dict:
    # filter: 7 days + at least 10k views
    filtered = []
    for v in items:
        if not isinstance(v, dict):
            continue
        if not v.get("createTimeISO"):
            continue
        if hours_since(v["createTimeISO"]) > 24 * 7:
            continue
        if v.get("playCount", 0) < 10_000:
            continue
        filtered.append(v)

    if not filtered:
        raise RuntimeError("No candidates after filtering. Lower thresholds or increase sample size.")

    for v in filtered:
        age = max(hours_since(v["createTimeISO"]), 1)
        views = v["playCount"]
        likes = v.get("diggCount", 0)
        shares = v.get("shareCount", 0)

        vph = views / age
        er = likes / views if views else 0
        sr = shares / views if views else 0

        v["score"] = math.log10(views + 1) * vph * (er + 2 * sr + 1e-6)

    return max(filtered, key=lambda x: x["score"])


def download_mp4_from_run2_dataset(dataset_id_2: str, out_path: str = "input.mp4") -> None:
    items = apify_get_dataset_items(dataset_id_2)
    if not items:
        raise RuntimeError("Run #2 dataset empty")

    item = items[0]
    media_urls = item.get("mediaUrls") or []
    if isinstance(media_urls, str):
        media_urls = [media_urls]

    mp4_url = next((u for u in media_urls if str(u).lower().endswith(".mp4")), None) or (media_urls[0] if media_urls else None)
    if not mp4_url:
        raise RuntimeError(f"No mediaUrls mp4 found. Keys={list(item.keys())}")

    # Apify storage often needs token
    r = requests.get(mp4_url, params={"token": APIFY_TOKEN}, stream=True)
    r.raise_for_status()

    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)


def brand_with_ffmpeg(input_mp4: str, logo_png: str, output_mp4: str) -> None:
    if not shutil.which("ffmpeg"):
        raise RuntimeError("ffmpeg not found on PATH")
    if not os.path.exists(logo_png):
        raise RuntimeError(f"Missing logo: {logo_png}")

    subprocess.run([
        "ffmpeg","-y",
        "-i", input_mp4,
        "-i", logo_png,
        "-filter_complex",
        "[1:v]colorchannelmixer=aa=0.18,scale=iw*0.12:-1[logo];[0:v][logo]overlay=W-w-24:H-h-24:format=auto,format=yuv420p",
        "-c:a","copy",
        output_mp4
    ], check=True)


def upload_to_drive(file_path: str, folder_id: str) -> str:
    creds = service_account.Credentials.from_service_account_info(
        json.loads(SA_JSON),
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )
    service = build("drive", "v3", credentials=creds)

    filename = os.path.basename(file_path)
    media = MediaFileUpload(file_path, mimetype="video/mp4", resumable=True)

    file_metadata = {
        "name": filename,
        "parents": [folder_id],
    }
    created = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, name"
    ).execute()

    return created["id"]


def main():
    # Run #1: hashtag scrape (optional; you can skip if you already run it on schedule)
    run1_payload = {
        "hashtags": [HASHTAG],
        "resultsPerPage": VIDEOS_PER_RUN,
    }
    run1_id = apify_start_run(APIFY_ACTOR_ID, run1_payload)
    run1 = apify_wait_run(run1_id)
    dataset1 = run1["defaultDatasetId"]

    items = apify_get_dataset_items(dataset1)
    winner = score_and_pick(items)
    winner_url = winner["webVideoUrl"]

    # Run #2: download single post
    run2_payload = {
        "postURLs": [winner_url],
        "shouldDownloadVideos": True,
    }
    run2_id = apify_start_run(APIFY_ACTOR_ID, run2_payload)
    run2 = apify_wait_run(run2_id)
    dataset2 = run2["defaultDatasetId"]

    download_mp4_from_run2_dataset(dataset2, "input.mp4")
    dated = dated_filename()
    brand_with_ffmpeg("input.mp4", "logo.png", dated)
    drive_id = upload_to_drive(dated, GDRIVE_FOLDER_ID)



if __name__ == "__main__":
    main()
