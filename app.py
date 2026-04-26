import glob
import json
import mimetypes
import os
import queue
import subprocess
import tempfile
import threading
import time
import uuid

import boto3
from boto3.s3.transfer import TransferConfig
from flask import Flask, jsonify, redirect, render_template, request

app = Flask(__name__)

S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
R2_ENDPOINT_URL = os.environ.get("R2_ENDPOINT_URL")
R2_CUSTOM_DOMAIN = os.environ.get("R2_CUSTOM_DOMAIN", "").rstrip("/")

CLEANUP_INTERVAL = int(os.environ.get("CLEANUP_INTERVAL", "600"))
FILE_RETENTION_TIME = int(os.environ.get("FILE_RETENTION_TIME", "3600"))
INFO_CACHE_TTL = int(os.environ.get("INFO_CACHE_TTL", "120"))
DOWNLOAD_TIMEOUT = int(os.environ.get("DOWNLOAD_TIMEOUT", "300"))
INFO_TIMEOUT = int(os.environ.get("INFO_TIMEOUT", "60"))
DOWNLOAD_WORKERS = max(1, int(os.environ.get("DOWNLOAD_WORKERS", "4")))
MAX_DOWNLOAD_QUEUE_SIZE = max(1, int(os.environ.get("MAX_DOWNLOAD_QUEUE_SIZE", "32")))
MAX_CONCURRENT_INFO_REQUESTS = max(1, int(os.environ.get("MAX_CONCURRENT_INFO_REQUESTS", "8")))
S3_MULTIPART_CHUNK_SIZE_MB = max(5, int(os.environ.get("S3_MULTIPART_CHUNK_SIZE_MB", "8")))

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    endpoint_url=R2_ENDPOINT_URL,
)
s3_transfer_config = TransferConfig(
    multipart_threshold=S3_MULTIPART_CHUNK_SIZE_MB * 1024 * 1024,
    multipart_chunksize=S3_MULTIPART_CHUNK_SIZE_MB * 1024 * 1024,
    use_threads=False,
)

COOKIES_FILE = os.path.join(os.path.dirname(__file__), "cookies.txt")
COOKIES_CMD = ["--cookies", COOKIES_FILE] if os.path.exists(COOKIES_FILE) else ["--cookies-from-browser", "chromium"]

jobs = {}
jobs_lock = threading.Lock()
download_queue = queue.Queue(maxsize=MAX_DOWNLOAD_QUEUE_SIZE)
info_cache = {}
info_cache_lock = threading.Lock()
info_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT_INFO_REQUESTS)


def get_json_payload():
    return request.get_json(silent=True) or {}


def create_job(job_id, **fields):
    with jobs_lock:
        jobs[job_id] = fields.copy()


def update_job(job_id, **fields):
    with jobs_lock:
        job = jobs.get(job_id)
        if job is not None:
            job.update(fields)


def get_job(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        return job.copy() if job else None


def delete_job(job_id):
    with jobs_lock:
        jobs.pop(job_id, None)


def extract_error(stderr):
    for line in reversed(stderr.splitlines()):
        stripped = line.strip()
        if stripped:
            return stripped
    return "Request failed"


def sanitize_filename(title, fallback_name):
    title = (title or "").strip()
    if not title:
        return fallback_name

    safe_title = "".join(c for c in title if c not in r'\/:*?"<>|').strip()[:80].strip()
    return f"{safe_title}{os.path.splitext(fallback_name)[1]}" if safe_title else fallback_name


def build_s3_url(s3_key):
    if R2_CUSTOM_DOMAIN:
        return f"{R2_CUSTOM_DOMAIN}/{s3_key}"
    return s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET_NAME, "Key": s3_key},
        ExpiresIn=FILE_RETENTION_TIME,
    )


def cache_info(url, payload):
    with info_cache_lock:
        info_cache[url] = {
            "expires_at": time.time() + INFO_CACHE_TTL,
            "payload": payload,
        }


def get_cached_info(url):
    with info_cache_lock:
        cached = info_cache.get(url)
        if not cached:
            return None
        if cached["expires_at"] <= time.time():
            info_cache.pop(url, None)
            return None
        return cached["payload"]


def delete_s3_objects(keys):
    if not keys or not S3_BUCKET_NAME:
        return

    for start in range(0, len(keys), 1000):
        batch = keys[start:start + 1000]
        s3_client.delete_objects(
            Bucket=S3_BUCKET_NAME,
            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
        )


def cleanup_expired_bucket_objects(current_time):
    if not S3_BUCKET_NAME:
        return

    expired_keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix="downloads/"):
        for obj in page.get("Contents", []):
            if current_time - obj["LastModified"].timestamp() > FILE_RETENTION_TIME:
                expired_keys.append(obj["Key"])

    delete_s3_objects(expired_keys)


def cleanup_old_files():
    while True:
        try:
            time.sleep(CLEANUP_INTERVAL)
            current_time = time.time()
            cleanup_expired_bucket_objects(current_time)

            with info_cache_lock:
                expired_urls = [url for url, cached in info_cache.items() if cached["expires_at"] <= current_time]
                for url in expired_urls:
                    info_cache.pop(url, None)

            with jobs_lock:
                job_items = list(jobs.items())

            for job_id, job in job_items:
                finished_at = job.get("finished_at")
                if not finished_at or current_time - finished_at <= FILE_RETENTION_TIME:
                    continue

                delete_job(job_id)
        except Exception as exc:
            print(f"Cleanup job error: {exc}")


def find_downloaded_file(temp_dir, job_id, format_choice):
    files = glob.glob(os.path.join(temp_dir, f"{job_id}.*"))
    if not files:
        raise FileNotFoundError("Download completed but no file was found")

    preferred_ext = ".mp3" if format_choice == "audio" else ".mp4"
    target = [path for path in files if path.endswith(preferred_ext)]
    return target[0] if target else files[0]


def upload_file_to_s3(local_path, s3_key):
    extra_args = {}
    content_type = mimetypes.guess_type(local_path)[0]
    if content_type:
        extra_args["ContentType"] = content_type

    upload_kwargs = {"Config": s3_transfer_config}
    if extra_args:
        upload_kwargs["ExtraArgs"] = extra_args

    s3_client.upload_file(local_path, S3_BUCKET_NAME, s3_key, **upload_kwargs)


def run_download(job_id, url, format_choice, format_id):
    update_job(job_id, status="downloading", error=None)

    with tempfile.TemporaryDirectory() as temp_dir:
        out_template = os.path.join(temp_dir, f"{job_id}.%(ext)s")
        cmd = [
            "yt-dlp",
            "--no-playlist",
            "--no-progress",
            "--no-warnings",
            *COOKIES_CMD,
            "-o",
            out_template,
        ]

        if format_choice == "audio":
            cmd += ["-x", "--audio-format", "mp3"]
        elif format_id:
            cmd += ["-f", f"{format_id}+bestaudio/{format_id}/bestvideo+bestaudio/best", "--merge-output-format", "mp4"]
        else:
            cmd += ["-f", "bestvideo+bestaudio/best", "--merge-output-format", "mp4"]

        cmd.append(url)

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                timeout=DOWNLOAD_TIMEOUT,
            )
            if result.returncode != 0:
                update_job(
                    job_id,
                    status="error",
                    error=extract_error(result.stderr),
                    finished_at=time.time(),
                )
                return

            chosen = find_downloaded_file(temp_dir, job_id, format_choice)
            fallback_name = os.path.basename(chosen)
            job = get_job(job_id) or {}
            filename = sanitize_filename(job.get("title"), fallback_name)
            s3_key = f"downloads/{job_id}/{filename}"

            update_job(job_id, status="uploading")
            upload_file_to_s3(chosen, s3_key)

            update_job(
                job_id,
                status="done",
                s3_key=s3_key,
                s3_url=build_s3_url(s3_key),
                filename=filename,
                upload_time=time.time(),
                finished_at=time.time(),
                error=None,
            )
        except subprocess.TimeoutExpired:
            update_job(
                job_id,
                status="error",
                error=f"Download timed out ({DOWNLOAD_TIMEOUT // 60} min limit)",
                finished_at=time.time(),
            )
        except FileNotFoundError as exc:
            update_job(job_id, status="error", error=str(exc), finished_at=time.time())
        except Exception as exc:
            update_job(job_id, status="error", error=str(exc), finished_at=time.time())


def download_worker():
    while True:
        job_id, url, format_choice, format_id = download_queue.get()
        try:
            run_download(job_id, url, format_choice, format_id)
        finally:
            download_queue.task_done()


def start_background_workers():
    cleanup_thread = threading.Thread(target=cleanup_old_files, daemon=True)
    cleanup_thread.start()

    for worker_index in range(DOWNLOAD_WORKERS):
        worker = threading.Thread(target=download_worker, name=f"download-worker-{worker_index}", daemon=True)
        worker.start()


start_background_workers()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/info", methods=["POST"])
def get_info():
    data = get_json_payload()
    url = data.get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400

    cached = get_cached_info(url)
    if cached is not None:
        return jsonify(cached)

    if not info_semaphore.acquire(blocking=False):
        return jsonify({"error": "Server is busy fetching video info. Please retry in a moment."}), 429

    cmd = ["yt-dlp", "--no-playlist", "--no-warnings", *COOKIES_CMD, "-J", url]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=INFO_TIMEOUT)
        if result.returncode != 0:
            return jsonify({"error": extract_error(result.stderr)}), 400

        info = json.loads(result.stdout)

        best_by_height = {}
        for fmt in info.get("formats", []):
            height = fmt.get("height")
            vcodec = fmt.get("vcodec", "none")
            proto = fmt.get("protocol", "")
            if not height or vcodec == "none":
                continue
            if proto not in ("https", "http"):
                continue
            tbr = fmt.get("tbr") or 0
            if height not in best_by_height or tbr > (best_by_height[height].get("tbr") or 0):
                best_by_height[height] = fmt

        formats = [
            {
                "id": fmt["format_id"],
                "label": f"{height}p",
                "height": height,
            }
            for height, fmt in best_by_height.items()
        ]
        formats.sort(key=lambda item: item["height"], reverse=True)

        payload = {
            "title": info.get("title", ""),
            "thumbnail": info.get("thumbnail", ""),
            "duration": info.get("duration"),
            "uploader": info.get("uploader", ""),
            "formats": formats,
        }
        cache_info(url, payload)
        return jsonify(payload)
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Timed out fetching video info"}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400
    finally:
        info_semaphore.release()


@app.route("/api/download", methods=["POST"])
def start_download():
    data = get_json_payload()
    url = data.get("url", "").strip()
    format_choice = data.get("format", "video")
    format_id = data.get("format_id")
    title = data.get("title", "")

    if not url:
        return jsonify({"error": "No URL provided"}), 400

    job_id = uuid.uuid4().hex[:10]
    create_job(
        job_id,
        status="queued",
        url=url,
        title=title,
        format_choice=format_choice,
        format_id=format_id,
        created_at=time.time(),
        error=None,
    )

    try:
        download_queue.put_nowait((job_id, url, format_choice, format_id))
    except queue.Full:
        delete_job(job_id)
        return jsonify({"error": "Download queue is full. Please retry in a moment."}), 429

    return jsonify({"job_id": job_id, "status": "queued"})


@app.route("/api/status/<job_id>")
def check_status(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    return jsonify(
        {
            "status": job["status"],
            "error": job.get("error"),
            "filename": job.get("filename"),
        }
    )


@app.route("/api/file/<job_id>")
def download_file(job_id):
    job = get_job(job_id)
    if not job or job["status"] != "done":
        return jsonify({"error": "File not ready"}), 404
    return redirect(job["s3_url"])


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8899))
    host = os.environ.get("HOST", "127.0.0.1")
    app.run(host=host, port=port)
