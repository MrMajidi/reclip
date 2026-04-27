import glob
import json
import mimetypes
import os
import queue
import random
import subprocess
import tempfile
import threading
import time
import uuid
from urllib.parse import urlparse
from urllib.request import urlopen

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
YTDLP_COOKIES_MODE = os.environ.get("YTDLP_COOKIES_MODE", "auto").strip().lower()
YTDLP_BROWSER_NAME = os.environ.get("YTDLP_BROWSER_NAME", "chromium").strip()
YTDLP_COOKIES_DIR = os.environ.get("YTDLP_COOKIES_DIR", os.path.join(os.path.dirname(__file__), "cookies"))
PROXY_LIST_URL = os.environ.get("PROXY_LIST_URL", "https://proxy.webshare.io/api/v2/proxy/list/download/lwclhnsrgfkyxvbiqptqmhjbkqihgvmwgyhkwlai/-/any/username/direct/-/?plan_id=13259069").strip()
YTDLP_EXTRACTOR_RETRIES = max(1, int(os.environ.get("YTDLP_EXTRACTOR_RETRIES", "3")))
YTDLP_RETRY_SLEEP = os.environ.get("YTDLP_RETRY_SLEEP", "extractor:exp=5:60").strip()
YOUTUBE_REQUEST_SPACING_SECONDS = max(0.0, float(os.environ.get("YOUTUBE_REQUEST_SPACING_SECONDS", "6")))
YOUTUBE_SLEEP_REQUESTS_SECONDS = max(0.0, float(os.environ.get("YOUTUBE_SLEEP_REQUESTS_SECONDS", "1.0")))
YOUTUBE_SLEEP_INTERVAL_SECONDS = max(0.0, float(os.environ.get("YOUTUBE_SLEEP_INTERVAL_SECONDS", "5.0")))
YOUTUBE_MAX_SLEEP_INTERVAL_SECONDS = max(
    YOUTUBE_SLEEP_INTERVAL_SECONDS,
    float(os.environ.get("YOUTUBE_MAX_SLEEP_INTERVAL_SECONDS", "8.0")),
)
YOUTUBE_RATE_LIMIT_COOLDOWN_SECONDS = max(60, int(os.environ.get("YOUTUBE_RATE_LIMIT_COOLDOWN_SECONDS", "3600")))

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

jobs = {}
jobs_lock = threading.Lock()
download_queue = queue.Queue(maxsize=MAX_DOWNLOAD_QUEUE_SIZE)
info_cache = {}
info_cache_lock = threading.Lock()
info_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT_INFO_REQUESTS)
youtube_rate_limit_lock = threading.Lock()
cookie_rotation_lock = threading.Lock()
cookie_file_cooldowns = {}
cookie_rotation_index = 0
proxy_list = []
proxy_list_lock = threading.Lock()
youtube_last_request_started_at = 0.0
youtube_cooldown_until = 0.0


class YoutubeRateLimitError(Exception):
    pass


def get_json_payload():
    return request.get_json(silent=True) or {}


def is_youtube_url(url):
    hostname = (urlparse(url).hostname or "").lower()
    return hostname.endswith("youtube.com") or hostname.endswith("youtu.be") or hostname.endswith("youtube-nocookie.com")


def list_rotation_cookie_files():
    if not os.path.isdir(YTDLP_COOKIES_DIR):
        return []

    cookie_files = []
    for name in sorted(os.listdir(YTDLP_COOKIES_DIR)):
        if name.startswith("."):
            continue
        path = os.path.join(YTDLP_COOKIES_DIR, name)
        if os.path.isfile(path):
            cookie_files.append(path)
    return cookie_files


def fetch_proxy_list():
    global proxy_list
    if not PROXY_LIST_URL:
        return
    try:
        with urlopen(PROXY_LIST_URL, timeout=15) as resp:
            text = resp.read().decode("utf-8")
        parsed = []
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(":")
            if len(parts) == 4:
                host, port, user, pwd = parts
                parsed.append(f"http://{user}:{pwd}@{host}:{port}")
        with proxy_list_lock:
            proxy_list = parsed
        print(f"[proxy] Loaded {len(parsed)} proxies")
    except Exception as e:
        print(f"[proxy] Failed to fetch proxy list: {e}")


def get_random_proxy():
    with proxy_list_lock:
        if not proxy_list:
            return None
        return random.choice(proxy_list)


def build_cookie_source(source_type, *, path=None, browser_name=None):
    source = {"type": source_type, "path": path, "browser_name": browser_name}
    if source_type == "file" and path:
        source["args"] = ["--cookies", path]
    elif source_type == "browser" and browser_name:
        source["args"] = ["--cookies-from-browser", browser_name]
    else:
        source["args"] = []
    return source


def get_rotating_cookie_source(url):
    global cookie_rotation_index

    cookie_files = list_rotation_cookie_files()
    if not cookie_files:
        return None

    youtube_request = is_youtube_url(url)
    now = time.time()

    with cookie_rotation_lock:
        active_files = []
        next_ready_at = None

        for path in cookie_files:
            ready_at = cookie_file_cooldowns.get(path, 0.0)
            if not youtube_request or ready_at <= now:
                active_files.append(path)
            else:
                next_ready_at = ready_at if next_ready_at is None else min(next_ready_at, ready_at)

        stale_paths = [path for path in list(cookie_file_cooldowns) if path not in cookie_files]
        for path in stale_paths:
            cookie_file_cooldowns.pop(path, None)

        if not active_files:
            wait_seconds = max(1, int((next_ready_at or now) - now))
            minutes = max(1, int((wait_seconds + 59) // 60))
            raise YoutubeRateLimitError(
                "All rotated YouTube cookies are cooling down right now. "
                f"Please wait about {minutes} minute(s) or add more cookie files."
            )

        selected_index = cookie_rotation_index % len(active_files)
        selected_path = active_files[selected_index]
        cookie_rotation_index = (cookie_rotation_index + 1) % len(active_files)
        return build_cookie_source("file", path=selected_path)


def resolve_cookie_source(url):
    mode = YTDLP_COOKIES_MODE
    if mode == "off":
        return build_cookie_source("none")
    if mode == "browser":
        return build_cookie_source("browser", browser_name=YTDLP_BROWSER_NAME)

    rotating_source = get_rotating_cookie_source(url)
    if rotating_source:
        return rotating_source

    if mode == "file":
        if os.path.exists(COOKIES_FILE):
            return build_cookie_source("file", path=COOKIES_FILE)
        return build_cookie_source("none")

    if mode == "auto":
        if os.path.exists(COOKIES_FILE):
            return build_cookie_source("file", path=COOKIES_FILE)
        if is_youtube_url(url):
            return build_cookie_source("none")
        return build_cookie_source("browser", browser_name=YTDLP_BROWSER_NAME)

    if os.path.exists(COOKIES_FILE):
        return build_cookie_source("file", path=COOKIES_FILE)
    return build_cookie_source("none")


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


def is_youtube_rate_limit_message(message):
    message = (message or "").lower()
    return (
        "rate-limited by youtube" in message
        or "this content isn't available, try again later" in message
        or "sign in to confirm you’re not a bot" in message
        or "sign in to confirm you're not a bot" in message
    )


def get_youtube_cooldown_error(now=None):
    current_time = now or time.time()
    with youtube_rate_limit_lock:
        remaining = youtube_cooldown_until - current_time
    if remaining <= 0:
        return None

    minutes = max(1, int((remaining + 59) // 60))
    return (
        "YouTube is rate-limiting this account/IP right now. "
        f"Please wait about {minutes} minute(s), reduce request volume, or disable browser cookies for YouTube."
    )


def mark_youtube_rate_limited():
    global youtube_cooldown_until

    with youtube_rate_limit_lock:
        youtube_cooldown_until = max(youtube_cooldown_until, time.time() + YOUTUBE_RATE_LIMIT_COOLDOWN_SECONDS)


def wait_for_youtube_request_slot(url):
    global youtube_last_request_started_at

    if not is_youtube_url(url):
        return

    cooldown_error = get_youtube_cooldown_error()
    if cooldown_error:
        raise YoutubeRateLimitError(cooldown_error)

    with youtube_rate_limit_lock:
        now = time.time()
        sleep_for = max(0.0, (youtube_last_request_started_at + YOUTUBE_REQUEST_SPACING_SECONDS) - now)
        if sleep_for > 0:
            time.sleep(sleep_for)
        youtube_last_request_started_at = time.time()


def mark_cookie_source_rate_limited(cookie_source):
    if not cookie_source or cookie_source.get("type") != "file" or not cookie_source.get("path"):
        return

    with cookie_rotation_lock:
        cookie_file_cooldowns[cookie_source["path"]] = time.time() + YOUTUBE_RATE_LIMIT_COOLDOWN_SECONDS


def build_base_yt_dlp_cmd(url, cookie_source):
    proxy = get_random_proxy()
    cmd = [
        "yt-dlp",
        "--no-playlist",
        "--no-warnings",
        "--extractor-retries",
        str(YTDLP_EXTRACTOR_RETRIES),
        "--retry-sleep",
        YTDLP_RETRY_SLEEP,
        *cookie_source["args"],
    ]
    if proxy:
        cmd += ["--proxy", proxy]
    return cmd


def maybe_apply_youtube_rate_limit(result, url, cookie_source):
    if not is_youtube_url(url) or result.returncode == 0 or not is_youtube_rate_limit_message(result.stderr):
        return

    if cookie_source.get("type") == "file":
        mark_cookie_source_rate_limited(cookie_source)

    if cookie_source.get("type") != "file" or "not a bot" in result.stderr.lower():
        mark_youtube_rate_limited()


def run_yt_dlp_command(cmd, *, url, timeout, cookie_source, capture_stdout=True):
    wait_for_youtube_request_slot(url)

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE if capture_stdout else subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )
    maybe_apply_youtube_rate_limit(result, url, cookie_source)
    return result


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
        try:
            cookie_source = resolve_cookie_source(url)
            out_template = os.path.join(temp_dir, f"{job_id}.%(ext)s")
            cmd = [
                *build_base_yt_dlp_cmd(url, cookie_source),
                "--no-progress",
                "-o",
                out_template,
            ]

            if is_youtube_url(url):
                cmd += [
                    "--sleep-requests",
                    str(YOUTUBE_SLEEP_REQUESTS_SECONDS),
                    "--sleep-interval",
                    str(YOUTUBE_SLEEP_INTERVAL_SECONDS),
                    "--max-sleep-interval",
                    str(YOUTUBE_MAX_SLEEP_INTERVAL_SECONDS),
                ]

            if format_choice == "audio":
                cmd += ["-x", "--audio-format", "mp3"]
            elif format_id:
                cmd += ["-f", f"{format_id}+bestaudio/{format_id}/bestvideo+bestaudio/best", "--merge-output-format", "mp4"]
            else:
                cmd += ["-f", "bestvideo+bestaudio/best", "--merge-output-format", "mp4"]

            cmd.append(url)

            result = run_yt_dlp_command(
                cmd,
                url=url,
                timeout=DOWNLOAD_TIMEOUT,
                cookie_source=cookie_source,
                capture_stdout=False,
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
        except YoutubeRateLimitError as exc:
            update_job(job_id, status="error", error=str(exc), finished_at=time.time())
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


fetch_proxy_list()
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

    try:
        cookie_source = resolve_cookie_source(url)
        cmd = [*build_base_yt_dlp_cmd(url, cookie_source), "-J", url]
        if is_youtube_url(url):
            cmd += ["--sleep-requests", str(YOUTUBE_SLEEP_REQUESTS_SECONDS)]

        result = run_yt_dlp_command(cmd, url=url, timeout=INFO_TIMEOUT, cookie_source=cookie_source)
        if result.returncode != 0:
            error_message = extract_error(result.stderr)
            status_code = 429 if is_youtube_rate_limit_message(error_message) else 400
            return jsonify({"error": error_message}), status_code

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
    except YoutubeRateLimitError as exc:
        return jsonify({"error": str(exc)}), 429
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
