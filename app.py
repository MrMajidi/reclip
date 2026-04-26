import json
import subprocess
import threading
import tempfile
import boto3
from flask import Flask, request, jsonify, redirect, render_template

app = Flask(__name__)

S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
R2_ENDPOINT_URL = os.environ.get("R2_ENDPOINT_URL")
R2_CUSTOM_DOMAIN = os.environ.get("R2_CUSTOM_DOMAIN")
s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    endpoint_url=R2_ENDPOINT_URL
)

COOKIES_DIR = os.path.join(os.path.dirname(__file__), "cookies.txt")
COOKIES_CMD = ["--cookies-from-browser", "chromium"] if not os.path.exists(COOKIES_DIR) else ["--cookies", COOKIES_DIR]

jobs = {}


def run_download(job_id, url, format_choice, format_id):
    job = jobs[job_id]

    with tempfile.TemporaryDirectory() as temp_dir:
        out_template = os.path.join(temp_dir, f"{job_id}.%(ext)s")
        cmd = ["yt-dlp", "--no-playlist", *COOKIES_CMD, "-o", out_template]

        if format_choice == "audio":
            cmd += ["-x", "--audio-format", "mp3"]
        elif format_id:
            cmd += ["-f", f"{format_id}+bestaudio/{format_id}/bestvideo+bestaudio/best", "--merge-output-format", "mp4"]
        else:
            cmd += ["-f", "bestvideo+bestaudio/best", "--merge-output-format", "mp4"]

        cmd.append(url)

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                job["status"] = "error"
                job["error"] = result.stderr.strip().split("\n")[-1]
                return

            files = glob.glob(os.path.join(temp_dir, f"{job_id}.*"))
            if not files:
                job["status"] = "error"
                job["error"] = "Download completed but no file was found"
                return

            if format_choice == "audio":
                target = [f for f in files if f.endswith(".mp3")]
                chosen = target[0] if target else files[0]
            else:
                target = [f for f in files if f.endswith(".mp4")]
                chosen = target[0] if target else files[0]

            ext = os.path.splitext(chosen)[1]
            title = job.get("title", "").strip()
            if title:
                safe_title = "".join(c for c in title if c not in r'\/:*?"<>|').strip()[:20].strip()
                filename = f"{safe_title}{ext}" if safe_title else os.path.basename(chosen)
            else:
                filename = os.path.basename(chosen)

            s3_key = f"downloads/{job_id}/{filename}"
            with open(chosen, "rb") as f:
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=s3_key,
                    Body=f
                )

            s3_url = f"{R2_CUSTOM_DOMAIN}/{S3_BUCKET_NAME}/{s3_key}"
            job["status"] = "done"
            job["s3_url"] = s3_url
            job["filename"] = filename

        except subprocess.TimeoutExpired:
            job["status"] = "error"
            job["error"] = "Download timed out (5 min limit)"
        except Exception as e:
            job["status"] = "error"
            job["error"] = str(e)


@app.route("/")
    job = jobs.get(job_id)
    if not job or job["status"] != "done":
        return jsonify({"error": "File not ready"}), 404
    return redirect(job["s3_url"])


if __name__ == "__main__":
