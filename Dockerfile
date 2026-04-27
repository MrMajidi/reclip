FROM python:3.12-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg nodejs && \
    rm -rf /var/lib/apt/lists/*
RUN mkdir -p /root/.config/yt-dlp && \
    echo "--js-runtimes node" > /root/.config/yt-dlp/config
WORKDIR /app
COPY cookies /app/cookies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8899
ENV HOST=0.0.0.0
ENV PYTHONUNBUFFERED=1
CMD ["gunicorn", "--bind", "0.0.0.0:8899", "--workers", "1", "--threads", "16", "--timeout", "300", "app:app"]
