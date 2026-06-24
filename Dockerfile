FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    TZ=Asia/Shanghai \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    FLASK_HOST=0.0.0.0 \
    FLASK_PORT=8003 \
    DATABASE_PATH=/app/data/crawler_articles.db \
    CRAWL_RESULTS_DIR=/app/crawl_results \
    AUTH_STORAGE_DIR=/app/auth_storage \
    LOG_FILE=/app/crawl_logs/app.log

WORKDIR /app

COPY requirements.txt .
RUN python -m pip install --upgrade pip \
    && python -m pip install --no-cache-dir -r requirements.txt

COPY . .
RUN sed -i 's/\r$//' /app/docker-entrypoint.sh \
    && chmod +x /app/docker-entrypoint.sh \
    && mkdir -p /app/data /app/crawl_results /app/auth_storage /app/crawl_logs

EXPOSE 8003

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["python", "start_with_schedule.py"]
