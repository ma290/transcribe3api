FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MALLOC_ARENA_MAX=2 \
    PORT=8000 \
    UVICORN_LIMIT_CONCURRENCY=50

EXPOSE 8000

CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port \"${PORT:-8000}\" --workers 1 --limit-concurrency \"${UVICORN_LIMIT_CONCURRENCY:-2}\" --timeout-keep-alive 5"]
