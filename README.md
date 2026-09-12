# Transcription API

A small FastAPI proxy that uploads an audio file to a configured transcription
service and polls for its result. It uses a server-side API token supplied at
runtime; no browser automation or embedded credentials are used.

## Run

Set an API token that you are authorized to use, then start the app:

```powershell
$env:AUDIOCONVERT_API_TOKEN = "your-token"
py -m pip install -r requirements.txt
py -m uvicorn main:app --host 0.0.0.0 --port 8000
```

The health endpoint is available at `GET /`. Send audio as multipart form data
to `POST /transcribe` using the `file` field.

For Docker:

```powershell
docker build -t transcription-api .
docker run --rm -p 8000:8000 -e AUDIOCONVERT_API_TOKEN="your-token" transcription-api
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `AUDIOCONVERT_API_TOKEN` | required | Server-side upstream API token. A `Bearer ` prefix is optional. |
| `AUDIOCONVERT_API_BASE_URL` | `https://audioconvert.ai/api` | Upstream API base URL. |
| `MAX_UPLOAD_MB` | `100` | Per-file limit. Larger files receive HTTP 413. |
| `MAX_CONCURRENT_TRANSCRIPTIONS` | `1` | Number of active upload/poll workflows. Keep at `1` for the lowest RAM use. |
| `UVICORN_LIMIT_CONCURRENCY` | `2` | Docker-level cap on concurrent HTTP connections. |
| `POLL_TIMEOUT_SECONDS` | `360` | Maximum time spent waiting for a task result. |
| `POLL_INTERVAL_SECONDS` | `3` | Delay between status checks. |
| `UPSTREAM_CONNECT_TIMEOUT_SECONDS` | `15` | Per-request connection timeout. |
| `UPSTREAM_REQUEST_TIMEOUT_SECONDS` | `60` | API response/read timeout. |
| `UPLOAD_WRITE_TIMEOUT_SECONDS` | `300` | Maximum time allowed for the storage upload. |
| `ALLOWED_ORIGINS` | local dev origins | Comma-separated browser origins allowed by CORS. |

## Memory behavior

Files are kept in FastAPI/Starlette's spooled upload file and sent upstream in
64 KiB chunks. The application does not call `file.read()` or retain the audio
while it polls. Docker runs one worker, accepts only a small number of
connections, and defaults to one active transcription workflow.
