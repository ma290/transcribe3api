"""Low-memory transcription proxy.

The upstream service is accessed with a server-side token supplied through the
environment. Uploads stay in Starlette's spooled temporary file and are
streamed to the presigned URL instead of being copied into application memory.
"""

import asyncio
import logging
import os
import time
from collections.abc import Iterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import PurePath
from typing import Any, BinaryIO
from urllib.parse import quote, urlsplit, urlunsplit

import httpx
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


logger = logging.getLogger(__name__)


def _positive_int_env(name: str, default: int) -> int:
    """Read a positive integer setting without making startup fragile."""
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        logger.warning("Ignoring invalid %s value", name)
        return default
    if value <= 0:
        logger.warning("Ignoring non-positive %s value", name)
        return default
    return value


def _positive_float_env(name: str, default: float) -> float:
    """Read a positive float setting without making startup fragile."""
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        value = float(raw_value)
    except ValueError:
        logger.warning("Ignoring invalid %s value", name)
        return default
    if value <= 0:
        logger.warning("Ignoring non-positive %s value", name)
        return default
    return value


@dataclass(frozen=True)
class Settings:
    base_url: str
    max_upload_bytes: int
    max_request_bytes: int
    max_concurrent_transcriptions: int
    connect_timeout_seconds: float
    request_timeout_seconds: float
    upload_write_timeout_seconds: float
    poll_interval_seconds: float
    poll_timeout_seconds: float
    allowed_origins: list[str]

    @classmethod
    def from_environment(cls) -> "Settings":
        max_upload_mb = _positive_int_env("MAX_UPLOAD_MB", 100)
        # Multipart boundaries add a small amount of data beyond the file.
        max_upload_bytes = max_upload_mb * 1024 * 1024
        return cls(
            base_url=os.getenv("AUDIOCONVERT_API_BASE_URL", "https://audioconvert.ai/api").rstrip("/"),
            max_upload_bytes=max_upload_bytes,
            max_request_bytes=max_upload_bytes + 1024 * 1024,
            max_concurrent_transcriptions=_positive_int_env("MAX_CONCURRENT_TRANSCRIPTIONS", 1),
            connect_timeout_seconds=_positive_float_env("UPSTREAM_CONNECT_TIMEOUT_SECONDS", 15.0),
            request_timeout_seconds=_positive_float_env("UPSTREAM_REQUEST_TIMEOUT_SECONDS", 60.0),
            upload_write_timeout_seconds=_positive_float_env("UPLOAD_WRITE_TIMEOUT_SECONDS", 300.0),
            poll_interval_seconds=_positive_float_env("POLL_INTERVAL_SECONDS", 3.0),
            poll_timeout_seconds=_positive_float_env("POLL_TIMEOUT_SECONDS", 360.0),
            allowed_origins=[
                origin.strip()
                for origin in os.getenv(
                    "ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:5173"
                ).split(",")
                if origin.strip()
            ],
        )


settings = Settings.from_environment()


class ConfigurationError(RuntimeError):
    """The service cannot safely call its configured upstream."""


class UpstreamError(RuntimeError):
    """The upstream API returned an unusable response."""


class UpstreamTimeoutError(UpstreamError):
    """The upstream API did not finish in the configured time."""


class TranscriptionFailedError(UpstreamError):
    """The upstream service completed the task with a failure state."""


class ContentLengthLimitMiddleware(BaseHTTPMiddleware):
    """Reject declared oversized requests before multipart parsing writes them."""

    def __init__(self, app: Any, max_body_bytes: int) -> None:
        super().__init__(app)
        self.max_body_bytes = max_body_bytes

    async def dispatch(self, request: Request, call_next: Any) -> Any:
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                request_size = int(content_length)
            except ValueError:
                return JSONResponse(
                    status_code=400,
                    content={"detail": "Invalid Content-Length header."},
                )
            if request_size < 0 or request_size > self.max_body_bytes:
                return JSONResponse(
                    status_code=413,
                    content={"detail": "Upload is larger than the configured limit."},
                )
        return await call_next(request)


@asynccontextmanager
async def lifespan(application: FastAPI):
    # A single active workflow is the biggest RAM guard: it bounds open file
    # handles, HTTP clients, and long-running polling threads.
    application.state.transcription_limiter = asyncio.Semaphore(
        settings.max_concurrent_transcriptions
    )
    yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    ContentLengthLimitMiddleware,
    max_body_bytes=settings.max_request_bytes,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=False,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)


def _authorization_header() -> str:
    """Return a user-provided token without ever scraping or storing one."""
    token = os.getenv("AUDIOCONVERT_API_TOKEN", "").strip()
    if not token:
        raise ConfigurationError("AUDIOCONVERT_API_TOKEN is not configured.")
    return token if token.lower().startswith("bearer ") else f"Bearer {token}"


def _normalise_filename(filename: str | None) -> str:
    # Browsers may send either slash style, even when the server uses Linux.
    safe_name = (filename or "").replace("\\", "/").rsplit("/", 1)[-1].strip()
    safe_name = safe_name.replace("\x00", "")
    if not safe_name or safe_name in {".", ".."}:
        raise HTTPException(status_code=400, detail="A valid filename is required.")
    # PurePath removes harmless trailing separators without touching the host FS.
    return PurePath(safe_name).name


def _stream_size(stream: BinaryIO) -> int:
    """Determine the size of a seekable spooled upload without reading it."""
    original_position = stream.tell()
    try:
        stream.seek(0, os.SEEK_END)
        return stream.tell()
    finally:
        stream.seek(original_position)


def _stream_chunks(stream: BinaryIO, chunk_size: int = 64 * 1024) -> Iterator[bytes]:
    """Yield a file-like upload in bounded chunks for HTTPX."""
    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            return
        yield chunk


def _api_timeout() -> httpx.Timeout:
    return httpx.Timeout(
        settings.request_timeout_seconds,
        connect=settings.connect_timeout_seconds,
    )


def _upload_timeout() -> httpx.Timeout:
    return httpx.Timeout(
        connect=settings.connect_timeout_seconds,
        read=settings.request_timeout_seconds,
        write=settings.upload_write_timeout_seconds,
        pool=settings.connect_timeout_seconds,
    )


def _json_mapping(response: httpx.Response, operation: str) -> Mapping[str, Any]:
    try:
        payload = response.json()
    except (ValueError, httpx.DecodingError) as exc:
        raise UpstreamError(f"{operation} returned invalid JSON.") from exc
    if not isinstance(payload, Mapping):
        raise UpstreamError(f"{operation} returned an unexpected JSON shape.")
    return payload


def _nested_data(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, Mapping) else {}


def _first_value(
    primary: Mapping[str, Any], secondary: Mapping[str, Any], *keys: str
) -> Any:
    for key in keys:
        value = primary.get(key)
        if value is not None:
            return value
        value = secondary.get(key)
        if value is not None:
            return value
    return None


def _require_success(response: httpx.Response, operation: str) -> None:
    if 200 <= response.status_code < 300:
        return
    logger.warning("Upstream %s returned HTTP %s", operation, response.status_code)
    raise UpstreamError(f"{operation} was rejected by the upstream service.")


def _extract_upload_urls(payload: Mapping[str, Any]) -> tuple[str, str]:
    data = _nested_data(payload)
    upload_url = _first_value(data, payload, "upload_url", "uploadUrl")
    if not isinstance(upload_url, str) or not upload_url:
        raise UpstreamError("The upstream service did not return an upload URL.")

    parsed_url = urlsplit(upload_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise UpstreamError("The upstream service returned an invalid upload URL.")

    # The task API expects the object URL, while the PUT needs its signature.
    object_url = urlunsplit(parsed_url._replace(query="", fragment=""))
    return upload_url, object_url


def _extract_task_id(payload: Mapping[str, Any]) -> str:
    task_id = _first_value(_nested_data(payload), payload, "id", "task_id", "taskId")
    if isinstance(task_id, (str, int)) and str(task_id):
        return str(task_id)
    raise UpstreamError("The upstream service did not return a task ID.")


def _terminal_result(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    data = _nested_data(payload)
    status = _first_value(data, payload, "status")
    normalised_status = str(status).strip().lower() if status is not None else ""

    if normalised_status in {"failed", "failure", "error", "cancelled", "canceled", "4"}:
        raise TranscriptionFailedError("The upstream service reported a failed transcription.")

    transcript = _first_value(data, payload, "transcript", "text")
    is_success = normalised_status in {"success", "completed", "complete", "done", "3"}
    if not is_success and transcript is None:
        return None

    return {
        "success": True,
        "transcript": transcript,
        "pdf_url": _first_value(data, payload, "pdf_url", "pdfUrl"),
        "srt_url": _first_value(data, payload, "srt_url", "srtUrl"),
    }


def run_transcription_workflow(stream: BinaryIO, filename: str, file_size: int) -> dict[str, Any]:
    """Run the blocking upstream workflow while streaming the spooled upload."""
    try:
        headers = {
            "accept": "application/json",
            "authorization": _authorization_header(),
        }
        with httpx.Client(headers=headers, timeout=_api_timeout()) as api_client:
            presign_response = api_client.get(
                f"{settings.base_url}/resource/upload/presign",
                params={"filename": filename},
            )
            _require_success(presign_response, "presign request")
            upload_url, object_url = _extract_upload_urls(
                _json_mapping(presign_response, "Presign request")
            )

            # Do not inherit Authorization onto a presigned storage request.
            # The explicit iterator keeps the largest in-memory upload piece at 64 KiB.
            stream.seek(0)
            with httpx.Client(timeout=_upload_timeout()) as upload_client:
                upload_response = upload_client.put(
                    upload_url,
                    content=_stream_chunks(stream),
                    headers={"Content-Length": str(file_size)},
                )
            _require_success(upload_response, "storage upload")

            task_response = api_client.post(
                f"{settings.base_url}/transcribe/",
                json={
                    "audio_url": object_url,
                    "language_code": "",
                    "file_name": filename,
                    "scenario": "auto",
                },
            )
            _require_success(task_response, "transcription request")
            task_id = _extract_task_id(_json_mapping(task_response, "Transcription request"))

            polling_url = f"{settings.base_url}/transcribe/{quote(task_id, safe='')}"
            deadline = time.monotonic() + settings.poll_timeout_seconds
            while time.monotonic() < deadline:
                poll_response = api_client.get(polling_url)
                if poll_response.status_code == 200:
                    result = _terminal_result(_json_mapping(poll_response, "Status request"))
                    if result is not None:
                        result["task_id"] = task_id
                        return result
                elif poll_response.status_code in {408, 429, 500, 502, 503, 504}:
                    logger.warning(
                        "Upstream status request for task %s returned HTTP %s",
                        task_id,
                        poll_response.status_code,
                    )
                else:
                    _require_success(poll_response, "status request")

                remaining = deadline - time.monotonic()
                if remaining > 0:
                    time.sleep(min(settings.poll_interval_seconds, remaining))

        raise UpstreamTimeoutError("The transcription did not finish before the polling deadline.")
    except httpx.TimeoutException as exc:
        raise UpstreamTimeoutError("The upstream service timed out.") from exc
    except httpx.HTTPError as exc:
        raise UpstreamError("The upstream service could not be reached.") from exc


@app.post("/transcribe")
async def transcribe(request: Request, file: UploadFile = File(...)) -> dict[str, Any]:
    try:
        filename = _normalise_filename(file.filename)
        try:
            file_size = await run_in_threadpool(_stream_size, file.file)
        except (OSError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="The uploaded file cannot be read.") from exc

        if file_size <= 0:
            raise HTTPException(status_code=400, detail="Empty file payload.")
        if file_size > settings.max_upload_bytes:
            raise HTTPException(status_code=413, detail="Upload is larger than the configured limit.")

        limiter: asyncio.Semaphore = request.app.state.transcription_limiter
        async with limiter:
            return await run_in_threadpool(
                run_transcription_workflow, file.file, filename, file_size
            )
    except HTTPException:
        raise
    except ConfigurationError as exc:
        logger.error("Transcription service is not configured: %s", exc)
        raise HTTPException(status_code=503, detail="Transcription service is not configured.") from exc
    except TranscriptionFailedError as exc:
        logger.info("Upstream transcription failed: %s", exc)
        raise HTTPException(status_code=422, detail="The upstream service could not transcribe this file.") from exc
    except UpstreamTimeoutError as exc:
        logger.warning("Upstream transcription timed out: %s", exc)
        raise HTTPException(status_code=504, detail="The upstream transcription request timed out.") from exc
    except UpstreamError as exc:
        logger.warning("Upstream transcription error: %s", exc)
        raise HTTPException(status_code=502, detail="The upstream transcription service failed.") from exc
    except Exception as exc:
        logger.exception("Unexpected error in transcription endpoint")
        raise HTTPException(status_code=500, detail="Internal transcription error.") from exc
    finally:
        try:
            await file.close()
        except (OSError, ValueError):
            logger.warning("Failed to close temporary upload file")


@app.get("/")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "configured": bool(os.getenv("AUDIOCONVERT_API_TOKEN", "").strip()),
    }
