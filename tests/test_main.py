from io import BytesIO

import pytest
from fastapi import HTTPException

import main


def test_normalise_filename_removes_path_components() -> None:
    assert main._normalise_filename(r"C:\fakepath\recording.mp3") == "recording.mp3"
    assert main._normalise_filename("../../recording.mp3") == "recording.mp3"


def test_normalise_filename_rejects_missing_name() -> None:
    with pytest.raises(HTTPException) as error:
        main._normalise_filename(None)

    assert error.value.status_code == 400


def test_stream_size_and_chunks_do_not_read_whole_file() -> None:
    payload = b"a" * (64 * 1024 + 3)
    stream = BytesIO(payload)

    assert main._stream_size(stream) == len(payload)
    assert list(main._stream_chunks(stream)) == [b"a" * (64 * 1024), b"a" * 3]


def test_terminal_result_ignores_null_transcript_until_success() -> None:
    assert main._terminal_result({"data": {"status": "processing", "transcript": None}}) is None
    assert main._terminal_result({"data": {"status": "completed", "transcript": None}}) == {
        "success": True,
        "transcript": None,
        "pdf_url": None,
        "srt_url": None,
    }


def test_terminal_result_maps_failure_state() -> None:
    with pytest.raises(main.TranscriptionFailedError):
        main._terminal_result({"data": {"status": "failed"}})
