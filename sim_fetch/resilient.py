from __future__ import annotations

import time
from typing import Callable, Optional, TypeVar

import requests

T = TypeVar("T")

RETRYABLE_HTTP_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
_RETRYABLE_MESSAGE_FRAGMENTS = (
    "429",
    "502",
    "503",
    "504",
    "connection aborted",
    "connection reset",
    "connection refused",
    "connection error",
    "name or service not known",
    "temporarily unavailable",
    "timed out",
    "timeout",
    "too many requests",
)


def describe_exception(exc: BaseException, *, limit: int = 220) -> str:
    text = f"{type(exc).__name__}: {exc}".replace("\n", " ").strip()
    if len(text) > int(limit):
        return text[: int(limit) - 3] + "..."
    return text


def is_retryable_network_error(exc: BaseException) -> bool:
    if isinstance(
        exc,
        (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ),
    ):
        return True
    if isinstance(exc, requests.HTTPError):
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        return int(status_code) in RETRYABLE_HTTP_STATUS_CODES if status_code is not None else False
    text = str(exc).lower()
    return any(fragment in text for fragment in _RETRYABLE_MESSAGE_FRAGMENTS)


def retry_call(
    func: Callable[[], T],
    *,
    attempts: int = 4,
    retry_if: Callable[[BaseException], bool] = is_retryable_network_error,
    base_sleep_seconds: float = 0.35,
    max_sleep_seconds: float = 5.0,
    on_retry: Optional[Callable[[BaseException, int, int, float], None]] = None,
) -> T:
    total_attempts = max(1, int(attempts))
    for attempt in range(1, total_attempts + 1):
        try:
            return func()
        except Exception as exc:
            if attempt >= total_attempts or not retry_if(exc):
                raise
            sleep_seconds = min(float(max_sleep_seconds), float(base_sleep_seconds) * float(attempt))
            if on_retry is not None:
                on_retry(exc, attempt, total_attempts, sleep_seconds)
            time.sleep(max(0.0, float(sleep_seconds)))
    raise RuntimeError("retry_call exhausted without returning")


def request_json(
    *,
    session: requests.Session,
    method: str,
    url: str,
    timeout: float,
    attempts: int = 4,
    retry_statuses: Optional[set[int]] = None,
    context: str = "request",
    base_sleep_seconds: float = 0.35,
    max_sleep_seconds: float = 5.0,
    on_retry: Optional[Callable[[BaseException, int, int, float], None]] = None,
    **kwargs,
) -> object:
    retryable_statuses = set(retry_statuses or RETRYABLE_HTTP_STATUS_CODES)

    def _load() -> object:
        response = session.request(str(method).upper(), url, timeout=timeout, **kwargs)
        if int(response.status_code) in retryable_statuses:
            raise requests.HTTPError(
                f"{context} HTTP {response.status_code}",
                response=response,
            )
        if int(response.status_code) >= 400:
            body = (response.text or "").replace("\n", " ").strip()
            if len(body) > 220:
                body = body[:220] + "..."
            raise RuntimeError(f"{context} HTTP {response.status_code}: body={body}")
        try:
            return response.json()
        except ValueError as exc:
            body = (response.text or "").replace("\n", " ").strip()
            if len(body) > 220:
                body = body[:220] + "..."
            raise RuntimeError(f"{context} non-JSON response: body={body}") from exc

    return retry_call(
        _load,
        attempts=int(attempts),
        retry_if=is_retryable_network_error,
        base_sleep_seconds=float(base_sleep_seconds),
        max_sleep_seconds=float(max_sleep_seconds),
        on_retry=on_retry,
    )
