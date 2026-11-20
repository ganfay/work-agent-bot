"""
Shared LLM / Embedding call helpers with exponential backoff for transient API errors.
"""

import logging
import random
import re
import time
from typing import Callable, TypeVar

import httpx
from google import genai
from google.genai import errors as genai_errors
from google.genai import types

logger = logging.getLogger("ai_worker")

DEFAULT_GEMINI_TIMEOUT_MS = 30_000

T = TypeVar("T")

_TRANSIENT_HTTP_CODES = frozenset({429, 500, 502, 503, 504})
_TRANSIENT_STATUS_RE = re.compile(r"\b(?:429|500|502|503|504)\b")
_TRANSIENT_PHRASES = (
    "resource_exhausted",
    "rate limit",
    "too many requests",
    "service unavailable",
    "deadline exceeded",
    "timed out",
    "timeout",
    "connection reset",
    "connection refused",
    "connection aborted",
    "temporarily unavailable",
)


def create_genai_client(api_key: str, timeout_ms: int = DEFAULT_GEMINI_TIMEOUT_MS) -> genai.Client:
    """Creates a google-genai Client configured with an explicit HTTP timeout (in ms)."""
    return genai.Client(
        api_key=api_key,
        http_options=types.HttpOptions(timeout=timeout_ms),
    )


def is_transient_error(err: Exception) -> bool:
    if isinstance(err, (ValueError, TypeError, KeyError, AttributeError)):
        return False

    if isinstance(
        err,
        (
            httpx.TimeoutException,
            httpx.NetworkError,
            httpx.RemoteProtocolError,
            TimeoutError,
            ConnectionError,
        ),
    ):
        return True

    if isinstance(err, genai_errors.APIError):
        return err.code in _TRANSIENT_HTTP_CODES

    code = getattr(err, "code", None)
    if code is None:
        code = getattr(err, "status_code", None)
    if code is None:
        resp = getattr(err, "response", None)
        code = getattr(resp, "status_code", None) if resp is not None else None
    if isinstance(code, int):
        return code in _TRANSIENT_HTTP_CODES

    msg = str(err).lower()
    if _TRANSIENT_STATUS_RE.search(msg):
        return True
    return any(phrase in msg for phrase in _TRANSIENT_PHRASES)


def call_with_retry(
    fn: Callable[[], T],
    operation: str,
    max_attempts: int = 3,
    base_delay: float = 1.0,
) -> T:
    """Executes fn with exponential backoff + jitter on transient API errors."""
    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as err:
            last_err = err
            if attempt >= max_attempts or not is_transient_error(err):
                raise
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0.0, 0.5)
            logger.warning(
                f"[Retry] {operation} failed (attempt {attempt}/{max_attempts}): {err}. "
                f"Retrying in {delay:.2f}s..."
            )
            time.sleep(delay)
    assert last_err is not None
    raise last_err
