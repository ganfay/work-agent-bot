"""
Shared LLM / Embedding call helpers with exponential backoff for transient API errors.
"""

import logging
import random
import time
from typing import Callable, TypeVar

logger = logging.getLogger("ai_worker")

T = TypeVar("T")

_TRANSIENT_MARKERS = (
    "429",
    "500",
    "502",
    "503",
    "504",
    "resource_exhausted",
    "rate limit",
    "quota",
    "unavailable",
    "deadline",
    "timeout",
    "connection",
)


def is_transient_error(err: Exception) -> bool:
    code = getattr(err, "code", None) or getattr(err, "status_code", None)
    if isinstance(code, int) and code in (429, 500, 502, 503, 504):
        return True
    msg = str(err).lower()
    return any(marker in msg for marker in _TRANSIENT_MARKERS)


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
