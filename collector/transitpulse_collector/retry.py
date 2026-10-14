from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass


class RetryableError(Exception):
    def __init__(self, message: str, http_status: int | None = None) -> None:
        super().__init__(message)
        self.http_status = http_status


class PermanentError(Exception):
    def __init__(self, message: str, http_status: int | None = None) -> None:
        super().__init__(message)
        self.http_status = http_status


class RetriesExhausted(Exception):
    def __init__(self, last: RetryableError, attempts: int) -> None:
        super().__init__(str(last))
        self.last = last
        self.attempts = attempts
        self.http_status = last.http_status


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_s: float = 1.0
    jitter_s: float = 0.25


def with_retry[T](
    op: Callable[[], T],
    *,
    policy: RetryPolicy,
    deadline: float,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
) -> tuple[T, int]:
    """Run op, retrying RetryableError with exponential backoff until attempts or the deadline run out.

    Returns the result and the number of attempts used. PermanentError propagates immediately.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return op(), attempt
        except RetryableError as exc:
            if attempt >= policy.max_attempts:
                raise RetriesExhausted(exc, attempt) from exc
            delay = policy.base_delay_s * 2 ** (attempt - 1) + random.uniform(0, policy.jitter_s)
            if clock() + delay >= deadline:
                raise RetriesExhausted(exc, attempt) from exc
            sleep(delay)
