"""Process-wide token-bucket rate limiting (contract §1, last bullet).

"One global rate limiter shared by all workers (token bucket at
`rate_limit_per_second`); extra workers must not exceed it."

Design
------
* Limiters are **named** and stored in a module-level registry, so every caller
  in the process that asks for ``get_limiter("kite")`` gets the *same* bucket.
  Spawning ten fetch threads therefore does not multiply the request rate.
* If two callers ask for the same name with different rates, the **lower** rate
  wins.  Being accidentally slower is recoverable; being accidentally faster
  gets the API key banned.
* ``capacity`` (burst) defaults to 1.0 — a strict pacer.  Raise it only if the
  vendor explicitly allows bursts.
* Cross-*process* limiting is out of scope here (Kite counts per API key, so a
  second Python process would double the rate).  ``FileLockLimiter`` below is
  the hook for that when the ingest runner starts forking; today the whole
  pipeline runs in one process and ``get_limiter`` is enough.
"""
from __future__ import annotations

import threading
import time
from typing import Dict, Optional

__all__ = ["TokenBucket", "get_limiter", "reset_limiters", "limiter_names"]


class TokenBucket:
    """Thread-safe token bucket.

    ``rate`` tokens are added per second up to ``capacity``.  ``acquire`` blocks
    until a token is available.  The invariant that matters: across *any* set of
    threads sharing one instance, the number of successful acquisitions in a
    window of T seconds never exceeds ``capacity + rate * T``.
    """

    __slots__ = ("_rate", "_capacity", "_tokens", "_last", "_lock", "name", "_granted")

    def __init__(self, rate: float, capacity: Optional[float] = None, name: str = ""):
        if rate <= 0:
            raise ValueError("rate must be > 0")
        self._rate = float(rate)
        self._capacity = float(capacity) if capacity is not None else 1.0
        if self._capacity < 1.0:
            self._capacity = 1.0
        self._tokens = self._capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()
        self.name = name
        self._granted = 0

    # -- introspection -------------------------------------------------------

    @property
    def rate(self) -> float:
        return self._rate

    @property
    def capacity(self) -> float:
        return self._capacity

    @property
    def granted(self) -> int:
        """Total successful acquisitions (used by tests / ingest run stats)."""
        return self._granted

    def lower_rate_to(self, rate: float) -> None:
        """Clamp this shared bucket to the slowest rate any caller asked for."""
        with self._lock:
            if rate < self._rate:
                self._rate = float(rate)
                self._tokens = min(self._tokens, self._capacity)

    # -- core ----------------------------------------------------------------

    def _refill_locked(self) -> float:
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last = now
        return self._tokens

    def try_acquire(self, tokens: float = 1.0) -> bool:
        with self._lock:
            self._refill_locked()
            if self._tokens >= tokens:
                self._tokens -= tokens
                self._granted += 1
                return True
            return False

    def acquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        """Block until ``tokens`` are available.  Returns False only on timeout."""
        if tokens <= 0:
            return True
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._lock:
                available = self._refill_locked()
                if available >= tokens:
                    self._tokens -= tokens
                    self._granted += 1
                    return True
                wait = (tokens - available) / self._rate
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                wait = min(wait, remaining)
            # Sleep in small slices so a rate change by another thread is picked
            # up quickly, and so many waiters wake staggered rather than in a herd.
            time.sleep(max(0.0005, min(wait, 0.05)))

    # convenience alias matching the engine project's RateLimiter.wait()
    def wait(self, tokens: float = 1.0) -> None:
        self.acquire(tokens)

    def __enter__(self) -> "TokenBucket":
        self.acquire()
        return self

    def __exit__(self, *exc) -> None:
        return None


# ── process-wide registry ────────────────────────────────────────────────────

_REGISTRY: Dict[str, TokenBucket] = {}
_REGISTRY_LOCK = threading.Lock()


def get_limiter(name: str, rate: float, capacity: Optional[float] = None) -> TokenBucket:
    """Return *the* process-wide limiter called ``name``.

    First caller creates it; later callers get the same object and can only
    slow it down, never speed it up.
    """
    with _REGISTRY_LOCK:
        bucket = _REGISTRY.get(name)
        if bucket is None:
            bucket = TokenBucket(rate=rate, capacity=capacity, name=name)
            _REGISTRY[name] = bucket
            return bucket
    bucket.lower_rate_to(rate)
    return bucket


def limiter_names() -> list[str]:
    with _REGISTRY_LOCK:
        return sorted(_REGISTRY)


def reset_limiters() -> None:
    """Test hook only — drops every registered limiter."""
    with _REGISTRY_LOCK:
        _REGISTRY.clear()
