"""Polite HTTP fetcher for NSE F&O bhavcopy archives.

* At most one request per ``min_interval`` seconds (default 1.1 s).
* Browser-like User-Agent. A best-effort GET of https://www.nseindia.com
  primes cookies. As of 2026-09-25 that page answers 403 to scripted clients
  while the archive host serves the zips without cookies, so a failed prime
  is logged and does not count against the stop rule.
* Retries timeouts and 5xx with exponential backoff.
* 404 means no file was published for that day (holiday, or not yet out).
* Repeated 403/429 raises :class:`Blocked`, and the caller stops the run.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import requests

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)
HOME = "https://www.nseindia.com"
ARCHIVE = "https://nsearchives.nseindia.com"
# Last day NSE published the old format as the primary file. UDiFF files also
# exist for some earlier days (verified for 2024-07-05), and the old format
# stops after this date (fo08JUL2024 answers 404).
OLD_FORMAT_LAST = date(2024, 7, 5)
MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
          "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


class Blocked(RuntimeError):
    """NSE answered 403/429 repeatedly; the run must stop."""


@dataclass
class FetchResult:
    trade_date: date
    status: str            # ok | holiday_or_missing | error
    url: str
    http_status: int | None
    content: bytes | None
    source_format: str | None
    note: str = ""
    from_cache: bool = False


def old_url(d: date) -> str:
    mon = MONTHS[d.month - 1]
    return (f"{ARCHIVE}/content/historical/DERIVATIVES/{d.year}/{mon}/"
            f"fo{d.day:02d}{mon}{d.year}bhav.csv.zip")


def udiff_url(d: date) -> str:
    return f"{ARCHIVE}/content/fo/BhavCopy_NSE_FO_0_0_0_{d:%Y%m%d}_F_0000.csv.zip"


def candidates(d: date) -> list[tuple[str, str]]:
    """(format, url) in the order to try. The primary format comes first."""
    if d <= OLD_FORMAT_LAST:
        return [("old", old_url(d))]
    return [("udiff", udiff_url(d))]


def cache_path(raw_dir: Path, d: date, url: str) -> Path:
    return raw_dir / str(d.year) / url.rsplit("/", 1)[-1]


class NSEClient:
    def __init__(self, min_interval: float = 1.1, max_retries: int = 3,
                 block_limit: int = 3, timeout: float = 30.0,
                 session: requests.Session | None = None, sleep=time.sleep,
                 log=print):
        self.s = session or requests.Session()
        self.s.headers.update({
            "User-Agent": UA,
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": HOME + "/",
        })
        self.min_interval = min_interval
        self.max_retries = max_retries
        self.block_limit = block_limit
        self.timeout = timeout
        self.sleep = sleep
        self.log = log
        self._last = 0.0
        self._consecutive_blocks = 0
        self._primed = False

    def _throttle(self) -> None:
        wait = self._last + self.min_interval - time.monotonic()
        if wait > 0:
            self.sleep(wait)
        self._last = time.monotonic()

    def prime(self) -> None:
        if self._primed:
            return
        self._primed = True
        try:
            self._throttle()
            r = self.s.get(HOME, timeout=self.timeout)
            self.log(f"[prime] {HOME} -> HTTP {r.status_code}, "
                     f"cookies={sorted(self.s.cookies.keys())}")
        except requests.RequestException as e:  # best effort only
            self.log(f"[prime] {HOME} failed: {e!r} (continuing)")

    def get(self, url: str) -> tuple[int | None, bytes | None, str]:
        """Return (http_status, body_or_None, note). Raises Blocked."""
        self.prime()
        note = ""
        code: int | None = None
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            try:
                r = self.s.get(url, timeout=self.timeout)
            except requests.RequestException as e:
                note = f"network error: {e.__class__.__name__}"
                self.log(f"  attempt {attempt}: {note}")
                self.sleep(2 ** attempt)
                continue
            code = r.status_code
            if code == 200:
                self._consecutive_blocks = 0
                return code, r.content, ""
            if code == 404:
                self._consecutive_blocks = 0
                return code, None, "HTTP 404: no file published (holiday or not yet out)"
            if code in (403, 429):
                self._consecutive_blocks += 1
                note = f"HTTP {code} (blocked {self._consecutive_blocks}x in a row)"
                self.log(f"  attempt {attempt}: {note}")
                if self._consecutive_blocks >= self.block_limit:
                    raise Blocked(f"{note} at {url}")
                self.sleep(10 * 2 ** attempt)
                continue
            note = f"HTTP {code}"
            self.log(f"  attempt {attempt}: {note}")
            self.sleep(2 ** attempt)
        return code, None, f"gave up after {self.max_retries} attempts: {note}"


def fetch_day(client: NSEClient, d: date, raw_dir: Path) -> FetchResult:
    """Get one day's zip from the raw cache or NSE. Never raises except Blocked."""
    last: FetchResult | None = None
    for fmt, url in candidates(d):
        cp = cache_path(raw_dir, d, url)
        if cp.exists() and cp.stat().st_size > 0:
            return FetchResult(d, "ok", url, 200, cp.read_bytes(), fmt,
                               "raw cache", from_cache=True)
        code, body, note = client.get(url)
        if body is not None and body[:2] == b"PK":
            cp.parent.mkdir(parents=True, exist_ok=True)
            tmp = cp.with_suffix(cp.suffix + ".part")
            tmp.write_bytes(body)
            tmp.replace(cp)
            return FetchResult(d, "ok", url, code, body, fmt)
        if body is not None:
            note = f"HTTP {code} but body is not a zip ({len(body)} bytes)"
            last = FetchResult(d, "error", url, code, None, fmt, note)
        elif code == 404:
            last = FetchResult(d, "holiday_or_missing", url, code, None, fmt, note)
        else:
            last = FetchResult(d, "error", url, code, None, fmt, note)
    assert last is not None
    return last
