"""Symbols the provider cannot serve -- measured, persisted, and re-checked.

The problem this solves
-----------------------
Six NIFTY 500 names are in the app's universe (``instrument_labels``) but not in
the Kite NSE instrument list, so every live ingest cycle raised one
``ProviderError`` per name and finished with ``errors > 0``.  ``/api/state``
reports that number, and the app's data panel turns it into *"The last ingest
cycle finished with N error(s); some symbols may be behind."*  The warning was
true of the loop and false of the data: nothing was behind, those symbols
simply do not exist for this account.

The rules this module keeps
---------------------------
* **Measured, not declared.**  Membership comes from asking the provider
  (:func:`probe`), never from a constant list.  A hard-coded list is exactly
  how a symbol that comes back stays excluded forever.
* **One audit trail.**  The decision is persisted by ``MarketStore.quarantine``,
  which writes both the ``quarantine`` row and a ``corrections`` row with
  ``field='quarantine_status'`` -- the mechanism the repair pass already used.
* **Self-healing.**  ``last_checked`` drives a once-a-day re-probe; the first
  cycle after the provider starts serving a symbol releases it.
* **Never silent.**  A quarantined symbol keeps every bar it already has and
  stays in the universe the app lists; it is excluded from *fetching*, not from
  existing.  What the app shows for it is a reporting question, answered in
  ``market_scanner/data_status.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence

#: Machine-readable reason codes.
NOT_IN_INSTRUMENT_LIST = "not_in_provider_instrument_list"
FETCH_FAILS = "provider_fetch_fails"

#: How long a quarantine decision stands before it is re-tested.
DEFAULT_RECHECK_HOURS = 24.0


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def _parse(stamp) -> datetime | None:
    if not stamp:
        return None
    try:
        return datetime.fromisoformat(str(stamp).replace("Z", ""))
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class ProbeResult:
    symbol: str
    available: bool
    error: str | None = None
    detail: str = ""


def probe(provider, symbol: str) -> ProbeResult:
    """Ask the provider whether it knows `symbol` at all.

    Uses the instrument list rather than a candle request: it is the question
    we actually mean ("does this symbol exist for this account?"), it costs no
    historical-data quota, and it cannot be confused with "listed but no bars
    in the window you asked for".
    """
    resolve = getattr(provider, "resolve", None)
    if resolve is None:
        return ProbeResult(symbol, True, None,
                           "provider exposes no instrument list; nothing to check")
    try:
        inst = resolve(symbol)
    except Exception as exc:  # noqa: BLE001 - the error IS the answer
        return ProbeResult(symbol, False, f"{type(exc).__name__}: {exc}",
                           f"{symbol} is not in the provider's instrument list")
    return ProbeResult(symbol, True, None,
                       f"{symbol} resolves to instrument {inst.instrument_id}")


def due_for_recheck(row: dict, *, now: datetime | None = None,
                    hours: float = DEFAULT_RECHECK_HOURS) -> bool:
    """Has `row`'s quarantine stood long enough to be tested again?"""
    if not row or row.get("status") != "quarantined":
        return False
    last = _parse(row.get("last_checked"))
    if last is None:
        return True
    return (now or _now()) - last >= timedelta(hours=hours)


def due_symbols(rows: dict, *, now: datetime | None = None,
                hours: float = DEFAULT_RECHECK_HOURS) -> set:
    return {s for s, r in rows.items() if due_for_recheck(r, now=now, hours=hours)}


def sync(store, provider, symbols: Sequence[str], *, run_id: str | None = None,
         now: datetime | None = None, release: bool = True) -> dict:
    """Probe `symbols` and bring the persisted quarantine into line.

    Returns ``{"quarantined": [...], "released": [...], "ok": [...]}``.
    """
    stamp = (now or _now()).isoformat(sep=" ")
    provider_id = getattr(provider, "provider_id", "unknown")
    out: dict[str, list] = {"quarantined": [], "released": [], "ok": []}
    for symbol in symbols:
        result = probe(provider, symbol)
        if result.available:
            if release and (store.get_quarantine(symbol) or {}).get("status") == "quarantined":
                store.release_quarantine(
                    symbol, detail=result.detail, run_id=run_id, when=stamp)
                out["released"].append(symbol)
            else:
                out["ok"].append(symbol)
            continue
        store.quarantine(symbol, reason=NOT_IN_INSTRUMENT_LIST,
                         detail=result.detail, provider=provider_id,
                         run_id=run_id, error=result.error, when=stamp)
        out["quarantined"].append(symbol)
    return out


def summarise(store) -> list[dict]:
    """The quarantine as the operator (and `/api/state`) should read it."""
    rows = store.quarantined()
    return [{"symbol": r["symbol"], "reason": r["reason"], "detail": r["detail"],
             "first_seen": r["first_seen"], "last_checked": r["last_checked"],
             "checks": r["checks"], "provider": r["provider"]}
            for r in rows.values()]
