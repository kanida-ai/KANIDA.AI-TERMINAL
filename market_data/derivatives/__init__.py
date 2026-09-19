"""KANIDA derivatives (F&O) capture — spec: ``docs/DERIVATIVES_SPEC.md``.

Capture and store only.  This package resolves the NFO contract scope, takes a
snapshot of every in-scope contract at every 15-minute mark, backfills 10
sessions of 15-minute candles with open interest, and keeps ``db/derivatives.db``
pruned.  It computes no signals and draws no UI: the §3 metrics and the §4
Derivative tab are other people's work, and the ``metrics`` table is left for
them.

    from market_data.derivatives import DerivativesStore, build

    cap = build()                 # store + the shared authenticated Kite provider
    cap.ensure_scope()            # front two expiries per underlying
    cap.run_once()                # one 15-minute mark

Layout:
    config.py        every constant the UI or a report might want to quote
    instruments.py   the NFO dump and the front-two-expiry scope
    schema.sql       db/derivatives.db (never kanida.db / market15.db)
    store.py         WAL, one writer, idempotent by (contract, mark)
    capture.py       the 15-minute snapshot loop
    backfill.py      10 sessions of 15-minute candles with OI, and the
                     in-scope futures' own daily candles with OI (candles_day)
    fake_nfo.py      a deterministic offline provider, for the tests
    cli.py           status · plan · once · run · backfill · backfill-daily ·
                     prune · rollup
"""
from .config import DEFAULT_DB_PATH
from .instruments import Contract, Scope, fetch_nfo_instruments, select_scope
from .store import DerivativesStore, open_readonly

__all__ = [
    "DEFAULT_DB_PATH",
    "Contract",
    "Scope",
    "DerivativesStore",
    "open_readonly",
    "fetch_nfo_instruments",
    "select_scope",
    "build",
    "DerivativesCapture",
]


def __getattr__(name):
    # capture.py pulls in the provider registry; keep `import market_data.derivatives`
    # cheap for readers that only want the store.
    if name in ("build", "DerivativesCapture"):
        from . import capture

        return getattr(capture, name)
    raise AttributeError(name)
