"""
Chart Agent · FULL-UNIVERSE daily SCREENER (post-market precompute + fast serve).

The detectors emit only COMPLETED patterns; this module is the LIVE-STAGE screener that runs each
registered pattern's live classifier (``detect``) across the WHOLE daily source for a chosen date and
returns the current setups (APPROACHING / BREAKOUT / RETEST / FAILED). It is the market-wide layer
that feeds the agent storyline — "today the Chart Agent found N setups" — instead of hand-picking.

Two-tier, reusable design (works for any future product agent, and for a 1-min/intraday tier that
swaps only the source):

  TIER 1 — SCREEN (this file, CHEAP):   post-market ``build_screen(D)`` scans the full universe
           point-in-time and writes a SMALL per-date artifact (setups only, not the 65 GB DB).
           ``load_screen(D)`` serves it in <1 s on the request path.
  TIER 2 — EVIDENCE (unchanged, HEAVY): the per-symbol decision/storyline path stays on-demand — a
           full-universe backtest is NEVER run up front.

POINT-IN-TIME IS LAW. A scan for date D uses only bars dated <= D. That is STRUCTURAL here: the panel
is queried ``date <= D`` (data.load_panel) and every detector slices to ``as_of_idx`` before it reads
anything, so a bar dated after D is never loaded, let alone seen.

BUILT:  horizontal_trendline (real live-stage classifier, ported R&D screener rules — agrees with the
        detector: TITAN 2022-08-30 -> BREAKOUT @ ~2565). triangle/channel are registered but their
        detectors are SPEC skeletons and honestly contribute nothing yet.
SPEC:   S3/EFS screen store (thin stub below); per-(symbol,pattern,date) evidence cache (not built).
"""
from __future__ import annotations
import os
import json
import time
import logging
from datetime import datetime, timezone

import pandas as pd

from . import data
from .patterns import registry as patterns

log = logging.getLogger("agents.chart.screener")

# Windowed panel lookback (calendar days). The horizontal detector reads at most level_window(120)+L
# +retest_max ~= 140 bars before as_of; 400 calendar days ~= 270 trading bars is comfortably larger,
# so the windowed frame is INDISTINGUISHABLE from full history for the level clustering -> the screen
# agrees bar-for-bar with the full-history detector (asserted in the tests).
LOOKBACK_DAYS = 400

# Skip symbols with fewer than this many bars <= as_of (honest, point-in-time). The horizontal
# detector itself needs >= level_window+L+5 = 130 bars or it returns [] — this just avoids the call.
MIN_BARS = 130

# Rank so the actionable stages surface first (same intent as the R&D console ranking).
_STAGE_ORDER = {"BREAKOUT": 0, "RETEST": 1, "APPROACHING": 2, "FAILED": 3}


# ------------------------------------------------------------------------------------ universe
def universe(as_of_date=None, min_bars: int | None = None) -> list:
    """All tradable symbols in the active daily source (minus NIFTY). With ``as_of_date`` AND
    ``min_bars`` set, applies a point-in-time min-history filter (symbols with < min_bars bars <=
    as_of are dropped) using the windowed panel. Without them, the raw symbol list."""
    syms = list(data.all_symbols())
    if as_of_date is None or not min_bars:
        return syms
    panel = data.load_panel(as_of_date, LOOKBACK_DAYS)
    return [s for s in syms if len(panel.get(s, [])) >= min_bars]


# ------------------------------------------------------------------------------------ scan
def scan_universe_detailed(as_of_date, pattern_ids=None, min_bars: int = MIN_BARS,
                           lookback_days: int = LOOKBACK_DAYS) -> dict:
    """Full-universe live-stage scan for ``as_of_date``, strictly point-in-time, WITH honest coverage
    accounting. Returns:
        {as_of_date, trading_day, note, universe_size, scanned, count,
         skipped_min_bars, skipped_stale, skipped_no_window, setups}

    FRESHNESS GUARD (auditor FIX 1): a symbol is classified only if its last available bar is dated
    EXACTLY on as_of (it actually traded on D). "Last bar <= D" would relabel a prior session's
    setups as fresh for a holiday/non-trading D and would violate "entry = next open AFTER the signal
    bar" (the signal bar would already have closed). If the WHOLE market didn't trade on D
    (market_max < D), the result is honestly EMPTY with a note — never a relabeled prior session.

    COVERAGE ACCOUNTING (auditor FIX 2): every universe symbol lands in exactly one bucket —
    scanned + skipped_min_bars + skipped_stale + skipped_no_window == universe_size — so the
    1561-vs-classified gap is labelled, not hidden.

    Each setup dict: {symbol, pattern, stage, level, distance_pct, volume_x, touches, direction,
    as_of_date}. Horizontal is REAL; triangle/channel (SPEC skeletons) contribute nothing — honestly.
    Fully guarded: a bad symbol or a bad detector is skipped, never fatal (additive + guarded)."""
    as_of = str(as_of_date)
    as_of_ts = pd.Timestamp(as_of_date).normalize()
    empty = {"as_of_date": as_of, "trading_day": False, "note": None, "universe_size": 0,
             "scanned": 0, "count": 0, "skipped_min_bars": 0, "skipped_stale": 0,
             "skipped_no_window": 0, "setups": []}
    if not data.db_available():
        log.warning("scan_universe: data source unavailable (%s).", data.db_path())
        empty["note"] = f"data source unavailable ({data.db_path()})"
        return empty

    universe = list(data.all_symbols())
    panel = data.load_panel(as_of_date, lookback_days)          # {sym -> frame}, bars <= as_of ONLY
    market_max = max((df.index.max() for df in panel.values() if len(df)), default=None)
    trading_day = market_max is not None and market_max.normalize() == as_of_ts

    dets = [d for d in patterns.all_patterns()
            if pattern_ids is None or d.pattern_id in set(pattern_ids)]

    out: list = []
    scanned = skipped_min_bars = skipped_stale = skipped_no_window = 0
    for sym in universe:
        df = panel.get(sym)
        if df is None or len(df) == 0:                          # no bars in the point-in-time window
            skipped_no_window += 1
            continue
        if len(df) < min_bars:                                  # too little history <= as_of
            skipped_min_bars += 1
            continue
        if df.index[-1].normalize() != as_of_ts:               # didn't trade ON D (FIX 1 freshness)
            skipped_stale += 1
            continue
        scanned += 1
        df.attrs["symbol"] = sym
        k = len(df) - 1                                         # the D bar (traded on D)
        for det in dets:
            try:
                for occ in det.detect(df, as_of_idx=k):
                    d = occ.to_dict()
                    ctx = d.get("context") or {}
                    out.append({
                        "symbol": d.get("stock"),
                        "pattern": d.get("pattern"),
                        "stage": d.get("stage"),
                        "level": d.get("level"),
                        "distance_pct": ctx.get("distance_to_level_pct"),
                        "volume_x": ctx.get("volume_x"),
                        "touches": len(d.get("touches") or []),
                        "direction": d.get("direction", "long"),
                        "as_of_date": ctx.get("as_of_date"),
                    })
            except Exception as e:  # noqa: BLE001 — one bad detector/symbol must not sink the scan
                log.warning("scan_universe: %s/%s failed (non-fatal): %s",
                            sym, getattr(det, "pattern_id", "?"), e)
    out.sort(key=lambda r: (_STAGE_ORDER.get(r.get("stage"), 9), -(r.get("volume_x") or 0)))

    note = None
    if not trading_day:
        last = None if market_max is None else str(market_max.date())
        note = (f"no trading on {as_of} (market last traded {last}) — empty screen, "
                f"no prior session relabeled")
    return {"as_of_date": as_of, "trading_day": trading_day, "note": note,
            "universe_size": len(universe), "scanned": scanned, "count": len(out),
            "skipped_min_bars": skipped_min_bars, "skipped_stale": skipped_stale,
            "skipped_no_window": skipped_no_window, "setups": out}


def scan_universe(as_of_date, pattern_ids=None, min_bars: int = MIN_BARS,
                  lookback_days: int = LOOKBACK_DAYS) -> list:
    """The setups list for ``as_of_date`` (back-compat surface). See scan_universe_detailed for the
    freshness guard + coverage accounting. Empty on a non-trading D (never a relabeled prior session)."""
    return scan_universe_detailed(as_of_date, pattern_ids, min_bars, lookback_days)["setups"]


# ------------------------------------------------------------------------------- precompute store
# Source-agnostic seam: the cloud session points this at S3/EFS later by setting AGENT_CHART_SCREEN_URI
# (an S3 adapter is a thin [SPEC] stub below). The store holds SETUPS ONLY — never the 65 GB DB.

def _default_dir() -> str:
    # backend/agents/chart/screener.py -> backend/var/chart_screens (local dev default)
    backend = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(backend, "var", "chart_screens")


class LocalScreenStore:
    """[BUILT] Cheap per-date JSON artifacts on the local filesystem."""

    def __init__(self, root: str):
        self.root = root

    def _path(self, as_of: str) -> str:
        return os.path.join(self.root, f"screen_{as_of}.json")

    def write(self, as_of: str, payload: dict) -> str:
        os.makedirs(self.root, exist_ok=True)
        path = self._path(as_of)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"))
        return path

    def read(self, as_of: str):
        path = self._path(as_of)
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as f:
            return json.load(f)


class S3ScreenStore:
    """[BUILT] S3/EFS screen store (boto3 — already in the cloud image). Post-market precompute
    writes the per-date artifact to shared storage; every serving task reads it (so cloud serves the
    fast ~ms path, not a ~3s live compute). read() is GUARDED — a missing key or any S3 error returns
    None so the endpoint falls back to live-compute, never a crash. Key: ``<prefix>/screen_<date>.json``."""

    def __init__(self, uri: str):
        self.uri = uri.rstrip("/")
        rest = self.uri[len("s3://"):] if self.uri.startswith("s3://") else self.uri
        self.bucket, _, self.prefix = rest.partition("/")

    def _key(self, as_of: str) -> str:
        pre = (self.prefix + "/") if self.prefix else ""
        return f"{pre}screen_{as_of}.json"

    def write(self, as_of: str, payload: dict) -> str:
        import boto3
        key = self._key(as_of)
        boto3.client("s3").put_object(
            Bucket=self.bucket, Key=key,
            Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json")
        return f"s3://{self.bucket}/{key}"

    def read(self, as_of: str):
        try:
            import boto3
            obj = boto3.client("s3").get_object(Bucket=self.bucket, Key=self._key(as_of))
            return json.loads(obj["Body"].read())
        except Exception as e:  # noqa: BLE001 — NoSuchKey / creds / network -> honest miss, never raise
            log.debug("S3ScreenStore.read(%s) miss/err: %s", as_of, e)
            return None


def _store():
    uri = os.environ.get("AGENT_CHART_SCREEN_URI", "")
    if uri.startswith("s3://"):
        return S3ScreenStore(uri)
    root = os.environ.get("AGENT_CHART_SCREEN_DIR") or _default_dir()
    return LocalScreenStore(root)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _horizontal_params() -> dict:
    """The exact detector thresholds baked into the artifact (audit/provenance). Guarded."""
    try:
        from .patterns.horizontal_trendline import PARAMS
        return {k: (None if v is None else v) for k, v in PARAMS.items()}
    except Exception:  # noqa: BLE001
        return {}


def build_screen(as_of_date, pattern_ids=None) -> dict:
    """POST-MARKET precompute: scan the full universe for ``as_of_date`` and write the per-date
    artifact (setups + coverage accounting + a note on non-trading days). Returns the payload.
    MEASURES its own wall-clock. Store-write failures are reported honestly, never fatal (FIX 3)."""
    as_of = str(as_of_date)
    t0 = time.perf_counter()
    res = scan_universe_detailed(as_of_date, pattern_ids)
    elapsed = time.perf_counter() - t0
    payload = {
        "as_of_date": as_of,
        "built_at": _now_iso(),
        "source": data.db_path(),
        "trading_day": res["trading_day"],
        "note": res["note"],
        "universe_size": res["universe_size"],
        "scanned": res["scanned"],
        "skipped_min_bars": res["skipped_min_bars"],
        "skipped_stale": res["skipped_stale"],
        "skipped_no_window": res["skipped_no_window"],
        "count": res["count"],
        "elapsed_sec": round(elapsed, 3),
        "params": _horizontal_params(),
        "setups": res["setups"],
    }
    # Persist even a non-trading-day screen — but EXPLICITLY empty with its note (FIX 1): serving that
    # is honest; live-recomputing a holiday every request is wasteful and would still be empty.
    try:
        path = _store().write(as_of, payload)
        payload["stored_at"] = path
    except Exception as e:  # noqa: BLE001 — a broken store must not crash the precompute
        payload["store_error"] = f"{type(e).__name__}: {e}"
        log.warning("build_screen(%s): store write failed (non-fatal): %s", as_of, e)
    log.info("build_screen(%s): %d setups | scanned %d/%d (min_bars %d, stale %d, no_window %d) "
             "in %.2fs -> %s", as_of, payload["count"], payload["scanned"], payload["universe_size"],
             payload["skipped_min_bars"], payload["skipped_stale"], payload["skipped_no_window"],
             elapsed, payload.get("stored_at") or payload.get("store_error"))
    return payload


def load_screen(as_of_date):
    """Serve the precomputed screen for ``as_of_date`` (None if absent). <1 s on the request path."""
    if as_of_date is None:
        return None
    return _store().read(str(as_of_date))
