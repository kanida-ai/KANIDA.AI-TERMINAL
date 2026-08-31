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

# Sample bar for "statistically meaningful" (== §9 G1). Imported lazily to keep boot cheap/guarded.
try:
    from .evidence import N_MIN as _MEANINGFUL_N
except Exception:  # noqa: BLE001
    _MEANINGFUL_N = 20


def _enrich_row(row: dict, occ_dict: dict) -> None:
    """Attach REAL storyline enrichment (tier, quality_score, evidence_summary, hook) to a setup row.
    Guarded — enrichment never sinks the scan. Reuses the occurrence so the detector isn't re-run."""
    try:
        from . import setup as _setup
        e = _setup.enrich(row.get("symbol"), row.get("pattern"), row.get("as_of_date"), occ_dict)
        row["tier"] = e.get("tier")
        row["quality_score"] = e.get("quality_score")
        row["evidence_summary"] = e.get("evidence_summary")
        row["hook"] = e.get("hook")
    except Exception as ex:  # noqa: BLE001
        log.warning("enrich row %s/%s failed (non-fatal): %s",
                    row.get("symbol"), row.get("pattern"), ex)


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
                           lookback_days: int = LOOKBACK_DAYS, enrich: bool = False) -> dict:
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

    # REAL sector map (instrument_labels) — one lookup reused across every row; {} when no sector
    # source exists (e.g. a Parquet-only cloud source) -> honest None per row, never fabricated.
    sectors = data.sector_map()

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
                    row = {
                        "symbol": d.get("stock"),
                        "pattern": d.get("pattern"),
                        "stage": d.get("stage"),
                        "level": d.get("level"),
                        "distance_pct": ctx.get("distance_to_level_pct"),
                        "volume_x": ctx.get("volume_x"),
                        "touches": len(d.get("touches") or []),
                        "direction": d.get("direction", "long"),
                        "sector": sectors.get(sym),
                        "as_of_date": ctx.get("as_of_date"),
                    }
                    if enrich:
                        _enrich_row(row, d)
                    out.append(row)
            except Exception as e:  # noqa: BLE001 — one bad detector/symbol must not sink the scan
                log.warning("scan_universe: %s/%s failed (non-fatal): %s",
                            sym, getattr(det, "pattern_id", "?"), e)
    out.sort(key=lambda r: (_STAGE_ORDER.get(r.get("stage"), 9), -(r.get("volume_x") or 0)))

    note = None
    if not trading_day:
        last = None if market_max is None else str(market_max.date())
        note = (f"no trading on {as_of} (market last traded {last}) — empty screen, "
                f"no prior session relabeled")

    # storyline summary counts (REAL, from this scan). statistically_meaningful/qualified are only
    # populated when enrich=True (they need the per-setup evidence); else honestly None.
    statistically_meaningful = qualified = None
    if enrich:
        statistically_meaningful = sum(1 for r in out
                                       if (r.get("evidence_summary") or {}).get("n", 0) >= _MEANINGFUL_N)
        qualified = sum(1 for r in out if r.get("tier") == "qualified")

    try:
        from . import story as _story
        market = _story.market_story(out, as_of)
    except Exception as e:  # noqa: BLE001 — story is best-effort, never fatal
        log.warning("market_story failed (non-fatal): %s", e)
        market = None

    return {"as_of_date": as_of, "trading_day": trading_day, "note": note,
            "universe_size": len(universe), "scanned": scanned, "count": len(out),
            "skipped_min_bars": skipped_min_bars, "skipped_stale": skipped_stale,
            "skipped_no_window": skipped_no_window,
            "enriched": bool(enrich),
            "statistically_meaningful": statistically_meaningful, "qualified": qualified,
            "market_story": market, "setups": out}


def scan_universe(as_of_date, pattern_ids=None, min_bars: int = MIN_BARS,
                  lookback_days: int = LOOKBACK_DAYS, enrich: bool = False) -> list:
    """The setups list for ``as_of_date`` (back-compat surface). See scan_universe_detailed for the
    freshness guard + coverage accounting. Empty on a non-trading D (never a relabeled prior session)."""
    return scan_universe_detailed(as_of_date, pattern_ids, min_bars, lookback_days, enrich)["setups"]


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


# ------------------------------------------------------------------- per-setup EVIDENCE store (option B)
# Post-market the EOD job precomputes ONE self-contained bundle per (symbol,pattern,date) — the full
# build_setup output PLUS the drawing bars — so a column-3 click serves in MS from a single object read
# (both /setup AND /bars) instead of a live re-detect+replay+parquet-read. Store abstraction mirrors the
# screen store so embed-in-screen vs per-setup-objects stays swappable.
#
# Key scheme: ``<prefix>/<date>/<symbol>_<pattern>.json`` (option B = per-setup objects).
#   S3 (cloud):  AGENT_CHART_SETUP_URI, else DERIVED from AGENT_CHART_SCREEN_URI + "/setups" so it lands
#                UNDER the existing screen prefix (e.g. s3://<bucket>/kanida/chart_screens/setups/...),
#                reusing the task-role IAM already granted on chart_screens/* — ZERO new grant.
#   Local (dev): AGENT_CHART_SETUP_DIR, else backend/var/chart_setups. Same key scheme.
# read() is GUARDED -> None on any miss/error so the endpoint falls back to a live compute, never crashes.

# Bars embedded in each bundle. 250 >= the 180-bar UI default so /bars can slice from the bundle.
PRECOMPUTE_BARS_LOOKBACK = 250


def _default_setup_dir() -> str:
    backend = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(backend, "var", "chart_setups")


def _safe(name: str) -> str:
    """Filesystem/-key-safe token (symbols/patterns are alnum + _/- already; guard the odd space)."""
    return "".join(ch if (ch.isalnum() or ch in "-_.") else "_" for ch in str(name))


class LocalSetupStore:
    """[BUILT] Per-setup JSON bundles on the local filesystem (dev default).
    Path: ``<root>/<date>/<symbol>_<pattern>.json``."""

    def __init__(self, root: str):
        self.root = root

    def _path(self, as_of: str, symbol: str, pattern: str) -> str:
        return os.path.join(self.root, _safe(as_of), f"{_safe(symbol)}_{_safe(pattern)}.json")

    def write(self, as_of: str, symbol: str, pattern: str, payload: dict) -> str:
        path = self._path(as_of, symbol, pattern)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"))
        return path

    def read(self, as_of: str, symbol: str, pattern: str):
        try:
            path = self._path(as_of, symbol, pattern)
            if not os.path.exists(path):
                return None
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:  # noqa: BLE001 — corrupt/locked file -> honest miss, never raise
            log.debug("LocalSetupStore.read(%s/%s/%s) miss/err: %s", as_of, symbol, pattern, e)
            return None


class S3SetupStore:
    """[BUILT] Per-setup S3 bundles (boto3 — already in the cloud image). Post-market precompute writes
    one object per setup; a click reads exactly one (ms). read() is GUARDED — a missing key or any S3
    error returns None so the endpoint live-falls-back, never crashes.
    Key: ``<prefix>/<date>/<symbol>_<pattern>.json``."""

    def __init__(self, uri: str):
        self.uri = uri.rstrip("/")
        rest = self.uri[len("s3://"):] if self.uri.startswith("s3://") else self.uri
        self.bucket, _, self.prefix = rest.partition("/")

    def _key(self, as_of: str, symbol: str, pattern: str) -> str:
        pre = (self.prefix + "/") if self.prefix else ""
        return f"{pre}{_safe(as_of)}/{_safe(symbol)}_{_safe(pattern)}.json"

    def write(self, as_of: str, symbol: str, pattern: str, payload: dict) -> str:
        import boto3
        key = self._key(as_of, symbol, pattern)
        boto3.client("s3").put_object(
            Bucket=self.bucket, Key=key,
            Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json")
        return f"s3://{self.bucket}/{key}"

    def read(self, as_of: str, symbol: str, pattern: str):
        try:
            import boto3
            obj = boto3.client("s3").get_object(
                Bucket=self.bucket, Key=self._key(as_of, symbol, pattern))
            return json.loads(obj["Body"].read())
        except Exception as e:  # noqa: BLE001 — NoSuchKey / creds / network -> honest miss, never raise
            log.debug("S3SetupStore.read(%s/%s/%s) miss/err: %s", as_of, symbol, pattern, e)
            return None


def _setup_store():
    """Resolve the per-setup store. Explicit AGENT_CHART_SETUP_URI wins; else DERIVE the S3 store from
    the screen URI (nest under it as ``.../setups``) so it reuses the screen prefix + IAM; else Local."""
    uri = os.environ.get("AGENT_CHART_SETUP_URI", "")
    if not uri:
        screen_uri = os.environ.get("AGENT_CHART_SCREEN_URI", "")
        if screen_uri.startswith("s3://"):
            uri = screen_uri.rstrip("/") + "/setups"
    if uri.startswith("s3://"):
        return S3SetupStore(uri)
    root = os.environ.get("AGENT_CHART_SETUP_DIR") or _default_setup_dir()
    return LocalSetupStore(root)


def store_setup_bundle(as_of, symbol: str, pattern: str, bundle: dict) -> str:
    """Persist one per-setup bundle. Returns the store path/URI. Raises only on a real store failure
    (the caller — _precompute_setups — guards per-setup so one failure is non-fatal to the screen)."""
    return _setup_store().write(str(as_of), symbol, pattern, bundle)


def load_setup_bundle(as_of, symbol: str, pattern: str):
    """Serve one precomputed per-setup bundle (None on any miss). ms on the request path. Never raises."""
    if as_of is None or not symbol or not pattern:
        return None
    return _setup_store().read(str(as_of), str(symbol), str(pattern))


def _precompute_setups(as_of, setups) -> dict:
    """For EACH screener setup, build the FULL per-setup detail (build_setup) + drawing bars and write
    one self-contained bundle to the per-setup store. Point-in-time is preserved (build_setup + bars are
    both <= as_of). GUARDED per setup — a single failure is recorded and skipped, never fatal to the
    screen. Returns {precomputed, errors, error_sample, elapsed_sec, bars_lookback}."""
    from . import setup as _setup     # lazy: keep boot cheap + import problems non-fatal
    store = _setup_store()
    t0 = time.perf_counter()
    n_ok = n_err = 0
    errors: list = []
    for s in setups:
        sym = s.get("symbol") or s.get("stock")
        pat = s.get("pattern")
        if not sym or not pat:
            continue
        try:
            detail = _setup.build_setup(sym, pat, str(as_of))
            bars = _setup.build_bars(sym, str(as_of), PRECOMPUTE_BARS_LOOKBACK)
            bundle = {**detail, "bars": bars, "bars_lookback": PRECOMPUTE_BARS_LOOKBACK,
                      "precomputed_at": _now_iso()}
            store.write(str(as_of), sym, pat, bundle)
            n_ok += 1
        except Exception as e:  # noqa: BLE001 — one bad setup must not sink the precompute
            n_err += 1
            if len(errors) < 20:
                errors.append(f"{sym}/{pat}: {type(e).__name__}: {e}")
            log.warning("precompute_setup %s/%s failed (non-fatal): %s", sym, pat, e)
    return {"precomputed": n_ok, "errors": n_err, "error_sample": errors,
            "bars_lookback": PRECOMPUTE_BARS_LOOKBACK,
            "elapsed_sec": round(time.perf_counter() - t0, 3)}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _horizontal_params() -> dict:
    """The exact detector thresholds baked into the artifact (audit/provenance). Guarded."""
    try:
        from .patterns.horizontal_trendline import PARAMS
        return {k: (None if v is None else v) for k, v in PARAMS.items()}
    except Exception:  # noqa: BLE001
        return {}


def build_screen(as_of_date, pattern_ids=None, enrich: bool = True,
                 precompute_setups: bool = False) -> dict:
    """POST-MARKET precompute: scan the full universe for ``as_of_date`` and write the per-date
    artifact (setups + coverage accounting + a note on non-trading days). Returns the payload.
    MEASURES its own wall-clock. Store-write failures are reported honestly, never fatal (FIX 3).

    ``enrich`` (default True) attaches the REAL storyline enrichment (tier/quality/evidence hook) to
    every setup so the served artifact powers the 3-column UX without a per-request recompute. The
    enrichment cost is measured in ``elapsed_sec`` (reported).

    ``precompute_setups`` (EOD path) additionally builds ONE self-contained per-setup bundle
    (build_setup detail + drawing bars) for EACH setup and writes it to the per-setup store, so a
    column-3 click serves in MS (both /setup and /bars) from a single object read. Point-in-time
    preserved; GUARDED per setup (a failure is recorded, non-fatal — the screen still builds). Its
    off-request wall-clock is reported under ``setup_precompute``."""
    as_of = str(as_of_date)
    t0 = time.perf_counter()
    res = scan_universe_detailed(as_of_date, pattern_ids, enrich=enrich)
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
        "enriched": res.get("enriched", False),
        "statistically_meaningful": res.get("statistically_meaningful"),
        "qualified": res.get("qualified"),
        "market_story": res.get("market_story"),
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

    # Per-setup evidence bundles (EOD path) — precompute AFTER the screen is safely stored so a
    # precompute problem can never lose the screen. Fully guarded; timing reported (off-request cost).
    if precompute_setups:
        try:
            payload["setup_precompute"] = _precompute_setups(as_of, res["setups"])
            log.info("build_screen(%s): precomputed %d/%d setup bundles in %.2fs (errors=%d)", as_of,
                     payload["setup_precompute"]["precomputed"], payload["count"],
                     payload["setup_precompute"]["elapsed_sec"], payload["setup_precompute"]["errors"])
        except Exception as e:  # noqa: BLE001 — a broken per-setup store must not crash the screen
            payload["setup_precompute"] = {"error": f"{type(e).__name__}: {e}"}
            log.warning("build_screen(%s): setup precompute failed (non-fatal): %s", as_of, e)

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
