"""
Chart Agent · PER-SETUP PRECOMPUTE (option B) tests.

The EOD job precomputes ONE self-contained bundle per (symbol,pattern,date) — the full build_setup
detail PLUS the drawing bars — so a column-3 click serves in MS (both /setup and /bars) from a single
object read instead of a live re-detect+replay. These tests cover:

  (a) LocalSetupStore write/read round-trip + guarded miss (no DB needed).
  (b) S3SetupStore key scheme, guarded miss, faked-client round-trip (no DB needed).
  (c) DERIVED S3 setup URI = screen URI + "/setups" (nested under the screen prefix; reuses IAM).
  (d) precompute -> store -> fast-serve == live build_setup for the same key fields (DB).
  (e) bars fast-path: bundle-sliced bars == live build_bars (DB).
  (f) endpoint served flags: precompute when the bundle exists, live fallback on a miss (DB).

Runnable two ways:
    pytest backend/agents/chart/tests/test_setup_precompute.py
    python  backend/agents/chart/tests/test_setup_precompute.py     (pass/fail + timing summary)

DB-dependent tests skip gracefully when the R&D DB is absent. Run against the R&D DB via:
    AGENT_CHART_DB=C:/Users/SPS/Documents/Kanida_Falcon/db/kanida.db
"""
from __future__ import annotations
import os
import sys
import json
import types
import tempfile
import shutil
import time

_BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

from agents import registry                                    # noqa: E402
from agents.chart import data                                  # noqa: E402
from agents.chart import screener as scr                       # noqa: E402
from agents.chart import setup as csetup                       # noqa: E402

registry.load_builtin()

TITAN_DATE = "2022-08-30"
PATTERN = "horizontal_trendline"

# key fields a served bundle must reproduce exactly vs the live build_setup output
KEY_FIELDS = ("ok", "stage", "direction", "level", "sector", "geometry", "quality",
              "evidence", "paths", "decision", "watch_plan", "as_of_date")


def _skip(name: str) -> bool:
    if not data.db_available():
        print(f"SKIP {name} — DB absent: {data.db_path()}")
        return True
    return False


def _tmp_setup_dir():
    tmp = tempfile.mkdtemp(prefix="chart_setups_")
    saved = os.environ.get("AGENT_CHART_SETUP_DIR")
    os.environ["AGENT_CHART_SETUP_DIR"] = tmp
    return tmp, saved


def _restore_setup_dir(tmp, saved):
    if saved is None:
        os.environ.pop("AGENT_CHART_SETUP_DIR", None)
    else:
        os.environ["AGENT_CHART_SETUP_DIR"] = saved
    shutil.rmtree(tmp, ignore_errors=True)


# (a) local store round-trip + guarded miss --------------------------------------------------------
def test_local_setup_store_roundtrip_and_miss():
    tmp = tempfile.mkdtemp(prefix="chart_setups_local_")
    try:
        st = scr.LocalSetupStore(tmp)
        assert st.read("2022-08-30", "TITAN", PATTERN) is None    # clean miss before any write
        bundle = {"ok": True, "stage": "BREAKOUT", "level": 2565.0, "bars": [{"date": "2022-08-30"}]}
        path = st.write("2022-08-30", "TITAN", PATTERN, bundle)
        assert path.endswith(os.path.join("2022-08-30", f"TITAN_{PATTERN}.json")), path
        got = st.read("2022-08-30", "TITAN", PATTERN)
        assert got == bundle, "local round-trip differs"
        assert st.read("2022-08-30", "TITAN", "cup_and_handle") is None   # other pattern -> miss
        assert st.read("1990-01-02", "TITAN", PATTERN) is None            # other date -> miss
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return "local per-setup store: round-trip + guarded miss"


# (b) S3 store key scheme + guarded miss + faked round-trip -----------------------------------------
def test_s3_setup_store_guarded_and_roundtrip():
    st = scr.S3SetupStore("s3://no-such-bucket-xyzzy/kanida/chart_screens/setups")
    assert st.bucket == "no-such-bucket-xyzzy"
    assert st.prefix == "kanida/chart_screens/setups"
    assert st._key("2022-08-30", "TITAN", PATTERN) == \
        f"kanida/chart_screens/setups/2022-08-30/TITAN_{PATTERN}.json"
    # guarded: missing bucket/creds/boto3 -> None, never raises
    assert st.read("2022-08-30", "TITAN", PATTERN) is None

    blob = {}

    class _FakeS3:
        def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
            blob[(Bucket, Key)] = Body
        def get_object(self, Bucket, Key):  # noqa: N803
            if (Bucket, Key) not in blob:
                raise KeyError("NoSuchKey")
            return {"Body": types.SimpleNamespace(read=lambda: blob[(Bucket, Key)])}

    fake = types.ModuleType("boto3")
    fake.client = lambda svc: _FakeS3()
    saved = sys.modules.get("boto3")
    try:
        sys.modules["boto3"] = fake
        uri = st.write("2022-08-30", "TITAN", PATTERN, {"stage": "BREAKOUT", "bars": []})
        assert uri == f"s3://no-such-bucket-xyzzy/kanida/chart_screens/setups/2022-08-30/TITAN_{PATTERN}.json"
        got = st.read("2022-08-30", "TITAN", PATTERN)
        assert got and got["stage"] == "BREAKOUT"
        assert st.read("1990-01-02", "TITAN", PATTERN) is None   # still a clean miss
    finally:
        if saved is None:
            sys.modules.pop("boto3", None)
        else:
            sys.modules["boto3"] = saved
    return "s3 per-setup store: key scheme + guarded miss + faked round-trip"


# (c) DERIVED S3 setup URI nests under the screen prefix (reuses IAM) -------------------------------
def test_setup_store_derives_from_screen_uri():
    saved = {k: os.environ.get(k) for k in
             ("AGENT_CHART_SETUP_URI", "AGENT_CHART_SCREEN_URI", "AGENT_CHART_SETUP_DIR")}
    try:
        for k in saved:
            os.environ.pop(k, None)
        os.environ["AGENT_CHART_SCREEN_URI"] = "s3://kanida-cb-src/kanida/chart_screens"
        store = scr._setup_store()
        assert isinstance(store, scr.S3SetupStore), type(store)
        assert store.prefix == "kanida/chart_screens/setups", store.prefix
        assert store._key("2022-08-30", "TITAN", PATTERN) == \
            f"kanida/chart_screens/setups/2022-08-30/TITAN_{PATTERN}.json"
        # explicit AGENT_CHART_SETUP_URI wins over the derivation
        os.environ["AGENT_CHART_SETUP_URI"] = "s3://other/prefix"
        store2 = scr._setup_store()
        assert store2.bucket == "other" and store2.prefix == "prefix"
        # no S3 env at all -> Local
        os.environ.pop("AGENT_CHART_SETUP_URI", None)
        os.environ.pop("AGENT_CHART_SCREEN_URI", None)
        assert isinstance(scr._setup_store(), scr.LocalSetupStore)
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return "setup URI derives to screen+/setups; explicit override + local fallback"


# (d) precompute -> store -> fast-serve == live build_setup (key fields) ----------------------------
def test_precompute_matches_live_build_setup():
    if _skip("test_precompute_matches_live_build_setup"):
        return "SKIP"
    tmp, saved = _tmp_setup_dir()
    try:
        # run the REAL precompute path on a single known setup (TITAN horizontal breakout)
        summary = scr._precompute_setups(TITAN_DATE, [{"symbol": "TITAN", "pattern": PATTERN}])
        assert summary["precomputed"] == 1 and summary["errors"] == 0, summary
        assert summary["elapsed_sec"] >= 0

        served = scr.load_setup_bundle(TITAN_DATE, "TITAN", PATTERN)
        assert served is not None, "bundle not served after precompute"
        assert served["stage"] == "BREAKOUT", served.get("stage")
        assert isinstance(served.get("bars"), list) and served["bars"], "bars not embedded"
        assert served.get("bars_lookback") == scr.PRECOMPUTE_BARS_LOOKBACK
        # REAL sector carried through (instrument_labels) — TITAN is labelled, not fabricated
        assert served.get("sector") == data.sector_map().get("TITAN"), served.get("sector")

        # The served bundle is what the store round-trips through JSON, so the correct equivalence is
        # served == json-serialization of the live build (canonicalizes int-keyed horizon dicts and
        # numpy floats identically — the wire payload is byte-for-byte the same).
        live = json.loads(json.dumps(csetup.build_setup("TITAN", PATTERN, TITAN_DATE)))
        for f in KEY_FIELDS:
            assert served.get(f) == live.get(f), f"served vs live differ on {f!r}"

        # guarded miss -> None (fast-path would then live-fall-back at the endpoint)
        assert scr.load_setup_bundle(TITAN_DATE, "TITAN", "cup_and_handle") is None
        assert scr.load_setup_bundle(None, "TITAN", PATTERN) is None
        return (f"precompute==live over {len(KEY_FIELDS)} key fields; "
                f"{len(served['bars'])} bars embedded ({summary['elapsed_sec']}s/setup)")
    finally:
        _restore_setup_dir(tmp, saved)


# (e) bars fast-path: bundle slice == live build_bars ----------------------------------------------
def test_bars_fast_path_matches_live():
    if _skip("test_bars_fast_path_matches_live"):
        return "SKIP"
    tmp, saved = _tmp_setup_dir()
    try:
        scr._precompute_setups(TITAN_DATE, [{"symbol": "TITAN", "pattern": PATTERN}])
        bundle = scr.load_setup_bundle(TITAN_DATE, "TITAN", PATTERN)
        assert bundle and bundle["bars"], "no embedded bars"
        # slice the last 180 (UI default) from the bundle and compare to a live 180-bar read
        sliced = bundle["bars"][-180:]
        live_bars = csetup.build_bars("TITAN", TITAN_DATE, 180)
        assert sliced == live_bars, "bundle-sliced bars != live build_bars"
        assert len(live_bars) == 180, len(live_bars)
        return f"bars fast-path: last-180 slice byte-identical to live build_bars"
    finally:
        _restore_setup_dir(tmp, saved)


# (f) endpoint served flags: precompute when present, live on a miss -------------------------------
def test_endpoints_served_flags():
    if _skip("test_endpoints_served_flags"):
        return "SKIP"
    from agents import router
    tmp, saved = _tmp_setup_dir()
    try:
        scr._precompute_setups(TITAN_DATE, [{"symbol": "TITAN", "pattern": PATTERN}])

        # /setup — precomputed key -> served precompute (instant), carries embedded bars
        s_fast = router.chart_setup(symbol="TITAN", pattern=PATTERN, date=TITAN_DATE)
        assert s_fast.get("served") == "precompute", s_fast.get("served")
        assert s_fast.get("stage") == "BREAKOUT"
        assert isinstance(s_fast.get("bars"), list) and s_fast["bars"]

        # /setup — un-precomputed key -> honest live fallback
        s_live = router.chart_setup(symbol="TITAN", pattern="cup_and_handle", date=TITAN_DATE)
        assert s_live.get("served") == "live", s_live.get("served")

        # /bars with pattern -> precompute; without pattern -> live
        b_fast = router.chart_bars(symbol="TITAN", date=TITAN_DATE, lookback=180, pattern=PATTERN)
        assert b_fast.get("served") == "precompute", b_fast.get("served")
        assert b_fast["count"] == 180 and b_fast["bars"]
        b_live = router.chart_bars(symbol="TITAN", date=TITAN_DATE, lookback=180)
        assert b_live.get("served") == "live", b_live.get("served")
        # the two agree on the actual candles (fast path is a pure slice of the same PIT bars)
        assert b_fast["bars"] == b_live["bars"], "fast vs live bars differ"

        # /bars fast path with a lookback deeper than the bundle -> live fallback (correctness first)
        b_deep = router.chart_bars(symbol="TITAN", date=TITAN_DATE,
                                   lookback=scr.PRECOMPUTE_BARS_LOOKBACK + 50, pattern=PATTERN)
        assert b_deep.get("served") == "live", b_deep.get("served")
        return "endpoints: precompute when bundled, honest live fallback on miss/deep-lookback"
    finally:
        _restore_setup_dir(tmp, saved)


if __name__ == "__main__":
    results = []
    for fn in (test_local_setup_store_roundtrip_and_miss,
               test_s3_setup_store_guarded_and_roundtrip,
               test_setup_store_derives_from_screen_uri,
               test_precompute_matches_live_build_setup,
               test_bars_fast_path_matches_live,
               test_endpoints_served_flags):
        t0 = time.perf_counter()
        try:
            r = fn()
            results.append((fn.__name__, "PASS", r, time.perf_counter() - t0))
        except Exception as e:  # noqa: BLE001
            results.append((fn.__name__, "FAIL", repr(e), time.perf_counter() - t0))
    print("\n=== Chart Agent per-setup precompute tests ===")
    for name, status, detail, secs in results:
        print(f"  [{status}] {name} ({secs:.2f}s)" + (f"  -> {detail}" if detail is not None else ""))
    if any(s == "FAIL" for _, s, _, _ in results):
        sys.exit(1)
