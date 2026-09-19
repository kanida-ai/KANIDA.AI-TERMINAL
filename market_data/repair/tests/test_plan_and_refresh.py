"""Plan construction and plan execution.  Uses W1's FakeProvider; no network."""

from __future__ import annotations

import gzip
import json
from datetime import date, datetime

import pytest

from market_data.fake_provider import FakeProvider
from market_data.repair import plan as P
from market_data.repair import refresh as R


# ---------------------------------------------------------------------------
# chunking / caps
# ---------------------------------------------------------------------------

def test_chunk_respects_the_provider_day_cap():
    reqs = P.chunk("X", 1, "15minute", date(2020, 1, 1), date(2021, 1, 1),
                   "gap_fill", 200)
    assert len(reqs) == 2
    assert reqs[0].start == "2020-01-01" and reqs[0].end == "2020-07-18"
    assert reqs[1].start == "2020-07-19" and reqs[1].end == "2021-01-01"
    # contiguous, no overlap
    assert date.fromisoformat(reqs[1].start) - date.fromisoformat(reqs[0].end) == \
        (date(2020, 7, 19) - date(2020, 7, 18))
    assert all(r.days <= 200 for r in reqs)


def test_chunk_of_a_single_day_is_one_request():
    reqs = P.chunk("X", 1, "day", date(2020, 1, 1), date(2020, 1, 1), "probe", 2000)
    assert len(reqs) == 1 and reqs[0].days == 1


def test_request_key_is_deterministic_and_unique():
    a = P.FetchRequest("X", 1, "15minute", "2020-01-01", "2020-02-01", "gap_fill")
    b = P.FetchRequest("X", 1, "15minute", "2020-01-01", "2020-02-01", "gap_fill")
    c = P.FetchRequest("X", 1, "15minute", "2020-01-01", "2020-02-01", "audit_sample")
    assert a.key == b.key and a.key != c.key
    assert R.request_id_for(a) == R.request_id_for(b)
    assert R.request_id_for(a) != R.request_id_for(c)


# ---------------------------------------------------------------------------
# estimates
# ---------------------------------------------------------------------------

def test_estimate_is_the_rate_limit_floor():
    plan = P.FetchPlan("now", "kite", 3.0, {"15minute": 200},
                       requests=[P.FetchRequest("X", 1, "15minute", "2020-01-01",
                                                "2020-01-02", "gap_fill")] * 900)
    assert plan.estimated_seconds() == pytest.approx(300.0)
    assert plan.to_dict()["estimated_minutes_at_rate_limit"] == 5.0
    assert "estimated wall clock" in plan.render()


def test_by_purpose_counts_symbols_not_requests():
    reqs = (P.chunk("A", 1, "15minute", date(2020, 1, 1), date(2021, 1, 1), "gap_fill", 200)
            + P.chunk("B", 2, "15minute", date(2020, 1, 1), date(2020, 2, 1), "gap_fill", 200))
    plan = P.FetchPlan("now", "kite", 3.0, {}, requests=reqs)
    bp = plan.by_purpose()["gap_fill"]
    assert bp["requests"] == 3 and bp["symbols"] == 2


# ---------------------------------------------------------------------------
# execution against the fake provider
# ---------------------------------------------------------------------------

class _FakeStore:
    """The smallest thing that satisfies what refresh calls on W2's store."""

    def __init__(self):
        self.archived: list[dict] = []
        self.candles: list[dict] = []
        self.order: list[str] = []
        self.runs: list[tuple] = []
        self.resolved = {"start_run": self._start, "finish_run": self._finish}

    def _start(self, run_id, **kw):
        self.runs.append(("start", run_id, kw))

    def _finish(self, run_id, **kw):
        self.runs.append(("finish", run_id, kw))

    def archive_raw(self, **kw):
        self.archived.append(kw)
        self.order.append(f"archive:{kw['symbol']}:{kw['start']}")
        return kw.get("request_id")

    def upsert_candles(self, symbol, instrument_id, bars, **kw):
        self.candles.append({"symbol": symbol, "instrument_id": instrument_id,
                             "n": len(list(bars)), **kw})
        self.order.append(f"write:{symbol}")
        return len(self.candles[-1] and bars), 2

    def has(self, op):
        return op in ("record_finding",)

    def close(self):
        pass


def _plan_for(symbols, purpose="gap_fill", tf="15minute"):
    reqs = [P.FetchRequest(s, i + 1, tf, "2024-01-01", "2024-01-10", purpose)
            for i, s in enumerate(symbols)]
    return P.FetchPlan(datetime.now().isoformat(), "fake", 1000.0,
                       {"15minute": 30, "day": 365}, requests=reqs)


def test_refresh_archives_before_writing_and_records_provenance(tmp_path):
    provider = FakeProvider()
    store = _FakeStore()
    plan = _plan_for(["RELIANCE"])
    stats = R.run(plan, store=store, provider=provider,
                  archive_root=tmp_path / "raw", out_dir=tmp_path / "out")

    assert stats.requests_sent == 1
    assert stats.requests_failed == 0
    assert stats.rows_fetched > 0
    # archive first, candles second -- provenance can never be missing
    assert store.order[0].startswith("archive:")
    assert store.order[1].startswith("write:")

    a = store.archived[0]
    assert a["sha256"] and a["request_id"]
    payload = json.loads(gzip.decompress(open(a["payload_path"], "rb").read()))
    assert payload["request_id"] == a["request_id"]
    assert payload["row_count"] == len(payload["rows"]) == a["rows"]
    # the sha in the archive row is the sha of the bytes on disk
    import hashlib
    raw = gzip.decompress(open(a["payload_path"], "rb").read())
    assert hashlib.sha256(raw).hexdigest() == a["sha256"]

    c = store.candles[0]
    assert c["vendor_id"] == "fake"
    assert c["adjustment_basis_id"]            # basis is always recorded
    assert c["source_request_id"] == a["request_id"]
    assert c["run_id"] == stats.run_id


def test_refresh_never_writes_daily_bars_into_the_15m_table(tmp_path):
    store = _FakeStore()
    plan = _plan_for(["RELIANCE"], purpose="daily_crosscheck", tf="day")
    R.run(plan, store=store, provider=FakeProvider(),
          archive_root=tmp_path / "raw", out_dir=tmp_path / "out")
    assert len(store.archived) == 1           # the daily payload is archived
    assert store.candles == []                # ...but never lands in candles_15m


def test_refresh_is_resumable(tmp_path, monkeypatch):
    store = _FakeStore()
    plan = _plan_for(["RELIANCE", "INFY"])
    done = {R.request_id_for(plan.requests[0])}
    monkeypatch.setattr(R, "already_done", lambda s, ids: done)
    stats = R.run(plan, store=store, provider=FakeProvider(),
                  archive_root=tmp_path / "raw", out_dir=tmp_path / "out")
    assert stats.requests_skipped == 1
    assert stats.requests_sent == 1
    assert {a["symbol"] for a in store.archived} == {"INFY"}


def test_refresh_records_an_empty_response_without_inventing_bars(tmp_path):
    provider = FakeProvider(symbols=("RELIANCE",))
    store = _FakeStore()
    plan = _plan_for(["NOTLISTED"])
    stats = R.run(plan, store=store, provider=provider,
                  archive_root=tmp_path / "raw", out_dir=tmp_path / "out")
    # either the provider refuses the unknown symbol or it returns nothing;
    # in neither case is a bar fabricated
    assert stats.rows_written == 0
    assert store.candles == []
    assert stats.requests_failed + stats.empty_responses == 1


def test_visibility_probe_result_is_recorded(tmp_path):
    store = _FakeStore()
    plan = P.FetchPlan(datetime.now().isoformat(), "fake", 1000.0, {},
                       requests=[P.FetchRequest("LTIM", 0, "15minute",
                                                "2024-01-01", "2024-01-05",
                                                "visibility_probe")])
    stats = R.run(plan, store=store, provider=FakeProvider(),
                  archive_root=tmp_path / "raw", out_dir=tmp_path / "out")
    assert "LTIM" in stats.visibility
