"""GTT-OCO per-position stop/target = CAPITAL basis (converted to the broker
PRICE trigger via session leverage).

The knobs per_position_stop_pct / per_position_target_pct are now FRACTIONS OF
THE POSITION'S CAPITAL. GTTMan.place_for_position converts them to a price
fraction via the basket leverage L = Σ(qty*avg_price)/total_allocated_capital
(clamped ≥1) BEFORE calling compute_levels (which stays pure/price-basis):

    price_stop_pct = capital_stop_pct / L

Real money: for anything that sets a real broker stop we assert BOTH the
leveraged case (fires at the right price) AND the fallback case (conservative
widest stop). compute_levels' math/signature is unchanged and NOT retested here
(see test_futures_long_short.py) — only the CALLER's capital→price conversion.
"""
from autotrade.config import TradingSessionConfig, BrokerProfile
from autotrade.monitoring.registry import PositionRegistry
from autotrade.monitoring.gtt_manager import GTTManager
from tests.autotrade.mock_broker import MockBroker


def _prof(pid="zer", product="CNC"):
    return BrokerProfile(profile_id=pid, broker_name="zerodha",
                         allocated_capital=500000.0, order_product=product)


def _cfg(capital, product="CNC", itype="EQ", **kw):
    base = dict(total_allocated_capital=capital, instrument_type=itype,
                order_product=product)
    base.update(kw)
    cfg = TradingSessionConfig(**base)
    return cfg


class _GttSpyBroker(MockBroker):
    """Records every place_gtt_oco call so a test can assert whether — and with
    what levels — a real GTT was placed."""
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.gtt_placed = []   # list of dict(symbol, product, stop, target, direction)

    def place_gtt_oco(self, symbol, qty, stop_price, target_price, last_price,
                      product="CNC", exchange="NSE", order_type="LIMIT",
                      stop_limit_price=None, direction="long"):
        self.gtt_placed.append({"symbol": symbol, "product": product,
                                "stop": stop_price, "target": target_price,
                                "direction": direction})
        return f"GTT-{symbol}"


def _gm(session_id, cfg, prof, broker, reg):
    cfg.broker_profiles = [prof]
    return GTTManager(session_id, cfg, {prof.profile_id: broker}, reg)


# ── config default is now capital-basis 8% / 20% ─────────────────────────────

def test_config_defaults_are_capital_basis_8_and_20():
    cfg = TradingSessionConfig(total_allocated_capital=500000.0)
    assert cfg.per_position_stop_pct == 0.08
    assert cfg.per_position_target_pct == 0.20
    # Still valid fractions in (0, 0.5] (validate() must not reject the defaults).
    cfg.validate()  # raises on failure


# ── _session_leverage math ────────────────────────────────────────────────────

def test_session_leverage_5x(clean_positions):
    """Σnotional = 5 × capital → L = 5.0."""
    cap = 2000.0
    reg = PositionRegistry("sess-lev-5x", cap)
    # one leg: qty 10 × avg 1000 = 10,000 notional = 5 × 2,000 capital
    reg.register(symbol="AAA", broker_profile="zer", qty=10, avg_price=1000.0,
                 product="MTF", instrument_type="MTF")
    cfg = _cfg(cap, product="MTF", itype="MTF")
    gm = _gm("sess-lev-5x", cfg, _prof(product="MTF"),
             _GttSpyBroker(profile=_prof(product="MTF"), dry_run=False), reg)
    assert gm._session_leverage() == 5.0


def test_session_leverage_clamps_to_one_for_cash(clean_positions):
    """A cash (CNC) book where Σnotional ≤ capital → L clamps to 1.0."""
    cap = 2000.0
    reg = PositionRegistry("sess-lev-1x", cap)
    reg.register(symbol="AAA", broker_profile="zer", qty=1, avg_price=1000.0,
                 product="CNC")
    cfg = _cfg(cap, product="CNC")
    gm = _gm("sess-lev-1x", cfg, _prof(), _GttSpyBroker(profile=_prof(),
             dry_run=False), reg)
    assert gm._session_leverage() == 1.0


def test_session_leverage_fallback_is_conservative_one(clean_positions):
    """No open positions OR ≤0 capital → L = 1.0 (price == capital == WIDEST
    stop; a leverage miscompute can never TIGHTEN a real broker stop)."""
    # (a) no open positions.
    reg = PositionRegistry("sess-lev-empty", 2000.0)
    cfg = _cfg(2000.0)
    gm = _gm("sess-lev-empty", cfg, _prof(),
             _GttSpyBroker(profile=_prof(), dry_run=False), reg)
    assert gm._session_leverage() == 1.0
    # (b) zero capital even with an open leg → 1.0 (never divide-by-zero).
    reg2 = PositionRegistry("sess-lev-zerocap", 2000.0)
    reg2.register(symbol="AAA", broker_profile="zer", qty=10, avg_price=1000.0,
                  product="MTF")
    cfg2 = _cfg(2000.0)
    cfg2.total_allocated_capital = 0.0   # force the fallback path
    gm2 = _gm("sess-lev-zerocap", cfg2, _prof(),
              _GttSpyBroker(profile=_prof(), dry_run=False), reg2)
    assert gm2._session_leverage() == 1.0


# ── the actual GTT level conversion ───────────────────────────────────────────

def test_mis_5x_basket_records_capital_converted_levels(clean_positions):
    """MIS 5x basket, entry 1000, 8% CAPITAL stop → PRICE stop 8%/5 = 1.6% →
    stop_trigger 984.0 (NOT 920). MIS is SUPPRESSED (no real broker GTT) but
    records the SAME capital-derived levels for the UI. Target 20%/5 = 4% →
    1040.0."""
    cap = 2000.0
    reg = PositionRegistry("sess-mis-5x", cap)
    reg.register(symbol="INTRA", broker_profile="mis", qty=10, avg_price=1000.0,
                 product="MIS")   # 10 × 1000 = 10,000 = 5 × 2,000 → L = 5
    cfg = _cfg(cap, product="MIS")
    prof = _prof(pid="mis", product="MIS")
    broker = _GttSpyBroker(profile=prof, dry_run=False, ltps={"INTRA": 1000.0})
    gm = _gm("sess-mis-5x", cfg, prof, broker, reg)

    pos = reg.get_open_positions()[0]
    out = gm.place_for_position(pos)

    # MIS → suppressed (record-only), no real broker GTT placed.
    assert out["status"] == "SUPPRESSED_INTRADAY"
    assert out["gtt_id"] is None
    assert broker.gtt_placed == []
    # Capital-converted levels: 984.0 (not the un-levered 920.0), target 1040.0.
    assert out["stop"] == 984.0
    assert out["target"] == 1040.0
    # And the SAME levels are persisted on the row.
    row = reg.get_open_positions()[0]
    assert row["gtt_stop"] == 984.0
    assert row["gtt_target"] == 1040.0


def test_cnc_1x_uses_full_capital_pct(clean_positions):
    """CNC cash 1x (L clamps to 1) → the 8% capital stop is a full 8% PRICE stop
    → stop_trigger 920.0, target 20% → 1200.0. Real GTT PLACED."""
    cap = 2000.0
    reg = PositionRegistry("sess-cnc-1x", cap)
    reg.register(symbol="CARRY", broker_profile="zer", qty=1, avg_price=1000.0,
                 product="CNC")   # 1 × 1000 ≤ 2000 → L clamps to 1
    cfg = _cfg(cap, product="CNC")
    prof = _prof()
    broker = _GttSpyBroker(profile=prof, dry_run=False, ltps={"CARRY": 1000.0})
    gm = _gm("sess-cnc-1x", cfg, prof, broker, reg)

    pos = reg.get_open_positions()[0]
    out = gm.place_for_position(pos)

    assert out["status"] == "PLACED"
    assert out["stop"] == 920.0
    assert out["target"] == 1200.0
    # The real broker GTT got the converted (full-capital) levels.
    assert len(broker.gtt_placed) == 1
    assert broker.gtt_placed[0]["stop"] == 920.0
    assert broker.gtt_placed[0]["target"] == 1200.0


def test_short_direction_inverts_with_converted_pct(clean_positions):
    """A SHORT future still inverts (stop ABOVE entry, target BELOW) AND applies
    the capital→price conversion. 5x basket, entry 1000, 8% capital stop →
    1.6% price stop ABOVE = 1016.0; 20% capital target → 4% price target
    BELOW = 960.0. Real GTT placed with direction='short'."""
    cap = 200.0
    reg = PositionRegistry("sess-short-5x", cap)
    reg.register(symbol="FUTX", broker_profile="zer", qty=1, avg_price=1000.0,
                 product="NRML", instrument_type="FUT", direction="short")
    # 1 × 1000 = 1000 = 5 × 200 → L = 5
    cfg = _cfg(cap, product="NRML", itype="FUT")
    prof = _prof(product="NRML")
    broker = _GttSpyBroker(profile=prof, dry_run=False, ltps={"FUTX": 1000.0})
    gm = _gm("sess-short-5x", cfg, prof, broker, reg)

    pos = reg.get_open_positions()[0]
    out = gm.place_for_position(pos)

    assert out["status"] == "PLACED"
    # SHORT inverts: stop ABOVE entry, target BELOW.
    assert out["stop"] == 1016.0
    assert out["target"] == 960.0
    assert out["stop"] > 1000.0 > out["target"]
    assert broker.gtt_placed[0]["direction"] == "short"
    assert broker.gtt_placed[0]["stop"] == 1016.0
    assert broker.gtt_placed[0]["target"] == 960.0
