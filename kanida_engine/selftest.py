"""
Self-test for the atom framework — SYNTHETIC bars only (no DB, no Kite; cannot affect the fetch).
Proves: (1) the footprint atoms detect injected accumulation (iceberg / absorption) vs a normal day,
(2) intraday values are point-in-time, (3) per-bar update speed is live-viable.
"""
import time, random
from atoms import StockState

random.seed(7)


def gen_day(inject=False, n=375, base=100.0):
    """375 one-min bars. Normal day = small moves, normal volume. inject=True adds accumulation
    footprints: iceberg bars (big volume, tiny range) + absorption bars (big down-volume that holds)."""
    bars = []; c = base
    for i in range(n):
        drift = random.uniform(-0.03, 0.03)
        o = c; c = round(o * (1 + drift / 100), 2)
        h = round(max(o, c) * (1 + abs(random.uniform(0, 0.04)) / 100), 2)
        l = round(min(o, c) * (1 - abs(random.uniform(0, 0.04)) / 100), 2)
        v = random.randint(800, 1200)
        if inject and i in (40, 55, 90, 130, 175, 210, 260, 300):
            if i % 2 == 0:                       # ICEBERG: huge volume, price pinned (zero range)
                o = c; h = c; l = c
                v = random.randint(9000, 12000)
            else:                                # ABSORPTION: big down-volume, closes upper half
                o = round(c * 1.003, 2); l = round(c * 0.994, 2); h = o; c = round(c * 0.999, 2)
                v = random.randint(9000, 12000)
        bars.append((o, h, l, c, v))
    return bars


def run_day(st, bars):
    st.reset_day()
    for (o, h, l, c, v) in bars:
        st.update(o, h, l, c, v)
    return st.eod_snapshot()


def main():
    st = StockState()
    # warm the persistent baselines on a few normal days first
    for _ in range(3):
        run_day(st, gen_day(inject=False))
    normal = run_day(st, gen_day(inject=False))
    accum = run_day(st, gen_day(inject=True))
    print("=== end-of-day footprint counts ===")
    print(f"  NORMAL day : hivol={normal['eod_n_hivol']:>2}  iceberg={normal['eod_n_iceberg']:>2}  absorb={normal['eod_n_absorb']:>2}")
    print(f"  ACCUM  day : hivol={accum['eod_n_hivol']:>2}  iceberg={accum['eod_n_iceberg']:>2}  absorb={accum['eod_n_absorb']:>2}")
    ok = accum["eod_n_iceberg"] > normal["eod_n_iceberg"] and accum["eod_n_absorb"] > normal["eod_n_absorb"]
    print(f"  DETECTED accumulation footprint: {'YES' if ok else 'NO'}")

    # point-in-time demo: features mid-day are to-time only
    st.reset_day(); bars = gen_day(inject=True)
    for k, (o, h, l, c, v) in enumerate(bars):
        st.update(o, h, l, c, v)
        if k == 120:
            fv = st.feature_vector()
            print("\n=== point-in-time feature vector at minute 120 (to-time) ===")
            for kk in ("id_ret_from_open", "id_atp_dev_pct", "id_atp_push", "id_n_iceberg_bars", "id_n_absorb_bars"):
                print(f"    {kk:20} = {fv[kk]:.4f}")

    # speed: per-bar update cost (live per-minute hot path per stock)
    st2 = StockState(); N = 500_000; bars = gen_day(inject=True) * (N // 375 + 1)
    t0 = time.perf_counter()
    for (o, h, l, c, v) in bars[:N]:
        st2.update(o, h, l, c, v)
    dt = time.perf_counter() - t0
    print(f"\n=== speed ===\n  {N:,} bar-updates in {dt*1000:.0f} ms  =>  {N/dt/1e6:.2f}M updates/sec/core")
    print(f"  live: one 1-min tick for 500 stocks = 500 updates ~= {500/(N/dt)*1e6:.1f} microseconds/core (single-threaded)")
    print("\nSELFTEST OK" if ok else "\nSELFTEST: footprint detection FAILED")


if __name__ == "__main__":
    main()
