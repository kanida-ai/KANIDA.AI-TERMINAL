"""Build the POSITIONAL 1-min dataset: for each Falcon signal day D, take the Top-5
by engine_rank, enter at D's 09:15 open, and store the concatenated 1-min path across
the hold horizon D..D+HOLD (HOLD=7 forward sessions -> up to 8 hold-days).

Everything the trail needs lives at 1-MINUTE resolution across EVERY hold day (not EOD),
so MFE/MAE, overnight gaps, fixed-hold, trailing and per-stock stops can all be simulated
faithfully. Entry universe verified 100% 1-min-consistent (Step 0).

Per signal day we store (float32 to keep the pickle small):
  syms, rank, entry(5)=09:15 open on D, qty(5)=floor(alloc/entry), dep, nstocks, signal_date
  C  : (Ttot,5) per-stock 1-min CLOSE, aligned to each day's minute grid, ffilled
  O  : (Ttot,5) per-stock 1-min OPEN  (for next-bar / next-open fills; across a day
       boundary O[i+1] is naturally the next session's 09:15 open)
  day: (Ttot,) hold-day index 0..HOLD for each minute
  eod: (HOLD+1,) index of the LAST minute of each hold-day (square-off / carry points)
  dates: list of the actual hold-day date strings (len HOLD+1, trimmed if data ends)
"""
import sys, pickle, sqlite3
from pathlib import Path
import numpy as np

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
OUT = ROOT / "docs" / "ops" / "_pos_dataset.pkl"
ALIAS = {"ZOMATO": "ETERNAL"}
ASSIGNED = 500000.0
HOLD = 7                      # forward sessions -> hold-days 0..7 (D .. D+7)
# Study covers a consistent 5-yr history to 2026-06-15; live table (same engine, verified
# bit-exact on overlap) extends entries to the present. Entries through 2026-06-30 can still
# complete a 3-session hold within the 1-min data (now ending 2026-07-03).
WIN_START, STUDY_END, WIN_END = "2024-05-13", "2026-06-15", "2026-06-30"


def load_signals(con):
    """Study signals (entry_date <= STUDY_END) + LIVE signals (entry_date in (STUDY_END, WIN_END])
    from the prod DB. Live entry_date semantics match the study (verified)."""
    rows = con.execute(
        "SELECT entry_date, symbol, engine_rank FROM falcon_signal_day_study "
        "WHERE persona='falcon_top10_daily' AND engine_rank<=5 "
        "AND entry_date BETWEEN ? AND ? ORDER BY entry_date, engine_rank",
        (WIN_START, STUDY_END)).fetchall()
    days = {}
    for ed, sym, rk in rows:
        days.setdefault(ed, []).append((rk, sym))
    import sys as _sys
    _sys.path.insert(0, str(ROOT / "backend"))
    from falcon.db import falcon_conn
    with falcon_conn() as pc:
        live = pc.execute(
            "SELECT entry_date, symbol, rank FROM falcon_signals_live "
            "WHERE rank<=5 AND entry_date > ? AND entry_date <= ? ORDER BY entry_date, rank",
            (STUDY_END, WIN_END)).fetchall()
    for ed, sym, rk in live:
        days.setdefault(ed, []).append((rk, sym))
    return days


def day_bars(con, osym, date):
    """Return dict HH:MM -> (open, high, low, close) for one symbol on one date, else {}."""
    rows = con.execute(
        "SELECT substr(bar_time,12,5) hm, open, high, low, close FROM ohlc_1min "
        "WHERE symbol=? AND bar_time>=? AND bar_time<? ORDER BY bar_time",
        (osym, date + " 00:00", date + " 23:59")).fetchall()
    return {hm: (o, h, l, c) for hm, o, h, l, c in rows}


def main():
    con = sqlite3.connect(str(RND))
    cal = [r[0] for r in con.execute(
        "SELECT DISTINCT substr(bar_time,1,10) d FROM ohlc_1min ORDER BY d").fetchall()]
    cidx = {d: i for i, d in enumerate(cal)}
    signals = load_signals(con)
    dates = sorted(signals)
    print(f"[*] {len(dates)} signal days  {dates[0]}..{dates[-1]}  HOLD={HOLD}", flush=True)

    dataset = []
    frozen_flags = 0
    for di, D in enumerate(dates):
        # dedupe on the DATA symbol (ZOMATO & ETERNAL are the same company post-rename);
        # keep the higher-ranked instance, trade the distinct Top-5 set.
        seen = set(); syms = []; osyms = []; rank = []
        for rk, s in signals[D]:
            o = ALIAS.get(s, s)
            if o in seen:
                continue
            seen.add(o); syms.append(s); osyms.append(o); rank.append(rk)
            if len(syms) == 5:
                break
        nstk = len(syms)
        if D not in cidx:
            continue
        e = cidx[D]
        holddates = [cal[e + k] for k in range(HOLD + 1) if e + k < len(cal)]

        # pull per-stock per-day bar dicts
        perday = []  # list over holddays: list over stocks of {hm:(o,c)}
        for hd in holddates:
            perday.append([day_bars(con, os, hd) for os in osyms])

        # entry = 09:15 open on D (fall back to first bar if 09:15 missing)
        entry = np.zeros(nstk)
        for j in range(nstk):
            b = perday[0][j]
            if "09:15" in b:
                entry[j] = b["09:15"][0]
            else:
                first = sorted(b)[0]; entry[j] = b[first][0]
        alloc = ASSIGNED / nstk
        qty = np.floor(alloc / entry)
        dep = float((entry * qty).sum())

        # build aligned concatenated path
        C_cols, O_cols, H_cols, L_cols, dayarr, eod = [], [], [], [], [], []
        last_close = entry.copy()      # for ffill across missing minutes/days
        for k, hd in enumerate(holddates):
            grids = perday[k]
            # master minute grid = union across the 5 stocks
            mins = sorted(set().union(*[set(g) for g in grids])) if any(grids) else []
            if not mins:                # whole day missing for all -> freeze (rare)
                mins = ["ffill"]; frozen_flags += 1
            block_c = np.zeros((len(mins), nstk)); block_o = np.zeros((len(mins), nstk))
            block_h = np.zeros((len(mins), nstk)); block_l = np.zeros((len(mins), nstk))
            for j in range(nstk):
                g = grids[j]
                lc = last_close[j]
                for ti, hm in enumerate(mins):
                    if hm in g:
                        o, h, l, c = g[hm]
                        o = o if (o and o > 0) else lc
                        c = c if (c and c > 0) else lc
                        h = h if (h and h > 0) else max(o, c)
                        l = l if (l and l > 0) else min(o, c)
                        block_o[ti, j] = o; block_c[ti, j] = c
                        block_h[ti, j] = h; block_l[ti, j] = l
                        lc = c
                    else:               # missing minute -> ffill last close (position held flat)
                        block_o[ti, j] = lc; block_c[ti, j] = lc
                        block_h[ti, j] = lc; block_l[ti, j] = lc
                last_close[j] = lc
            C_cols.append(block_c); O_cols.append(block_o)
            H_cols.append(block_h); L_cols.append(block_l)
            dayarr.append(np.full(len(mins), k, np.int16))
            eod.append(sum(len(b) for b in C_cols) - 1)   # last global index of this day
        C = np.vstack(C_cols).astype(np.float32)
        O = np.vstack(O_cols).astype(np.float32)
        H = np.vstack(H_cols).astype(np.float32)
        L = np.vstack(L_cols).astype(np.float32)
        day = np.concatenate(dayarr)
        dataset.append(dict(
            signal_date=D, syms=syms, rank=rank, nstocks=nstk,
            entry=entry.astype(np.float32), qty=qty.astype(np.float32), dep=dep,
            C=C, O=O, H=H, L=L, day=day, eod=np.array(eod, np.int32), dates=holddates))
        if (di + 1) % 50 == 0:
            print(f"  [{di+1}/{len(dates)}] {D}  bars/day~{C.shape[0]}  syms={syms}", flush=True)
    con.close()

    pickle.dump(dataset, open(OUT, "wb"), protocol=4)
    sz = OUT.stat().st_size / 1e6
    print(f"\n[*] WROTE {OUT}  ({len(dataset)} signal days, {sz:.0f} MB, frozen-day flags={frozen_flags})")


if __name__ == "__main__":
    main()
