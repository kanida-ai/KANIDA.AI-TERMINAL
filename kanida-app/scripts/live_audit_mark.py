"""Live-market audit of ONE 15-minute reading, straight from the F&O store (read-only).

    python kanida-app/scripts/live_audit_mark.py "2026-09-21 09:30:00" [NIFTY BANKNIFTY ...]

It does not trust the metrics job. For the named underlyings it recomputes, from the raw `snapshots` rows,
the figures the screen is built on — the 15-minute OI change of every contract, the put/call OI ratio and
the max-pain strike — and diffs them against what `metrics` / `underlying_snapshots` stored. Across every
underlying it looks for the failure modes that make a live screen lie quietly:

  * coverage     — how many contracts / underlyings actually have a row at this mark
  * carry-over   — quotes whose exchange time predates the PREVIOUS mark (an old quote stamped as new)
  * frozen       — underlyings whose every contract has identical OI to the previous mark
  * absent-as-0  — a stored 0 change where either end of the change was never captured
  * spot / IV / PCR / max pain present or missing, and where spot came from

Read-only: opens the database with mode=ro and never writes.
"""
from __future__ import annotations

import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

DB = Path(__file__).resolve().parents[2] / 'db' / 'derivatives.db'


def q(d, sql, args=()):
    return d.execute(sql, args).fetchall()


def prev_mark(d, mark):
    r = q(d, "select max(captured_at) from underlying_snapshots where captured_at < ? and substr(captured_at,1,10)=substr(?,1,10)",
          (mark, mark))
    return r[0][0] if r and r[0][0] else None


def max_pain(rows):
    """rows: (strike, type, oi). The strike at which option WRITERS pay out least at expiry."""
    strikes = sorted({s for s, _, _ in rows})
    best = None
    for k in strikes:
        pain = 0.0
        for s, t, oi in rows:
            if oi is None:
                continue
            if t == 'CE' and k > s:
                pain += oi * (k - s)
            elif t == 'PE' and k < s:
                pain += oi * (s - k)
        if best is None or pain < best[1]:
            best = (k, pain)
    return best[0] if best else None


def main():
    mark = sys.argv[1]
    focus = sys.argv[2:] or ['NIFTY', 'BANKNIFTY']
    d = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    prev = prev_mark(d, mark)
    out = []
    say = out.append
    say(f"MARK {mark}   previous {prev}")

    # --- coverage --------------------------------------------------------------------------------------------
    n_snap = q(d, "select count(*) from snapshots where captured_at=?", (mark,))[0][0]
    n_und = q(d, "select count(*) from underlying_snapshots where captured_at=?", (mark,))[0][0]
    n_met = q(d, "select scope,count(*) from metrics where captured_at=? group by scope", (mark,))
    say(f"coverage: snapshots={n_snap}  underlyings={n_und}  metrics={dict(n_met)}")
    if not n_snap:
        say("FAIL: nothing captured at this mark")
        print("\n".join(out))
        return

    # --- carry-over: an exchange time older than the previous mark is an old quote wearing a new stamp -------
    if prev:
        stale = q(d, """select count(*) from snapshots where captured_at=? and exchange_time is not null
                         and exchange_time < ?""", (mark, prev))[0][0]
        noexch = q(d, "select count(*) from snapshots where captured_at=? and exchange_time is null", (mark,))[0][0]
        say(f"carry-over: {stale} quotes with exchange_time before the previous mark; {noexch} with no exchange_time")

    # --- per-underlying health -------------------------------------------------------------------------------
    und = q(d, """select underlying,spot,spot_source,pcr_oi,max_pain_strike,total_ce_oi,total_pe_oi,
                         ce_contracts,pe_contracts from underlying_snapshots where captured_at=?""", (mark,))
    nospot = [u[0] for u in und if u[1] is None]
    nopcr = [u[0] for u in und if u[3] is None]
    nomp = [u[0] for u in und if u[4] is None]
    src = defaultdict(int)
    for u in und:
        src[u[2]] += 1
    say(f"underlyings: {len(und)}  spot missing={len(nospot)} {nospot[:8]}  pcr missing={len(nopcr)}  "
        f"max_pain missing={len(nomp)}  spot_source={dict(src)}")

    # frozen: every contract of an underlying has the same OI as the previous mark
    if prev:
        rows = q(d, """select c.underlying, sum(case when a.oi is not null and b.oi is not null and a.oi=b.oi then 1 else 0 end),
                              sum(case when a.oi is not null and b.oi is not null then 1 else 0 end)
                         from snapshots a join contracts c on c.instrument_token=a.instrument_token
                         join snapshots b on b.instrument_token=a.instrument_token and b.captured_at=?
                        where a.captured_at=? and c.instrument_type in ('CE','PE') group by c.underlying""", (prev, mark))
        frozen = [r[0] for r in rows if r[2] >= 4 and r[1] == r[2]]
        say(f"frozen OI (every compared option unchanged vs previous mark): {len(frozen)} of {len(rows)} {frozen[:10]}")

    # absent stored as zero
    z = q(d, """select count(*) from metrics m left join snapshots s on s.instrument_token=m.instrument_token
                  and s.captured_at=m.captured_at
                 where m.captured_at=? and m.scope='contract' and m.oi_change_15m=0 and s.oi is null""", (mark,))[0][0]
    say(f"absent-as-zero: {z} contracts store a 0 OI change with no OI captured")

    # --- the focus underlyings: recompute and diff ------------------------------------------------------------
    for name in focus:
        say(f"--- {name}")
        u = [x for x in und if x[0] == name]
        if not u:
            say("   FAIL: no underlying row at this mark")
            continue
        u = u[0]
        say(f"   stored: spot={u[1]} ({u[2]})  pcr={u[3]}  max_pain={u[4]}  ce_oi={u[5]} pe_oi={u[6]}  "
            f"contracts ce={u[7]} pe={u[8]}")
        opt = q(d, """select c.instrument_token,c.instrument_type,c.strike,c.expiry,s.oi,s.last_price,s.exchange_time
                        from snapshots s join contracts c on c.instrument_token=s.instrument_token
                       where s.captured_at=? and c.underlying=? and c.instrument_type in ('CE','PE')""", (mark, name))
        if not opt:
            say("   no option snapshots")
            continue
        expiries = sorted({o[3] for o in opt})
        near = expiries[0]
        ce = sum(o[4] or 0 for o in opt if o[1] == 'CE')
        pe = sum(o[4] or 0 for o in opt if o[1] == 'PE')
        near_rows = [(o[2], o[1], o[4]) for o in opt if o[3] == near]
        say(f"   recomputed over {len(opt)} options, expiries {expiries[:3]}:")
        say(f"      total CE OI {ce:.0f} vs stored {u[5]}   PE OI {pe:.0f} vs stored {u[6]}   "
            f"PCR {pe/ce if ce else float('nan'):.4f} vs stored {u[3]}")
        # The screen reads PCR and max pain from `metrics` scope='underlying', ONE ROW PER EXPIRY — so they are
        # recomputed per expiry here, from nothing but the raw snapshots.
        stored = {r[0]: r[1:] for r in q(d, """select expiry,pcr_oi,max_pain_strike,total_ce_oi,total_pe_oi,spot,spot_source
                                                  from metrics where scope='underlying' and underlying=? and captured_at=?""",
                                          (name, mark))}
        for ex in expiries[:2]:
            rows_ex = [o for o in opt if o[3] == ex]
            ce_x = sum(o[4] or 0 for o in rows_ex if o[1] == 'CE')
            pe_x = sum(o[4] or 0 for o in rows_ex if o[1] == 'PE')
            mp_x = max_pain([(o[2], o[1], o[4]) for o in rows_ex])
            st = stored.get(ex)
            if not st:
                say(f"      {ex}: FAIL no metrics row for this expiry")
                continue
            pcr_x = pe_x / ce_x if ce_x else None
            ok_pcr = st[0] is not None and pcr_x is not None and abs(st[0] - pcr_x) < 1e-6
            ok_mp = st[1] is not None and mp_x is not None and abs(st[1] - mp_x) < 1e-6
            say(f"      {ex}: PCR {pcr_x if pcr_x is None else round(pcr_x,4)} vs stored {st[0] if st[0] is None else round(st[0],4)} "
                f"[{'ok' if ok_pcr else 'MISMATCH'}]  max pain {mp_x} vs stored {st[1]} [{'ok' if ok_mp else 'MISMATCH'}]  "
                f"spot {st[4]} ({st[5]})")

        # 15-minute OI change, contract by contract
        if prev:
            chk = q(d, """select m.tradingsymbol, m.oi_change_15m, a.oi, b.oi, m.price_change_15m, a.last_price, b.last_price
                            from metrics m
                            join snapshots a on a.instrument_token=m.instrument_token and a.captured_at=m.captured_at
                            left join snapshots b on b.instrument_token=m.instrument_token and b.captured_at=?
                           where m.captured_at=? and m.scope='contract' and m.underlying=?""", (prev, mark, name))
            bad_oi, bad_px, n = [], [], 0
            for sym, doi, a, b, dpx, pa, pb in chk:
                n += 1
                want = None if a is None or b is None else a - b
                if (want is None) != (doi is None) or (want is not None and abs(want - doi) > 0.5):
                    bad_oi.append((sym, doi, want))
                wpx = None if pa is None or pb is None else round(pa - pb, 2)
                if (wpx is None) != (dpx is None) or (wpx is not None and abs(wpx - dpx) > 0.011):
                    bad_px.append((sym, dpx, wpx))
            say(f"      15-min changes checked on {n} contracts: OI mismatches {len(bad_oi)} {bad_oi[:4]}  "
                f"price mismatches {len(bad_px)} {bad_px[:4]}")
    print("\n".join(out))


if __name__ == '__main__':
    main()
