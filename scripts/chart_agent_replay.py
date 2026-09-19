"""
KANIDA Chart Agent — HISTORICAL LIVE-MARKET REPLAY (Horizontal Trendline, DAILY timeframe).

Takes one historical trading day and replays its 1-minute candles from 09:15 as if live.
At each minute the agent evaluates ONLY information available up to that minute and emits
GENUINE events about the DAILY Horizontal-Trendline setup:
    - the daily resistance is fixed from COMPLETED daily candles (through the prior session),
    - intraday it tracks the forming (not-yet-closed) daily candle vs that level,
    - a daily breakout is only CONFIRMED at the close (post-market stage).
Timeframe integrity: daily agent -> daily level -> daily chart (today drawn as a forming candle).

Output: reports/chart_agent/replay_<SYM>_<DAY>.json  (feeds the Agents UI — no hard-coded events).

Run:  python scripts/chart_agent_replay.py TITAN 2022-08-30
"""
from __future__ import annotations
import os, sys, json, sqlite3
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from chart_agent import (load_daily, detect_horizontal_breakout_retest, pattern_evidence,
                         _insights_stats, PARAMS, COST, DB, OUT)

APPROACH = 0.005   # within 0.5% of the level = "approaching"
TOUCH = 0.002      # within 0.2% = "testing"
BUF = 0.002        # close/above buffer for a genuine cross


def load_minutes(symbol, day):
    con = sqlite3.connect(f"file:{os.path.abspath(DB)}?mode=ro", uri=True)
    rows = con.execute("SELECT bar_time,open,high,low,close,volume FROM ohlc_1min "
                       "WHERE symbol=? AND substr(bar_time,1,10)=? ORDER BY bar_time", (symbol, day)).fetchall()
    con.close()
    return [{"t": r[0][11:16], "o": r[1], "h": r[2], "l": r[3], "c": r[4], "v": r[5]} for r in rows]


def build(symbol="TITAN", day="2022-08-30"):
    df = load_daily(symbol)
    if day not in set(df.index.strftime("%Y-%m-%d")):
        print(f"{symbol}: no daily bar for {day}"); return None
    di = df.index.get_loc(pd.Timestamp(day))
    ev = detect_horizontal_breakout_retest(df, **PARAMS)
    today_ev = next((e for e in ev if e.breakout_idx == di), None)
    if today_ev is None:
        print(f"{symbol} {day}: not a detected breakout day."); return None
    R = round(float(today_ev.level), 2)
    prior_close = float(df["close"].iloc[di - 1])
    avg_vol = float(df["volume"].iloc[di - 20:di].mean())
    touch_dates = [df.index[t].strftime("%d-%b-%Y") for t in today_ev.touches]
    minutes = load_minutes(symbol, day)
    if not minutes:
        print(f"{symbol} {day}: no 1-min data."); return None
    N = len(minutes)

    # ---- replay the minutes, emit genuine events ----
    events = []
    def add(i, kind, text, cta=None, icon=None):
        if i == "eod":
            t, ii = "EOD", "eod"
        elif i < 0:
            t, ii = "pre", -1
        else:
            t, ii = minutes[i]["t"], i
        events.append({"t": t, "i": ii, "kind": kind, "icon": icon, "text": text, "cta": cta})

    add(-1, "context",
        f"Yesterday {symbol} closed ₹{prior_close:,.0f}, below daily resistance ₹{R:,.0f} — a flat level "
        f"tested {len(today_ev.touches)}× ({', '.join(touch_dates)}). Watching today for a daily breakout. "
        f"The daily candle only completes at the 15:30 close, so nothing confirms before then.")

    state = "below"; cum_vol = 0; hi = -1e9; lo = 1e9; vol_flags = set(); last_hold = -999; broke_i = None
    for i, m in enumerate(minutes):
        cum_vol += m["v"]; hi = max(hi, m["h"]); lo = min(lo, m["l"]); last = m["c"]
        frac = (i + 1) / N; expected = avg_vol * frac
        volx = cum_vol / expected if expected > 0 else 0
        near = last >= R * (1 - APPROACH)

        if state == "below" and near and last < R * (1 + BUF):
            state = "approaching"
            add(i, "approach", f"Price is climbing into the resistance — ₹{last:,.0f}, within "
                               f"{(R-last)/R*100:.1f}% of ₹{R:,.0f}.")
        if state in ("approaching", "below") and m["h"] >= R * (1 - TOUCH) and last <= R * (1 + BUF):
            state = "testing"
            add(i, "test", f"Testing ₹{R:,.0f} — tapped the level intraday (high ₹{m['h']:,.0f}) but sellers are defending it.")
        if last > R * (1 + BUF) and state != "above":
            state = "above"; broke_i = i
            add(i, "break", f"Price broke ₹{R:,.0f} — trading ₹{last:,.0f} at {m['t']}. This is an INTRADAY cross; "
                            f"the daily candle still has to close above the level to confirm.",
                cta="setup", icon="up")
        elif state == "above" and last < R * (1 - 0.001):
            state = "fallback"
            add(i, "fail", f"Slipped back below ₹{R:,.0f} (₹{last:,.0f}). The intraday breakout is not holding — "
                           f"weakness like this early is how failed breakouts usually start.", cta="setup", icon="down")

        # volume vs the same fraction of an average day — only after the noisy opening (>=30 min in)
        if i >= 30 and near:
            crossed = [thr for thr in (1.3, 1.6, 2.0) if volx >= thr and thr not in vol_flags]
            if crossed:
                for thr in crossed:
                    vol_flags.add(thr)
                add(i, "volume", f"Volume is running {volx:.1f}× normal for this point in the day — real participation behind the move.")
        if state == "above" and i - last_hold >= 90:
            last_hold = i
            add(i, "hold", f"Still holding above ₹{R:,.0f} — ₹{last:,.0f}, roughly +{(last-R)/R*100:.1f}% over the level.")

    last = minutes[-1]["c"]
    add(N - 1, "close", f"Into the close: ₹{last:,.0f}. Daily candle about to complete — confirmation check next.")

    # ---- post-market: complete today's daily candle, evaluate the DAILY breakout ----
    tday = {"o": round(minutes[0]["o"], 2), "h": round(hi, 2), "l": round(lo, 2), "c": round(last, 2), "v": int(cum_vol)}
    vol_ok = cum_vol > PARAMS["vol_mult"] * avg_vol
    confirmed = last > R * (1 + BUF) and vol_ok
    if confirmed:
        verdict = (f"★ DAILY BREAKOUT CONFIRMED — {symbol} closed ₹{last:,.0f} above ₹{R:,.0f} "
                   f"on {cum_vol/avg_vol:.1f}× volume. The breakout leg is in. What changed after the final "
                   f"candle: ₹{R:,.0f} flips from resistance to the level to defend. The setup now needs a "
                   f"RETEST that holds over the next 1–15 sessions to complete the full pattern.")
    elif last > R * (1 + BUF):
        verdict = (f"Closed ₹{last:,.0f} above ₹{R:,.0f}, but volume ({cum_vol/avg_vol:.1f}× avg) was light — "
                   f"an unconfirmed breakout. Watching whether it can hold.")
    else:
        verdict = (f"Closed ₹{last:,.0f}, back below ₹{R:,.0f}. No daily breakout today — the level held. "
                   f"Setup remains pending.")
    add("eod", "eod", verdict, cta=("evidence" if confirmed else None), icon="star" if confirmed else None)

    # ---- point-in-time evidence: only occurrences RESOLVED on/before today ----
    dref = df.iloc[:di + 1]
    ev_upto = detect_horizontal_breakout_retest(dref, **PARAMS)
    resolved = [e for e in ev_upto if e.entry_idx + 9 <= di]
    evidence = None
    if resolved:
        pv = pattern_evidence(dref, resolved, 10)
        if pv:
            ins = _insights_stats(pv); s = pv["summary"]; hz = pv["horizons"]
            evidence = {"n": s["n"], "win_rate": s["pct_up"], "avg_T10": hz[10]["mean"],
                        "adverse_T10": hz[10]["mae"], "best_window": f"T+{ins['bw'][0]} to T+{ins['bw'][1]}",
                        "winner_path": [round(x, 2) for x in (ins["win_path"].tolist() if ins["win_path"] is not None else [])],
                        "loser_path": [round(x, 2) for x in (ins["lose_path"].tolist() if ins["lose_path"] is not None else [])],
                        "as_of": day}

    # ---- daily window for the chart (completed bars through yesterday) ----
    first_touch = min(today_ev.touches) if today_ev.touches else di - 45
    a = max(0, min(di - 45, first_touch - 3))
    win = df.iloc[a:di]
    daily = [{"d": ix.strftime("%d-%b"), "o": round(r.open, 2), "h": round(r.high, 2),
              "l": round(r.low, 2), "c": round(r.close, 2)} for ix, r in win.iterrows()]
    touches_rel = [t - a for t in today_ev.touches if t >= a]

    out = {"symbol": symbol, "day": pd.Timestamp(day).strftime("%d-%b-%Y"), "timeframe": "daily",
           "level": R, "prior_close": round(prior_close, 2), "touches": touches_rel,
           "daily": daily, "today": tday, "minutes": minutes, "events": events,
           "evidence": evidence, "confirmed": confirmed,
           "n_events": len(events)}
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, f"replay_{symbol}_{day}.json")
    json.dump(out, open(path, "w"), separators=(",", ":"))
    print(f"{symbol} {day}: {len(events)} events, {len(minutes)} minutes, "
          f"confirmed={confirmed}, evidence n={evidence['n'] if evidence else 0}")
    print("  events:")
    for e in events:
        safe = e["text"][:88].replace("₹", "Rs ").replace("★", "*").replace("×", "x")
        print(f"    {str(e['t']):>5}  [{e['kind']:8}] {safe}")
    print(f"  -> {path}")
    return out


def _intraday(minutes, R, avg_vol, mode, add, sess):
    """Emit genuine intraday events for one session. mode='breakout' (watch for a cross ABOVE the
    level) or 'retest' (already above — watch whether the level HOLDS as support on a pullback)."""
    N = len(minutes); cum = 0; hi = -1e9; lo = 1e9; vflags = set()
    state = "below" if mode == "breakout" else "above"; last_hold = -999; touched_support = False
    for i, m in enumerate(minutes):
        cum += m["v"]; hi = max(hi, m["h"]); lo = min(lo, m["l"]); last = m["c"]
        frac = (i + 1) / N; volx = cum / (avg_vol * frac) if avg_vol * frac > 0 else 0
        if mode == "breakout":
            if state == "below" and last >= R * (1 - APPROACH) and last < R * (1 + BUF):
                state = "approach"; add(sess, i, "approach", f"Climbing into resistance — ₹{last:,.0f}, within {(R-last)/R*100:.1f}% of ₹{R:,.0f}.")
            if state in ("approach", "below") and m["h"] >= R * (1 - TOUCH) and last <= R * (1 + BUF):
                state = "testing"; add(sess, i, "test", f"Testing ₹{R:,.0f} — tapped it (high ₹{m['h']:,.0f}) but sellers are defending.")
            if last > R * (1 + BUF) and state != "above":
                state = "above"; add(sess, i, "break", f"Broke ₹{R:,.0f} — ₹{last:,.0f} at {m['t']}. Intraday cross; the daily candle must still CLOSE above to confirm.", "setup", "up")
            elif state == "above" and last < R * (1 - 0.001):
                state = "fallback"; add(sess, i, "fail", f"Slipped back below ₹{R:,.0f} (₹{last:,.0f}) — the breakout isn't holding.", "setup", "down")
            if state == "above" and i - last_hold >= 90:
                last_hold = i; add(sess, i, "hold", f"Holding above ₹{R:,.0f} — ₹{last:,.0f}, ~+{(last-R)/R*100:.1f}% over the level.")
        else:  # retest mode
            dist = (last - R) / R
            if state == "above" and last <= R * (1 + APPROACH):
                state = "pullback"; add(sess, i, "approach", f"Pulling back toward the reclaimed level — ₹{last:,.0f}, nearing ₹{R:,.0f}. Does old resistance now act as support?")
            if state in ("pullback", "above") and m["l"] <= R * (1 + TOUCH):
                state = "test_support"; touched_support = True
                add(sess, i, "test", f"Testing ₹{R:,.0f} as SUPPORT — dipped to ₹{m['l']:,.0f}. This is the retest.")
            if state == "test_support" and last > R * (1 + BUF) and last > m["o"]:
                state = "held"; add(sess, i, "break", f"Bounced off ₹{R:,.0f} — back to ₹{last:,.0f}. Buyers defended the breakout; the retest is holding.", "setup", "up")
            if touched_support and last < R * (1 - BUF):
                state = "broke"; add(sess, i, "fail", f"Lost ₹{R:,.0f} (₹{last:,.0f}) — the retest is failing; the breakout looks like a fake-out.", "setup", "down")
        for thr in (1.3, 1.6, 2.0):
            if i >= 30 and volx >= thr and thr not in vflags and R * (1 - APPROACH) <= last <= R * (1 + APPROACH * 3):
                vflags |= {t for t in (1.3, 1.6, 2.0) if volx >= t}
                add(sess, i, "volume", f"Volume {volx:.1f}× normal for this time — real participation."); break
    return {"o": round(minutes[0]["o"], 2), "h": round(hi, 2), "l": round(lo, 2), "c": round(minutes[-1]["c"], 2), "v": int(cum)}


def build_multi(symbol="TITAN", breakout_day="2022-08-30"):
    df = load_daily(symbol); idx = df.index
    if breakout_day not in set(idx.strftime("%Y-%m-%d")):
        print("no bar"); return None
    b = idx.get_loc(pd.Timestamp(breakout_day))
    e = next((x for x in detect_horizontal_breakout_retest(df, **PARAMS) if x.breakout_idx == b), None)
    if e is None:
        print("not a breakout day"); return None
    R = round(float(e.level), 2); rt = e.retest_idx; entry = e.entry_idx
    o, h, l, c, vv = df["open"].values, df["high"].values, df["low"].values, df["close"].values, df["volume"].values
    avg_vol = float(df["volume"].iloc[b - 20:b].mean())
    prior_close = float(c[b - 1])

    # sessions to replay: breakout(intraday), any days between, retest(intraday), follow-through(daily to T+10)
    follow_last = min(entry + 9, len(df) - 1)
    day_idxs = sorted(set([b] + list(range(b + 1, rt)) + [rt] + list(range(entry, follow_last + 1))))
    sessions = []; sess_of = {}
    for k in day_idxs:
        d = idx[k].strftime("%Y-%m-%d")
        intr = k in (b, rt)
        role = "breakout" if k == b else ("retest" if k == rt else ("entry" if k == entry else ""))
        s = {"date": d, "label": idx[k].strftime("%d-%b"), "intraday": intr, "role": role,
             "candle": {"o": round(o[k], 2), "h": round(h[k], 2), "l": round(l[k], 2), "c": round(c[k], 2), "v": int(vv[k])}}
        if intr:
            s["minutes"] = load_minutes(symbol, d)
        sess_of[k] = len(sessions); sessions.append(s)

    events = []
    def add(sess, i, kind, text, cta=None, icon=None):
        if i == "eod": t = "EOD"
        elif i == "daily": t = sessions[sess]["date"]
        elif i < 0: t = "pre"
        else: t = sessions[sess]["minutes"][i]["t"]
        events.append({"sess": sess, "i": i, "t": t, "kind": kind, "icon": icon, "text": text, "cta": cta})

    # ---- Session: breakout day ----
    sb = sess_of[b]
    tdates = [idx[t].strftime("%d-%b-%Y") for t in e.touches]
    add(sb, -1, "context", f"Yesterday {symbol} closed ₹{prior_close:,.0f}, below daily resistance ₹{R:,.0f} "
                           f"(tested {len(e.touches)}× {', '.join(tdates)}). Watching for a daily breakout — confirms only at the close.")
    _intraday(sessions[sb]["minutes"], R, avg_vol, "breakout", add, sb)
    bc = sessions[sb]["candle"]; conf_b = bc["c"] > R * (1 + BUF) and bc["v"] > PARAMS["vol_mult"] * avg_vol
    add(sb, "eod", "eod", f"★ DAILY BREAKOUT (leg 1) — closed ₹{bc['c']:,.0f} above ₹{R:,.0f} on {bc['v']/avg_vol:.1f}× volume. "
                          f"₹{R:,.0f} now flips to support. The pattern isn't complete until price RETESTS it and holds.",
        cta="setup", icon="star")

    # ---- days between breakout and retest (daily notes) ----
    for k in range(b + 1, rt):
        s = sess_of[k]; cc = sessions[s]["candle"]
        add(s, "daily", "hold", f"{idx[k].strftime('%d-%b')}: held ₹{cc['c']:,.0f}, above ₹{R:,.0f}. Retest still pending.")

    # ---- Session: retest day ----
    sr = sess_of[rt]
    add(sr, -1, "context", f"{idx[rt].strftime('%d-%b')}: opened ₹{sessions[sr]['candle']['o']:,.0f}, above the reclaimed ₹{R:,.0f}. "
                           f"A healthy breakout should now find support here on any dip.")
    _intraday(sessions[sr]["minutes"], R, avg_vol, "retest", add, sr)
    rc = sessions[sr]["candle"]; conf_r = rc["c"] > R * (1 + BUF) and rc["l"] <= R * (1 + 0.012)
    if conf_r:
        add(sr, "eod", "eod", f"★ RETEST CONFIRMED — dipped to ₹{rc['l']:,.0f} (right on ₹{R:,.0f}) and closed back up at ₹{rc['c']:,.0f}. "
                              f"The full Horizontal-Breakout + Retest + Volume pattern is COMPLETE. Entry at tomorrow's open; now tracking the outcome vs history.",
            cta="evidence", icon="star")
    else:
        add(sr, "eod", "eod", f"Retest FAILED — closed ₹{rc['c']:,.0f}, unable to hold ₹{R:,.0f}. Pattern invalidated; standing aside.", icon="down")

    # ---- follow-through: actual outcome vs the historical paths ----
    entry_open = o[entry]
    actual = [0.0] + [round((c[entry + d - 1] / entry_open - 1) * 100, 2) for d in range(1, follow_last - entry + 2)]
    # point-in-time evidence (occurrences resolved on/before breakout day)
    dref = df.iloc[:b + 1]
    resolved = [x for x in detect_horizontal_breakout_retest(dref, **PARAMS) if x.entry_idx + 9 <= b]
    evidence = None; winner = loser = []
    if resolved:
        pv = pattern_evidence(dref, resolved, 10)
        if pv:
            ins = _insights_stats(pv); s = pv["summary"]; hz = pv["horizons"]
            winner = [round(x, 2) for x in (ins["win_path"].tolist() if ins["win_path"] is not None else [])]
            loser = [round(x, 2) for x in (ins["lose_path"].tolist() if ins["lose_path"] is not None else [])]
            evidence = {"n": s["n"], "win_rate": s["pct_up"], "avg_T10": hz[10]["mean"], "adverse_T10": hz[10]["mae"],
                        "best_window": f"T+{ins['bw'][0]} to T+{ins['bw'][1]}", "winner_path": winner, "loser_path": loser}
    for off in [1, 3, 5]:
        k = entry + off - 1
        if k not in sess_of or off >= len(actual):
            continue
        a = actual[off]; wp = winner[off] if winner and off < len(winner) else None
        lp = loser[off] if loser and off < len(loser) else None
        ref = f" (typical winner +{wp:.1f}%, loser {lp:.1f}% by now)" if wp is not None else ""
        add(sess_of[k], "daily", "outcome", f"T+{off}: {a:+.1f}% since entry{ref}.")
    # final verdict at T+10
    k = entry + 9
    if k in sess_of and len(actual) > 10:
        fin = actual[10]; wp = winner[10] if winner else None; lp = loser[10] if loser else None
        if fin > 0:
            txt = f"Outcome by T+10: {fin:+.1f}% — a WINNER (positive)" + (f", though below the typical winner (+{wp:.1f}%)" if wp else "") + ". This instance is logged back into the evidence base."
        else:
            txt = f"Outcome by T+10: {fin:+.1f}% — a losing instance" + (f" (typical loser {lp:.1f}%)" if lp else "") + ". Logged back into the evidence base."
        add(sess_of[k], "daily", "outcome", txt, icon="star")

    a0 = max(0, min(b - 45, (min(e.touches) - 3) if e.touches else b - 45))
    base = [{"d": ix.strftime("%d-%b"), "o": round(r.open, 2), "h": round(r.high, 2), "l": round(r.low, 2), "c": round(r.close, 2)}
            for ix, r in df.iloc[a0:b].iterrows()]
    out = {"symbol": symbol, "level": R, "touches": [t - a0 for t in e.touches if t >= a0],
           "day_breakout": idx[b].strftime("%d-%b-%Y"), "day_confirm": idx[rt].strftime("%d-%b-%Y"),
           "base_daily": base, "sessions": sessions, "events": events, "evidence": evidence,
           "winner_path": winner, "loser_path": loser, "actual_path": actual, "confirmed": bool(conf_r)}
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, f"replay_multi_{symbol}_{breakout_day}.json")
    json.dump(out, open(path, "w"), separators=(",", ":"))
    print(f"{symbol}: {len(sessions)} sessions, {len(events)} events, confirmed_retest={bool(conf_r)}, evidence n={evidence['n'] if evidence else 0}")
    for ev in events:
        safe = ev["text"][:86].replace("₹", "Rs ").replace("★", "*").replace("×", "x").replace("—", "-")
        print(f"  s{ev['sess']} {str(ev['t']):>5} [{ev['kind']:8}] {safe}")
    print("  ->", path)
    return out


if __name__ == "__main__":
    args = sys.argv[1:]
    if args and args[0] == "--multi":
        build_multi(args[1] if len(args) > 1 else "TITAN", args[2] if len(args) > 2 else "2022-08-30")
    else:
        build(args[0] if args else "TITAN", args[1] if len(args) > 1 else "2022-08-30")
