"""
Options Agent · STEP 0 — Kite options data-reality probe  (READ-ONLY)

Purpose
-------
The Options Agent's evidence pillar depends entirely on what Kite actually gives us for
options. This script MEASURES that instead of assuming it, and writes a JSON report.

It answers, empirically:
  Q1  What does kite.instruments("NFO") expose for NIFTY options? (fields, expiries,
      strike ladder + step, lot_size, weekly vs monthly)
  Q2  How deep is kite.historical_data() for a LIVE option instrument_token?
      (day + minute, with oi=True) -> the real history depth per contract.
  Q3  Does kite.quote() return IV / greeks for an option, or only price + OI + depth?
      (dumps the EXACT key set, so we never guess)
  Q4  What does basket_order_margins() report for a real 4-leg iron condor?
      (span+exposure WITH hedge benefit -- the number the agent must respect)
  Q5  What does get_virtual_contract_note() report as real charges for that basket?

EXECUTION BOUNDARY — this script places NO orders.
  It calls ONLY read/calculation endpoints: instruments(), historical_data(), quote(),
  basket_order_margins(), get_virtual_contract_note(). `basket_order_margins` and
  `get_virtual_contract_note` are *calculators* -- Kite returns a margin/charges estimate
  for a hypothetical basket and does NOT create, queue, or reserve anything. There is no
  place_order / modify_order / GTT call anywhere in this file, by design.

Usage
-----
    python scripts/options_step0_probe.py            # NIFTY, default
    python scripts/options_step0_probe.py --name BANKNIFTY
    python scripts/options_step0_probe.py --out reports/step0_nifty.json

Requires a valid Kite session (the SAME one the rest of the backend uses:
services.kite_auth.get_kite_client). With no credentials it exits cleanly and says so --
it never fabricates a result.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend"))


def _jsonable(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


# ---------------------------------------------------------------- Q1: instrument master
def probe_instruments(kite, name: str) -> dict:
    """The NFO instrument master, filtered to `name`'s options. Live contracts only."""
    rows = kite.instruments("NFO")
    opts = [r for r in rows
            if r.get("name") == name and r.get("instrument_type") in ("CE", "PE")]
    out = {
        "nfo_rows_total": len(rows),
        "option_rows_for_name": len(opts),
        "row_fields": sorted(opts[0].keys()) if opts else [],
    }
    if not opts:
        out["note"] = "no live " + name + " option contracts in the master"
        return out

    by_expiry: dict = {}
    for r in opts:
        e = str(r.get("expiry"))
        b = by_expiry.setdefault(e, {"n": 0, "strikes": set(), "lot_size": r.get("lot_size")})
        b["n"] += 1
        b["strikes"].add(float(r.get("strike") or 0))

    expiries = []
    for e in sorted(by_expiry):
        b = by_expiry[e]
        ks = sorted(b["strikes"])
        steps = sorted({round(ks[i + 1] - ks[i], 2) for i in range(len(ks) - 1)}) if len(ks) > 1 else []
        expiries.append({
            "expiry": e,
            "contracts": b["n"],
            "distinct_strikes": len(ks),
            "strike_min": ks[0] if ks else None,
            "strike_max": ks[-1] if ks else None,
            "strike_steps_seen": steps[:5],
            "lot_size": b["lot_size"],
        })
    out["expiries"] = expiries
    out["distinct_expiries"] = len(expiries)
    # An expiry with a dense ladder and a near date is the weekly; the last expiry-day of a
    # month is the monthly. We report the raw list and let the human classify -- we do NOT
    # guess the exchange's weekly/monthly rule here.
    out["sample_row"] = {k: _jsonable(v) for k, v in opts[0].items()}
    return out


# ------------------------------------------------------- Q2: historical depth per token
def probe_history(kite, kite_rows: list, name: str, spot_hint) -> dict:
    """THE decider: how much candle history does a LIVE option token actually return?

    We probe the nearest-ATM contract of the nearest expiry and of the FARTHEST expiry.
    The farthest expiry has been listed longest, so its depth is the best available proxy
    for 'history since listing'."""
    opts = [r for r in kite_rows
            if r.get("name") == name and r.get("instrument_type") == "CE"]
    if not opts:
        return {"error": "no live " + name + " CE contracts to probe"}

    expiries = sorted({r["expiry"] for r in opts})
    picks = []
    for label, exp in (("nearest_expiry", expiries[0]), ("farthest_expiry", expiries[-1])):
        same = [r for r in opts if r["expiry"] == exp]
        if spot_hint:
            same.sort(key=lambda r: abs(float(r["strike"]) - spot_hint))
        else:                       # no spot -> take the median strike, not a random one
            same.sort(key=lambda r: float(r["strike"]))
            same = [same[len(same) // 2]] + same
        picks.append((label, same[0]))

    results = []
    today = date.today()
    for label, row in picks:
        r = {"probe": label, "tradingsymbol": row["tradingsymbol"],
             "expiry": _jsonable(row["expiry"]), "strike": row.get("strike"),
             "instrument_token": row.get("instrument_token")}
        for interval, lookback_days in (("day", 400), ("minute", 55)):
            key = interval + "_candles"
            try:
                bars = kite.historical_data(
                    row["instrument_token"],
                    from_date=today - timedelta(days=lookback_days),
                    to_date=today,
                    interval=interval,
                    oi=True,
                )
                r[key] = {
                    "requested_from": (today - timedelta(days=lookback_days)).isoformat(),
                    "returned": len(bars),
                    "earliest": _jsonable(bars[0]["date"]) if bars else None,
                    "latest": _jsonable(bars[-1]["date"]) if bars else None,
                    "fields": sorted(bars[0].keys()) if bars else [],
                    "oi_present": ("oi" in bars[0]) if bars else False,
                }
            except Exception as e:                      # noqa: BLE001 - report, never crash
                r[key] = {"error": type(e).__name__ + ": " + str(e)}
        results.append(r)
    return {"probes": results}


# ------------------------------------------------------------------ Q3: quote field set
def probe_quote(kite, kite_rows: list, name: str) -> dict:
    """Settles empirically whether Kite gives IV / greeks, or only price + OI + depth."""
    opts = [r for r in kite_rows
            if r.get("name") == name and r.get("instrument_type") in ("CE", "PE")]
    if not opts:
        return {"error": "no live " + name + " option contracts to quote"}
    exp = sorted({r["expiry"] for r in opts})[0]
    same = sorted([r for r in opts if r["expiry"] == exp], key=lambda r: float(r["strike"]))
    sample = same[len(same) // 2]
    sym = "NFO:" + sample["tradingsymbol"]
    try:
        q = kite.quote(sym)
    except Exception as e:                              # noqa: BLE001
        return {"error": type(e).__name__ + ": " + str(e), "symbol": sym}
    body = q.get(sym, {})
    return {
        "symbol": sym,
        "top_level_keys": sorted(body.keys()),
        "depth_keys": sorted((body.get("depth") or {}).keys()),
        "depth_levels_bid": len((body.get("depth") or {}).get("buy") or []),
        "depth_levels_ask": len((body.get("depth") or {}).get("sell") or []),
        "has_oi": "oi" in body,
        "has_iv": any(k == "iv" or "implied" in k.lower() for k in body),
        "has_greeks": any(k.lower() in ("delta", "gamma", "theta", "vega", "rho") for k in body),
        "raw": {k: _jsonable(v) for k, v in body.items() if k != "depth"},
    }


# ------------------------------------------ Q4/Q5: real margin + real charges (no order)
def _condor_legs(kite_rows: list, name: str, spot: float, width: int, otm: int) -> list:
    """Build a symmetric iron condor around `spot` from the NEAREST expiry, purely as a
    payload for the margin/charges CALCULATORS. Nothing is placed."""
    opts = [r for r in kite_rows if r.get("name") == name
            and r.get("instrument_type") in ("CE", "PE")]
    if not opts:
        return []
    exp = sorted({r["expiry"] for r in opts})[0]
    same = [r for r in opts if r["expiry"] == exp]

    def pick(kind: str, target: float):
        c = [r for r in same if r["instrument_type"] == kind]
        if not c:
            return None
        return min(c, key=lambda r: abs(float(r["strike"]) - target))

    legs = [("SELL", pick("CE", spot + otm)), ("BUY", pick("CE", spot + otm + width)),
            ("SELL", pick("PE", spot - otm)), ("BUY", pick("PE", spot - otm - width))]
    out = []
    for side, row in legs:
        if row is None:
            continue
        out.append({
            "exchange": "NFO",
            "tradingsymbol": row["tradingsymbol"],
            "transaction_type": side,
            "variety": "regular",
            "product": "NRML",
            "order_type": "MARKET",
            "quantity": int(row.get("lot_size") or 0),
        })
    return out


def probe_margin_and_charges(kite, kite_rows: list, name: str, spot: float,
                             width: int, otm: int) -> dict:
    legs = _condor_legs(kite_rows, name, spot, width, otm)
    if len(legs) != 4:
        return {"error": "could not assemble 4 legs (got %d)" % len(legs), "legs": legs}
    out = {"legs": legs,
           "note": "READ-ONLY calculators; no order is created by either call"}
    try:
        out["basket_margin"] = kite.basket_order_margins(legs, consider_positions=False)
    except Exception as e:                              # noqa: BLE001
        out["basket_margin"] = {"error": type(e).__name__ + ": " + str(e)}
    try:
        out["virtual_contract_note"] = kite.get_virtual_contract_note(legs)
    except Exception as e:                              # noqa: BLE001
        out["virtual_contract_note"] = {"error": type(e).__name__ + ": " + str(e)}
    return out


# --------------------------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser(
        description="Options Agent Step-0 Kite data probe (read-only)")
    ap.add_argument("--name", default="NIFTY", help="underlying 'name' in the NFO master")
    ap.add_argument("--spot", type=float, default=None,
                    help="underlying spot; default = fetched live via quote()")
    ap.add_argument("--width", type=int, default=200, help="condor wing width (points)")
    ap.add_argument("--otm", type=int, default=400, help="short strike distance from spot")
    ap.add_argument("--out", default=None, help="write the JSON report here")
    args = ap.parse_args()

    report: dict = {"probe": "options_step0", "name": args.name,
                    "run_at": datetime.now().isoformat(timespec="seconds")}

    try:
        from services.kite_auth import get_kite_client
    except Exception as e:                              # noqa: BLE001
        print("FATAL: cannot import services.kite_auth (%s)" % e)
        return 2
    try:
        kite = get_kite_client()
    except Exception as e:                              # noqa: BLE001
        # Honest exit: no credentials -> no measurement. We report the gap, not a guess.
        report["status"] = "NO_KITE_SESSION"
        report["error"] = type(e).__name__ + ": " + str(e)
        print(json.dumps(report, indent=2))
        print("\nNo Kite session available -- nothing was measured. "
              "Provide credentials and re-run; this script never fabricates a result.")
        return 1

    report["status"] = "OK"
    rows = kite.instruments("NFO")

    spot = args.spot
    if spot is None:
        for sym in ("NSE:" + args.name + " 50", "NSE:" + args.name, "NSE:NIFTY 50"):
            try:
                spot = float(kite.quote(sym)[sym]["last_price"])
                report["spot_source"] = sym
                break
            except Exception:                           # noqa: BLE001, PERF203
                continue
    report["spot"] = spot

    report["q1_instruments"] = probe_instruments(kite, args.name)
    report["q2_history"] = probe_history(kite, rows, args.name, spot)
    report["q3_quote"] = probe_quote(kite, rows, args.name)
    if spot:
        report["q4q5_margin_and_charges"] = probe_margin_and_charges(
            kite, rows, args.name, spot, args.width, args.otm)
    else:
        report["q4q5_margin_and_charges"] = {"skipped": "no spot resolved"}

    text = json.dumps(report, indent=2, default=_jsonable)
    print(text)
    if args.out:
        p = Path(args.out)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
        print("\nwritten -> %s" % p)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
