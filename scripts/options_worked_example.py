"""
Options Agent · WORKED EXAMPLE on a real NIFTY chain (READ-ONLY).

Builds one point-in-time chain snapshot from live Kite, solves IV + greeks with
agents.options.pricing (Kite publishes neither), runs the governed iron-condor constructor,
and prints every candidate with its real metrics.

This is the brief's "real worked example" acceptance item, and it is also the end-to-end
validation that pricing.py -> payoff.py -> strategies/iron_condor.py agree on real data
rather than only on synthetic legs.

EXECUTION BOUNDARY: instruments() + quote() only. No order, no margin POST, no GTT.

Usage:
    python scripts/options_worked_example.py
    python scripts/options_worked_example.py --expiry 2026-09-29 --strikes 60
"""
from __future__ import annotations

import argparse
import datetime as dt
import math
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend"))

from agents.options import pricing as P              # noqa: E402
from agents.options import payoff as PO              # noqa: E402
from agents.options.strategies import _legs as L     # noqa: E402
from agents.options.strategies.iron_condor import (  # noqa: E402
    IronCondorConstructor, CONDOR_PARAMS)


def _kite():
    from services.kite_auth import get_kite_client
    return get_kite_client()


def build_snapshot(kite, underlying: str, expiry: str, n_strikes: int) -> tuple:
    """One point-in-time chain snapshot: master rows + quotes + solved IV/greeks.

    The forward is the SAME-EXPIRY future from the SAME snapshot (falling back to the
    nearest listed future), which is what makes Black-76 valid here and removes the dividend
    and most of the rate assumption."""
    rows = kite.instruments("NFO")
    opts = [r for r in rows if r.get("name") == underlying
            and r.get("instrument_type") in ("CE", "PE")]
    futs = [r for r in rows if r.get("name") == underlying
            and r.get("instrument_type") == "FUT"]
    if not opts:
        raise SystemExit("no %s options in the NFO master" % underlying)

    all_exp = sorted({str(r["expiry"])[:10] for r in opts})
    expiry = expiry or all_exp[0]
    if expiry not in all_exp:
        raise SystemExit("expiry %s not listed. available: %s" % (expiry, all_exp[:8]))

    spot = float(kite.quote("NSE:NIFTY 50")["NSE:NIFTY 50"]["last_price"])

    # Forward resolution. NIFTY WEEKLIES HAVE NO SAME-EXPIRY FUTURE (measured: weeklies expire
    # Tuesdays, futures are monthly), so the previous "nearest future" fallback priced every
    # weekly off the MONTHLY future. On a 7-DTE weekly that is a ~95-point forward error, which
    # manufactures a ~4.5-vol-point fake put skew out of a flat surface, shifts every delta,
    # and then makes delta-based strike selection place the condor asymmetrically -- a
    # directional trade wearing a condor's clothes. The delta-symmetry guard cannot catch it
    # because both deltas move the same way.
    #
    # Correct preference order, matching fetch_kite._forward_for_expiry:
    #   same-expiry future -> parity-implied from near-ATM pairs -> nearest LATER future (labelled)
    futs.sort(key=lambda r: str(r["expiry"]))
    fut_row = next((f for f in futs if str(f["expiry"])[:10] == expiry), None)
    fut_exact = fut_row is not None

    # Keep the strike window around spot -- a condor never reaches the far wings.
    same = [r for r in opts if str(r["expiry"])[:10] == expiry]
    same.sort(key=lambda r: abs(float(r["strike"]) - spot))
    window = same[: n_strikes * 2]

    syms = ["NFO:" + r["tradingsymbol"] for r in window]
    quotes = {}
    for i in range(0, len(syms), 200):                 # Kite caps a quote() batch
        quotes.update(kite.quote(syms[i:i + 200]))

    as_of = dt.date.today().isoformat()
    d = L.dte(as_of, expiry)
    T = P.year_fraction(d)

    def _px_of(row):
        q = quotes.get("NFO:" + row["tradingsymbol"]) or {}
        dep = q.get("depth") or {}
        b = float((dep.get("buy") or [{}])[0].get("price") or 0) or None
        a = float((dep.get("sell") or [{}])[0].get("price") or 0) or None
        if b and a and a >= b:
            return (b + a) / 2.0
        return float(q.get("last_price") or 0) or None

    forward, forward_source = None, None
    if fut_row:                                        # (1) same-expiry future
        fq = kite.quote("NFO:" + fut_row["tradingsymbol"])
        forward = float(fq["NFO:" + fut_row["tradingsymbol"]]["last_price"]) or None
        forward_source = "fut_same_expiry"
    if forward is None and T > 0:                      # (2) parity-implied, near-ATM pairs
        by_k = {}
        for r in window:
            by_k.setdefault(float(r["strike"]), {})[r["instrument_type"]] = r
        implied = []
        for K in sorted(by_k, key=lambda k: abs(k - spot))[:10]:
            pair = by_k[K]
            if "CE" in pair and "PE" in pair:
                c, pu = _px_of(pair["CE"]), _px_of(pair["PE"])
                if c and pu:
                    implied.append(K + (c - pu) * math.exp(P.R_DEFAULT * T))
        if implied:
            implied.sort()
            forward = implied[len(implied) // 2]       # median, robust to one bad pair
            forward_source = "parity_implied(n=%d)" % len(implied)
    if forward is None and futs:                       # (3) later future, LABELLED
        later = [f for f in futs if str(f["expiry"])[:10] >= expiry]
        if later:
            fq = kite.quote("NFO:" + later[0]["tradingsymbol"])
            forward = float(fq["NFO:" + later[0]["tradingsymbol"]]["last_price"]) or None
            forward_source = "fut_nearest_%s_BASIS_CONTAMINATED" % str(later[0]["expiry"])[:10]
    if forward is None:
        raise SystemExit("no forward resolvable for %s -- refusing to substitute spot" % expiry)

    chain, solved, unsolved = [], 0, 0
    for r in window:
        q = quotes.get("NFO:" + r["tradingsymbol"]) or {}
        depth = q.get("depth") or {}
        buy = (depth.get("buy") or [{}])[0]
        sell = (depth.get("sell") or [{}])[0]
        bid = float(buy.get("price") or 0) or None
        ask = float(sell.get("price") or 0) or None
        ltp = float(q.get("last_price") or 0) or None
        right = r["instrument_type"]

        px = ((bid + ask) / 2.0) if (bid and ask and ask >= bid) else ltp
        iv = greeks = None
        if px and T > 0:
            iv = P.implied_vol(px, forward, float(r["strike"]), T, right)
            if iv is not None:
                greeks = P.greeks(forward, float(r["strike"]), T, iv, right)
        solved += 1 if iv is not None else 0
        unsolved += 0 if iv is not None else 1

        chain.append({
            "underlying": underlying, "as_of_date": as_of, "spot": spot,
            "forward": forward, "snapshot_id": "live-%s" % as_of,
            "tradingsymbol": r["tradingsymbol"],
            "instrument_token": r["instrument_token"],
            "strike": float(r["strike"]), "right": right,
            "expiry": str(r["expiry"])[:10], "lot_size": int(r["lot_size"]),
            "ltp": ltp, "bid": bid, "ask": ask,
            "oi": q.get("oi"), "volume": q.get("volume"),
            "iv": iv,
            "delta": greeks["delta"] if greeks else None,
            "gamma": greeks["gamma"] if greeks else None,
            "theta": greeks["theta_per_day"] if greeks else None,
            "vega": greeks["vega_per_1pct_vol"] if greeks else None,
        })

    meta = {"expiry": expiry, "spot": spot, "forward": forward, "dte": d,
            "fut_exact": fut_exact,
            "fut_symbol": fut_row["tradingsymbol"] if fut_row else None,
            "forward_source": forward_source,
            "contracts": len(chain), "iv_solved": solved, "iv_unsolved": unsolved,
            "all_expiries": all_exp}
    return chain, meta


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--underlying", default="NIFTY")
    ap.add_argument("--expiry", default=None)
    ap.add_argument("--strikes", type=int, default=60)
    args = ap.parse_args()

    kite = _kite()
    chain, meta = build_snapshot(kite, args.underlying, args.expiry, args.strikes)

    print("=" * 78)
    print("SNAPSHOT  %s  expiry %s  DTE %d" % (args.underlying, meta["expiry"], meta["dte"]))
    print("  spot %.2f | forward %.2f  [source: %s]"
          % (meta["spot"], meta["forward"], meta["forward_source"]))
    print("  contracts %d | IV solved %d | IV unsolved %d"
          % (meta["contracts"], meta["iv_solved"], meta["iv_unsolved"]))
    atm = L.atm_iv(chain, meta["expiry"], meta["forward"])
    print("  ATM IV %s" % ("%.2f%%" % (atm * 100) if atm else "unsolved"))
    if atm:
        em = L.expected_move(meta["forward"], atm, P.year_fraction(meta["dte"]))
        print("  1-sigma expected move to expiry: %.0f points (%.2f%%)"
              % (em, em / meta["forward"] * 100))

    ic = IronCondorConstructor()
    occs = ic.construct(chain, as_of_idx=None)
    print("\nCONSTRUCTOR -> %d candidate(s)   [params %s]" % (len(occs), ic.params_version))

    for o in occs:
        print("\n" + "-" * 78)
        print("%s  %s  expiry %s (%s, DTE %d)  STAGE=%s"
              % (o["strategy"], o["underlying"], o["expiry"], o["expiry_kind"],
                 o["dte"], o["stage"]))
        if not o["legs"]:
            for r in o["reasons"]:
                print("   ! %s" % r)
            continue
        sig = o["signature"]
        print("   structure: SELL %gPE / BUY %gPE  ..  SELL %gCE / BUY %gCE   width %g"
              % (sig["short_put"], sig["long_put"], sig["short_call"], sig["long_call"],
                 sig["width"]))
        print("   %-6s %-4s %-18s %9s %9s %9s %8s %9s"
              % ("side", "R", "symbol", "price", "bid", "ask", "delta", "OI"))
        for l in o["legs"]:
            print("   %-6s %-4s %-18s %9.2f %9s %9s %8s %9s"
                  % (l["action"], l["right"], l["tradingsymbol"], l["price_used"],
                     "%.2f" % l["bid"] if l["bid"] else "-",
                     "%.2f" % l["ask"] if l["ask"] else "-",
                     "%.3f" % l["delta"] if l["delta"] is not None else "-",
                     int(l["oi"]) if l["oi"] is not None else "-"))
        m = o["metrics"]
        if "credit_total" in m:
            print("   credit      Rs %.2f   (%.1f%% of width)"
                  % (m["credit_total"], o["context"]["credit_pct_width"] * 100))
            print("   max profit  Rs %.2f" % m["max_profit"])
            print("   max loss    Rs %.2f" % m["max_loss"])
            print("   R:R         %.3f" % (m["risk_reward"] or 0))
            print("   breakevens  %s" % ["%.1f" % b for b in m["breakevens"]])
            print("   POP(profit) %.1f%%   POP(max)  %.1f%%   [%s]"
                  % (m["pop_breakeven"] * 100, (m["pop_body"] or 0) * 100,
                     m.get("pop_basis") or "unknown"))
            ev = m["ev"]
            print("   EV(risk-neutral, ANALYTIC, net of costs)  Rs %.2f  [%s]"
                  % (ev["ev_riskneutral"], ev["basis"]))
            print("        = carry Rs %.2f  -  costs Rs %.2f   (a tautology, never an edge)"
                  % (ev["ev_riskneutral_carry"], ev["costs_charged"]))
            fv = ev.get("flat_vol_diagnostic") or {}
            if "flat_vol_ev" in fv:
                print("   flat-vol diagnostic  Rs %.2f   -> model error Rs %.2f (NOT signal)"
                      % (fv["flat_vol_ev"], fv["model_error_vs_analytic"]))
            print("   EV(real-world)  %s" % ev["ev_realworld"])
            print("   costs       Rs %.2f  [%s]" % (m["costs"]["total"],
                                                    m["costs"]["costs_basis"]))
            print("   margin est  Rs %.2f  [%s, measured=%s]"
                  % (m["margin"]["margin_est"], m["margin"]["basis"],
                     m["margin"]["measured"]))
            g = m["greeks"]
            print("   greeks      delta %+.2f  theta/day %+.2f  vega/1%% %+.2f"
                  % (g["delta"], g["theta_per_day"], g["vega_per_1pct_vol"]))
            liq = m["liquidity"]
            print("   liquidity   worst spread %s  min OI %s"
                  % ("%.1f%%" % liq["worst_spread_pct"] if liq["worst_spread_pct"] else "-",
                     int(liq["min_oi"]) if liq["min_oi"] is not None else "-"))
        print("   IV rank     %s" % o["regime"]["iv_rank_status"])
        for r in o["reasons"]:
            print("   ! %s" % r)

    print("\n" + "=" * 78)
    print("NOTE: the constructor does NOT decide TRADE/WATCH. That is the gate stack's job,")
    print("fed by realised tracked outcomes. Nothing above is a recommendation.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
