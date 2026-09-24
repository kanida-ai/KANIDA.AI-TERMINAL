"""
TrueData · EXPIRED-OPTIONS PROBE  (READ-ONLY)

Answers the ONE question that decides whether TrueData is our vendor:

    Does TrueData serve historical data for EXPIRED NSE option contracts?

TrueData publishes history depth by TIMEFRAME (tick = 5 days, minute = 6 months, daily =
10+ years) but says nothing public about whether that covers EXPIRED option contracts. Kite
looked fine on depth too and turned out to serve live contracts only. So we measure before
paying, exactly as we did with Kite.

Written against the INSTALLED library (truedata 7.0.3), whose real API is:

    TD_hist(login_id, password)
        .get_bhavcopy(segment, date)                       -> List[Dict]
        .get_historic_data(contract, start_time=, end_time=, bar_size=, bidask=)
        .get_n_historical_bars(contract, end_time=, no_of_bars=, bar_size=)
    TD_analytics(login_id, password)
        .get_history_greeks(symbol, expiry, strike, series, ltp=False)
        .get_option_chain(symbol, expiry, greeks=False)

Two of those are PARAMETERISED by (symbol, expiry, strike, series) rather than by a symbol
string, so they sidestep TrueData's unpublished option nomenclature entirely. That is the
strongest form of this test: no guessing, no false negative from a wrong symbol format.

CREDENTIALS — never pasted into a chat, never committed. Put them in config/.env:
    TD_USERNAME=your_username
    TD_PASSWORD=your_password
They are read, used to log in, and never printed.

RUN:
    C:\\Users\\SPS\\anaconda3\\python.exe scripts/truedata_expired_probe.py

Places no orders, touches no broker. Reads history only.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# The exact 2022 condor we could NOT reach through Kite ("invalid token").
EXPIRY = datetime(2022, 11, 24)
ENTRY = datetime(2022, 11, 18)
LEGS = [(17700, "PE"), (17500, "PE"), (18700, "CE"), (18900, "CE")]


def _load_env_file():
    for candidate in (ROOT / "config" / ".env", ROOT / ".env"):
        if not candidate.exists():
            continue
        for line in candidate.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _creds():
    _load_env_file()
    u = os.environ.get("TD_USERNAME", "").strip()
    p = os.environ.get("TD_PASSWORD", "").strip()
    if not u or not p:
        print("No TrueData credentials found.\n"
              "Add these two lines to config/.env (the file your Kite keys live in):\n"
              "    TD_USERNAME=your_username\n"
              "    TD_PASSWORD=your_password\n"
              "Then re-run. Nothing is printed back and nothing is committed.")
        raise SystemExit(2)
    return u, p


def _try(label, fn, *a, **kw):
    """Run one probe, print a one-line verdict, return the result or None."""
    try:
        out = fn(*a, **kw)
    except Exception as e:                                # noqa: BLE001
        print("   %-46s ERROR  %s: %s" % (label, type(e).__name__, str(e)[:80]))
        return None
    if out is None:
        print("   %-46s returned None" % label)
        return None
    try:
        n = len(out)
    except Exception:                                     # noqa: BLE001
        print("   %-46s OK (%s)" % (label, type(out).__name__))
        return out
    print("   %-46s %s rows" % (label, n))
    return out


def main() -> int:
    u, p = _creds()
    try:
        from truedata import TD_hist, TD_analytics
    except ImportError:
        print("`truedata` not installed. Run:\n"
              "    C:\\Users\\SPS\\anaconda3\\python.exe -m pip install truedata")
        return 2

    print("connecting to TrueData ...")
    try:
        hist = TD_hist(u, p)
    except Exception as e:                                # noqa: BLE001
        print("LOGIN FAILED -> %s: %s" % (type(e).__name__, str(e)[:140]))
        return 1

    # TD_hist's constructor does NOT raise on a bad login -- it logs the error and hands back
    # an object with access_token=None. Every later call then dies with a confusing
    # "'<' not supported between NoneType and datetime" thrown by the token-expiry decorator.
    # Without this check the probe prints a wall of type errors and looks like a DATA problem
    # when it is actually an AUTH problem. (It did exactly that on the first run.)
    if getattr(hist, "access_token", None) is None:
        print("\nLOGIN FAILED - no access token was issued.")
        print("TrueData's own message: 'The user name or password is incorrect or "
              "subscription expired.'")
        print("\nNothing below this line was measured. Likely causes, in order:")
        print("  1. The API login is NOT the website email + password. TrueData issues a")
        print("     SEPARATE API user id and password, emailed after signup together with")
        print("     the documentation and sample code. Use those.")
        print("  2. No active subscription or trial on the account yet.")
        print("  3. Trial issued for a different segment than the one being requested.")
        return 1
    print("connected (access token issued).\n")

    # ══════════════════════════════════════════ CONTROL — prove the pipe works at all
    print("=" * 76)
    print("CONTROL 1 — NIFTY index, recent daily bars")
    _try("get_historic_data('NIFTY-I', 30d, 1 day)", hist.get_historic_data,
         "NIFTY-I", start_time=datetime.now() - timedelta(days=30),
         end_time=datetime.now(), bar_size="1 day")

    print("\nCONTROL 2 — daily-bar depth (their '10+ years' claim)")
    bars = _try("get_historic_data('NIFTY-I', since 2015)", hist.get_historic_data,
                "NIFTY-I", start_time=datetime(2015, 1, 1), end_time=datetime.now(),
                bar_size="1 day")
    if bars:
        try:
            print("        earliest: %s" % (bars[0],))
        except Exception:                                 # noqa: BLE001
            pass

    # ══════════════════════════════ TEST A — BHAVCOPY for an expired date (the cleanest)
    # A bhavcopy for 24-Nov-2022 must list EVERY F&O contract traded that day, including
    # every option strike. If this works, we have the whole chain for any past date.
    print("\n" + "=" * 76)
    print("TEST A — F&O BHAVCOPY for %s (an expired date)" % EXPIRY.date())
    bhav = None
    for seg in ("FO", "F&O", "NFO", "FUT", "EQ"):
        got = _try("get_bhavcopy('%s', 2022-11-24)" % seg, hist.get_bhavcopy, seg, EXPIRY)
        if got:
            bhav = (seg, got)
            break
    if bhav:
        seg, rows = bhav
        print("\n   sample row: %s" % (rows[0],))
        hits = []
        for r in rows:
            blob = str(r).upper()
            if "17700" in blob and "PE" in blob and "NIFTY" in blob:
                hits.append(r)
        print("   rows mentioning NIFTY 17700 PE: %d" % len(hits))
        for h in hits[:3]:
            print("      %s" % (h,))

    # ══════════════════════════ TEST B — PARAMETERISED historical greeks (no symbol guess)
    print("\n" + "=" * 76)
    print("TEST B — get_history_greeks() on the expired 2022 legs")
    print("(parameterised by symbol/expiry/strike/series — no symbol-format guessing)")
    try:
        ana = TD_analytics(u, p)
    except Exception as e:                                # noqa: BLE001
        print("   TD_analytics login failed: %s: %s" % (type(e).__name__, str(e)[:80]))
        ana = None
    if ana:
        for strike, series in LEGS:
            _try("history_greeks NIFTY %s %s %s" % (EXPIRY.date(), strike, series),
                 ana.get_history_greeks, "NIFTY", EXPIRY, strike, series, True)

        print("\n   and the whole chain as it stood for that expiry:")
        _try("get_option_chain('NIFTY', 2022-11-24)", ana.get_option_chain, "NIFTY", EXPIRY)

    # ══════════════════════════ TEST C — does history carry BID/ASK? (post-market problem)
    print("\n" + "=" * 76)
    print("TEST C — bidask=True on a RECENT contract")
    print("(Kite gives no book after the close; if TrueData archives bid/ask, that is solved)")
    _try("get_historic_data('NIFTY-I', bidask=True)", hist.get_historic_data,
         "NIFTY-I", start_time=datetime.now() - timedelta(days=3),
         end_time=datetime.now(), bar_size="1 min", bidask=True)

    print("\n" + "=" * 76)
    print("Read the rows above literally. What matters:")
    print("  * TEST A returning NIFTY 17700 PE rows  -> the full past chain is available")
    print("  * TEST B returning greeks for 2022      -> expired contracts are served")
    print("  * either one passing means the 2022 backtest question is ANSWERABLE")
    print("  * both empty is NOT proof of absence — it may be a plan entitlement. Ask:")
    print("    'Does my plan return historical data for EXPIRED NSE index option contracts")
    print("     (e.g. NIFTY 24-Nov-2022 17700 PE)? Which endpoint, and how far back?'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
