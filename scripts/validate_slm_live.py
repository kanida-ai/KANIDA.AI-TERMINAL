"""REAL-KITE-API validation for the MIS protective SL-M fix (b076713).

WHY: the SL-M backstop was mock-tested only and shipped BROKEN — Kite rejected it
("Market orders without market protection are not allowed via API") because the
order omitted `market_protection`. The fix adds market_protection=5.0. This probe
proves the fix against the REAL Kite API — NOT a mock — before the SL-M is trusted.

WHAT IT DOES (negligible risk):
  1. Builds a LIVE Zerodha client (dry_run=False) using your default profile.
  2. Reads the LTP of a liquid symbol (default SBIN).
  3. Places a 1-SHARE protective SL-M via the REAL code path
     (ZerodhaBroker.place_protective_slm) with the trigger set ~50% BELOW LTP so
     it can NEVER trigger.
  4. If Kite ACCEPTS it (returns an order_id) → the fix works → PASS. It then
     CANCELS the order immediately (cancel_protective_slm).
  5. If Kite REJECTS it (None) → FAIL, and the reason is in logs/backend.log
     ("place_protective_slm failed for ...").

SAFETY: 1 share, trigger 50% away (un-triggerable), cancelled within ~1s. Worst
case if it somehow filled (a 50% instant crash — impossible): ~one share sold.
Run during market hours so the order is live-placeable + cancelable.

RUN (from the repo root, with your live Kite session authed):
    PYTHONPATH=backend python scripts/validate_slm_live.py            # SBIN
    PYTHONPATH=backend python scripts/validate_slm_live.py INFY       # other symbol

This PLACES A REAL (immediately-cancelled) ORDER — Claude cannot run it for you;
you run it. It creates NO autotrade session and touches NO strategy state.
"""
import sys

from autotrade.config import BrokerProfile
from autotrade.broker.router import build_client


def main() -> int:
    symbol = (sys.argv[1] if len(sys.argv) > 1 else "SBIN").upper()

    prof = BrokerProfile(
        profile_id="zerodha_default", broker_name="zerodha",
        allocated_capital=100000.0, order_product="MIS",
        instrument_type="EQ", broker_account_id=None)
    broker = build_client(prof, dry_run=False)   # LIVE

    if not broker._live_allowed():
        print("ABORT: live orders not allowed (dry_run on OR FALCON_AUTOTRADE_ENABLED "
              "off OR token missing). Can't validate against the real API right now.")
        return 2

    ltp = broker.get_ltp(symbol)
    if not ltp or ltp <= 0:
        print(f"ABORT: could not read a live LTP for {symbol} — try another liquid "
              f"symbol: python scripts/validate_slm_live.py INFY")
        return 2

    # long-stop SELL SL-M, trigger ~50% BELOW LTP → un-triggerable.
    trigger = round(ltp * 0.5, 1)
    print(f"[probe] {symbol} LTP={ltp:.2f} → placing 1-share protective SL-M "
          f"(SELL, trigger={trigger:.2f}, product=MIS, market_protection=5.0) …")

    oid = broker.place_protective_slm(
        symbol=symbol, qty=1, trigger_price=trigger,
        direction="long", product="MIS")

    if oid:
        print(f"PASS ✅  Kite ACCEPTED the SL-M — order_id={oid}. The fix works: "
              f"market_protection is now supplied. Cancelling the probe order …")
        ok = False
        try:
            ok = broker.cancel_protective_slm(str(oid))
        except Exception as e:                      # noqa: BLE001
            print(f"   WARN: cancel raised: {e}")
        if ok:
            print("   Cancelled cleanly. Nothing left in the book.")
        else:
            print(f"   ⚠️  Could NOT auto-cancel order {oid} — CANCEL IT MANUALLY in "
                  f"Kite now (it's a 1-share SL-M at {trigger:.2f}, won't trigger).")
        return 0

    print("FAIL ❌  Kite REJECTED the SL-M (place_protective_slm returned None). "
          "The exact reason is in logs/backend.log — grep 'place_protective_slm failed'. "
          "The fix did NOT satisfy Kite; do not trust the broker-side backstop yet.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
