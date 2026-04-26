"""
KANIDA — FAST BATCH SCANNER (DB-backed)
=========================================
Reads pre-computed fingerprints from kanida_fingerprints.db.
Only runs Bot 2 (live scorer) live — no fingerprint recomputation.

Speed: 187 stocks in under 60 seconds vs 8+ minutes for full recompute.

Prerequisite: stocks must be built first via kanida_agent_builder.py
  python kanida_agent_builder.py --build HDFCBANK,RELIANCE,TCS --market NSE
  python kanida_agent_builder.py --build-universe NSE   (full universe)

Usage:
  # Scan all built NSE stocks — bullish
  python kanida_batch_scanner.py --market NSE --bias bullish

  # Scan all built US stocks — bearish
  python kanida_batch_scanner.py --market US --bias bearish

  # Scan specific tickers
  python kanida_batch_scanner.py --tickers HDFCBANK,RELIANCE,TCS --market NSE --bias bullish

  # Neutral / range setups
  python kanida_batch_scanner.py --market NSE --bias neutral

  # Filter by minimum score
  python kanida_batch_scanner.py --market NSE --bias bullish --min-score 40

  # Save results to CSV
  python kanida_batch_scanner.py --market NSE --bias bullish --save results.csv

  # Deep dive on a result
  python kanida_batch_scanner.py --market NSE --bias bullish --deep HDFCBANK

  # Show scan history for a stock
  python kanida_batch_scanner.py --history HDFCBANK --market NSE

  # Show which stocks are built and ready
  python kanida_batch_scanner.py --ready --market NSE
"""

import warnings; warnings.filterwarnings("ignore")
import argparse
import io
import sys
import time
from datetime import datetime
from typing import List, Optional, Dict

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    from custom_agent import (
        STRATEGY_MAP, BACKTEST_YEARS, NSE_FNO, US_OPTIONS,
        fetch_data, fetch_live_price, fetch_regime,
        score_live, check_regime, calculate_risk, _atr,
        StrategyRecord, LiveScore, RegimeContext,
    )
    AGENT_AVAILABLE = True
except ImportError as e:
    print(f"❌  Cannot import custom_agent: {e}")
    sys.exit(1)

try:
    from kanida_db import (
        init_db, get_conn, get_agent, list_agents,
        get_fingerprint, fingerprint_exists, get_strategy_summary,
        get_ledger_stats, save_scan_result,
        print_db_summary, DB_PATH,
    )
    DB_AVAILABLE = True
except ImportError as e:
    print(f"❌  Cannot import kanida_db: {e}")
    sys.exit(1)

try:
    from kanida_agent_builder import query_agent, ALL_BIASES
    BUILDER_AVAILABLE = True
except ImportError:
    BUILDER_AVAILABLE = False

TODAY = datetime.today()
W = 114


# ══════════════════════════════════════════════════════════════════
# CORE — score one stock from DB (no fingerprint recompute)
# ══════════════════════════════════════════════════════════════════

def scan_one(ticker: str, market: str, bias: str,
             timeframe: str = "1D", backtest_years: int = 5,
             capital: float = 500_000, max_risk: float = 2.0,
             db_path: str = DB_PATH) -> Dict:
    """
    Fast scan for one stock:
      1. Read qualified strategies from DB (instant)
      2. Fetch last 60 days of price data (live)
      3. Run Bot 2 (live scorer) only
      4. Return result dict
    """
    try:
        # Step 1 — load fingerprint from DB
        fp_rows = get_fingerprint(ticker, market, bias, timeframe,
                                  backtest_years, qualified_only=True,
                                  db_path=db_path)
        if not fp_rows:
            return {
                "ticker": ticker, "market": market, "bias": bias,
                "error": "not_built", "qualified": False,
                "score_pct": 0, "firing_count": 0, "qualified_total": 0,
            }

        # Reconstruct StrategyRecord list
        strategy_records = [StrategyRecord(
            name=r["strategy_name"],
            appearances=r["appearances"],
            wins=r["wins"],
            win_rate=r["win_rate"],
            avg_forward=r["avg_forward"],
            qualifies=True,
        ) for r in fp_rows]

        qualified_total = len(strategy_records)

        # Step 2 — fetch recent price data for live check
        # 60 days is enough for all strategy lookback windows on 1D
        # For 1W we need ~40 weeks
        fetch_days = 90 if timeframe in ("1D", "4H", "1H", "30m") else 400
        df = fetch_data(ticker, market, timeframe,
                        years=max(1, fetch_days // 252 + 1))
        if df is None or len(df) < 5:
            return {
                "ticker": ticker, "market": market, "bias": bias,
                "error": "no_live_data", "qualified": False,
                "score_pct": 0, "firing_count": 0,
                "qualified_total": qualified_total,
            }

        # Step 3 — live score (Bot 2 only)
        live_score = score_live(df, strategy_records, bias)

        # Step 4 — regime
        regime = check_regime(market, bias, ticker)

        # Step 5 — live price + risk
        live_price = fetch_live_price(ticker, market) or float(df["Close"].iloc[-1])
        risk = calculate_risk(live_price, capital, max_risk,
                              live_score, market, df, bias)

        # Step 6 — ledger stats from DB
        ledger = get_ledger_stats(ticker, market, bias, "historical", db_path)

        return {
            "ticker":          ticker,
            "market":          market,
            "bias":            bias,
            "timeframe":       timeframe,
            "qualified_total": qualified_total,
            "firing_count":    live_score.firing_count,
            "score_pct":       round(live_score.score_pct, 1),
            "score_label":     live_score.score_label,
            "top_strategy":    live_score.top_strategy.name if live_score.top_strategy else "",
            "top_win_rate":    round(live_score.top_strategy.win_rate * 100, 1) if live_score.top_strategy else 0,
            "firing_strategies": [s.name for s in live_score.firing_strategies],
            "regime":          regime.regime,
            "regime_score":    round(regime.regime_score, 1),
            "bias_aligned":    regime.bias_aligned,
            "sector":          regime.sector,
            "live_price":      round(live_price, 2),
            "stop_loss":       round(risk.stop_loss, 2),
            "stop_pct":        round(risk.stop_pct, 2),
            "target_1":        round(risk.target_1, 2),
            "target_2":        round(risk.target_2, 2),
            "kelly_fraction":  round(risk.kelly_fraction * 100, 1),
            "position_size":   round(risk.position_size, 0),
            "hist_win_pct":    ledger.get("win_pct", 0),
            "hist_avg_outcome":ledger.get("avg_outcome", 0),
            "hist_total":      ledger.get("total", 0),
            "qualified":       live_score.score_pct >= 35,
            "error":           None,
        }

    except Exception as e:
        return {
            "ticker": ticker, "market": market, "bias": bias,
            "error": str(e), "qualified": False,
            "score_pct": 0, "firing_count": 0, "qualified_total": 0,
        }


# ══════════════════════════════════════════════════════════════════
# BATCH RUNNER
# ══════════════════════════════════════════════════════════════════

def run_batch(tickers: List[str], market: str, bias: str,
              timeframe: str = "1D",
              capital: float = 500_000, max_risk: float = 2.0,
              delay: float = 0.3,
              db_path: str = DB_PATH) -> List[Dict]:

    results = []
    total   = len(tickers)

    # Determine backtest_years per stock from agent record
    # (use first available, fall back to 5)
    agent_years: Dict[str, int] = {}
    for t in tickers:
        a = get_agent(t, market, db_path)
        agent_years[t] = a["backtest_years"] if a else 5

    print(f"\n{'━'*W}")
    print(f"  KANIDA FAST BATCH SCANNER  (DB-backed — no fingerprint recompute)")
    print(f"  Market: {market.upper()}  ·  Bias: {bias.upper()}  ·  Timeframe: {timeframe}")
    print(f"  Universe: {total} stocks  ·  {TODAY.strftime('%d-%b-%Y %H:%M')}")
    print(f"  Only running live scorer — reads strategies from kanida_fingerprints.db")
    print(f"{'━'*W}")

    t_start = time.time()

    for n, ticker in enumerate(tickers, 1):
        pct  = n / total * 100
        bar  = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
        print(f"\r  [{bar}] {n:>3}/{total}  {ticker:<14}", end="", flush=True)

        old_stderr = sys.stderr
        sys.stderr = io.StringIO()
        try:
            result = scan_one(
                ticker=ticker, market=market, bias=bias,
                timeframe=timeframe,
                backtest_years=agent_years.get(ticker, 5),
                capital=capital, max_risk=max_risk,
                db_path=db_path,
            )
        except Exception as e:
            result = {"ticker": ticker, "market": market, "bias": bias,
                      "error": str(e), "qualified": False,
                      "score_pct": 0, "firing_count": 0, "qualified_total": 0}
        finally:
            sys.stderr = old_stderr

        results.append(result)

        # Save to scan_results table
        if not result.get("error"):
            try:
                save_scan_result(result, db_path)
            except Exception:
                pass

        if n < total:
            time.sleep(delay)

    elapsed = time.time() - t_start
    print(f"\r  {'━'*(W-2)}")

    ok        = sum(1 for r in results if not r.get("error"))
    signals   = sum(1 for r in results if r.get("qualified"))
    not_built = sum(1 for r in results if r.get("error") == "not_built")
    errors    = sum(1 for r in results if r.get("error") and r.get("error") != "not_built")

    print(f"  ✅  Scan complete in {elapsed:.0f}s  ·  {ok} scanned  ·  "
          f"{signals} with active signals  ·  {not_built} not built yet  ·  {errors} errors\n")

    return results


# ══════════════════════════════════════════════════════════════════
# RANKED TABLE
# ══════════════════════════════════════════════════════════════════

def print_ranked_table(results: List[Dict], bias: str,
                       min_score: float = 0) -> List[Dict]:

    not_built = [r for r in results if r.get("error") == "not_built"]
    errors    = [r for r in results if r.get("error") and r.get("error") != "not_built"]
    no_signal = [r for r in results if not r.get("error") and not r.get("qualified")]
    signals   = [r for r in results
                 if not r.get("error")
                 and r.get("qualified")
                 and r.get("score_pct", 0) >= min_score]

    # Sort: score_pct desc, then top_win_rate desc
    signals.sort(key=lambda r: (r.get("score_pct", 0),
                                r.get("top_win_rate", 0)), reverse=True)

    bias_icons = {"bullish": "▲", "bearish": "▼", "neutral": "◆"}
    cur_sym    = {"NSE": "₹", "US": "$"}

    print(f"\n{'━'*W}")
    print(f"  {bias_icons.get(bias,'?')}  {bias.upper()} SIGNAL SCAN — RANKED BY PER-STOCK ALIGNMENT SCORE")
    print(f"  Score = firing strategies ÷ per-stock qualified total  ·  denominator is stock-specific")
    print(f"{'━'*W}")

    if not signals:
        print(f"\n  No {bias} setups cleared the threshold today.")
        if not_built:
            print(f"  {len(not_built)} stocks not built yet — run --build-universe to include them.")
    else:
        hdr = (
            f"  {'#':>3}  {'Ticker':<12}  {'Score':>7}  {'%':>5}  "
            f"{'Top strategy':<28}  {'HWin%':>6}  "
            f"{'Regime':>7}  {'Aligned':>7}  "
            f"{'Price':>10}  {'Stop':>9}  {'T1':>9}  "
            f"{'H.Win%':>7}  {'Label'}"
        )
        print(hdr)
        print(f"  {'─'*(W-2)}")

        for rank, r in enumerate(signals, 1):
            mkt = r.get("market", "NSE").upper()
            cur = cur_sym.get(mkt, "₹")
            score_str  = f"{r.get('firing_count',0)}/{r.get('qualified_total',0)}"
            score_pct  = f"{r.get('score_pct',0):.0f}%"
            aligned    = "YES" if r.get("bias_aligned") else "NO "
            regime     = r.get("regime", "?")[:7]
            top_strat  = r.get("top_strategy", "")[:28]
            top_wr     = f"{r.get('top_win_rate',0):.0f}%"
            price      = f"{cur}{r.get('live_price',0):>9,.2f}"
            stop       = f"{cur}{r.get('stop_loss',0):>8,.2f}"
            t1         = f"{cur}{r.get('target_1',0):>8,.2f}"
            h_win      = f"{r.get('hist_win_pct',0):.0f}%"
            label      = r.get("score_label","")
            icons      = {"STRONG":"🟢","DEVELOPING":"🟡","NO SIGNAL":"⚪"}
            icon       = icons.get(label, "")

            print(
                f"  {rank:>3}  "
                f"{r.get('ticker',''):<12}  "
                f"{score_str:>7}  "
                f"{score_pct:>5}  "
                f"{top_strat:<28}  "
                f"{top_wr:>6}  "
                f"{regime:>7}  "
                f"{aligned:>7}  "
                f"{price:>10}  "
                f"{stop:>9}  "
                f"{t1:>9}  "
                f"{h_win:>7}  "
                f"{icon} {label}"
            )

        # Firing strategies detail for top 5
        print(f"\n  Top 5 — active strategies detail:")
        print(f"  {'─'*(W-2)}")
        for r in signals[:5]:
            firing = r.get("firing_strategies", [])
            if firing:
                print(f"  {r['ticker']}: {', '.join(firing[:5])}"
                      f"{'...' if len(firing) > 5 else ''}")

    print(f"\n  {'─'*(W-2)}")
    print(f"  Score: firing / per-stock qualified total  ·  6/6 outranks 8/22")
    print(f"  H.Win% = historical win rate from paper ledger (not just backtest)")
    print(f"  Regime aligned = market direction matches {bias} bias")
    if not_built:
        print(f"  {len(not_built)} stocks skipped (not built): "
              f"{', '.join(r['ticker'] for r in not_built[:8])}"
              f"{'...' if len(not_built) > 8 else ''}")
    print(f"{'━'*W}\n")

    return signals


# ══════════════════════════════════════════════════════════════════
# CONVICTION SUMMARY
# ══════════════════════════════════════════════════════════════════

def print_conviction_summary(signals: List[Dict], bias: str,
                             market: str) -> None:
    strong  = [r for r in signals if r.get("score_label") == "STRONG"]
    develop = [r for r in signals if r.get("score_label") == "DEVELOPING"]

    print(f"  ── {bias.upper()} CONVICTION SUMMARY  ·  {market.upper()}  ·  {TODAY.strftime('%d %b %Y')} ──")
    if strong:
        print(f"  🟢  STRONG ({len(strong)}):       {', '.join(r['ticker'] for r in strong)}")
    if develop:
        print(f"  🟡  DEVELOPING ({len(develop)}):  {', '.join(r['ticker'] for r in develop[:10])}")
    if not strong and not develop:
        print(f"  No strong or developing {bias} setups found today.")
    print()


# ══════════════════════════════════════════════════════════════════
# CSV EXPORT
# ══════════════════════════════════════════════════════════════════

def save_csv(results: List[Dict], path: str) -> None:
    if not PANDAS_AVAILABLE:
        print("  ⚠️  pandas not available — cannot save CSV"); return

    rows = [r for r in results if r.get("qualified")]
    if not rows:
        print("  No qualified results to save."); return

    df = pd.DataFrame(rows)
    df["firing_strategies"] = df["firing_strategies"].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else ""
    )
    df["scan_date"] = TODAY.strftime("%Y-%m-%d %H:%M")
    df = df.sort_values("score_pct", ascending=False)

    cols = [
        "ticker","market","bias","timeframe","qualified_total","firing_count",
        "score_pct","score_label","top_strategy","top_win_rate",
        "regime","regime_score","bias_aligned","sector",
        "live_price","stop_loss","stop_pct","target_1","target_2",
        "kelly_fraction","position_size",
        "hist_win_pct","hist_avg_outcome","hist_total",
        "firing_strategies","scan_date",
    ]
    cols = [c for c in cols if c in df.columns]
    df[cols].to_csv(path, index=False)
    print(f"  💾  Saved {len(rows)} results → {path}")


# ══════════════════════════════════════════════════════════════════
# READY STOCKS — show what's built
# ══════════════════════════════════════════════════════════════════

def show_ready(market: str, db_path: str = DB_PATH) -> None:
    conn  = get_conn(db_path)
    mkt_clause = "" if market.upper() == "ALL" else "AND a.market=?"
    params = [] if market.upper() == "ALL" else [market.upper()]

    rows = conn.execute(f"""
        SELECT a.ticker, a.market, a.last_built, a.backtest_years,
               COUNT(DISTINCT f.bias)  as biases,
               SUM(f.qualifies)        as qualified,
               COUNT(DISTINCT p.id)    as paper_trades
        FROM agents a
        LEFT JOIN fingerprints f ON f.ticker=a.ticker AND f.market=a.market
        LEFT JOIN paper_ledger p ON p.ticker=a.ticker AND p.market=a.market
        WHERE a.status='ready' {mkt_clause}
        GROUP BY a.ticker, a.market
        ORDER BY a.market, a.ticker
    """, params).fetchall()
    conn.close()

    if not rows:
        print(f"  No built agents found for {market.upper()}.")
        print(f"  Run: python kanida_agent_builder.py --build TICKER --market {market.upper()}")
        return

    print(f"\n  BUILT AGENTS — {market.upper()}  ({len(rows)} stocks ready)")
    print(f"  {'─'*75}")
    print(f"  {'TICKER':<12} {'MARKET':<6} {'BIASES':>6} {'QUALIFIED':>10} "
          f"{'PAPER TRADES':>13} {'BACKTEST':>9} {'LAST BUILT'}")
    print(f"  {'─'*75}")
    for r in rows:
        print(f"  {r['ticker']:<12} {r['market']:<6} {r['biases']:>6} "
              f"{r['qualified'] or 0:>10} {r['paper_trades'] or 0:>13} "
              f"{r['backtest_years']:>7}y  {(r['last_built'] or '—')[:19]}")
    print()


# ══════════════════════════════════════════════════════════════════
# SCAN HISTORY
# ══════════════════════════════════════════════════════════════════

def show_history(ticker: str, market: str,
                 bias: Optional[str] = None,
                 db_path: str = DB_PATH) -> None:
    from kanida_db import get_scan_history
    rows = get_scan_history(ticker, market, bias, days=30, db_path=db_path)
    if not rows:
        print(f"  No scan history for {ticker.upper()} ({market.upper()}).")
        return

    print(f"\n  SCAN HISTORY — {ticker.upper()}  ·  last 30 entries")
    print(f"  {'─'*80}")
    print(f"  {'DATE':<12} {'BIAS':<10} {'SCORE':>7} {'%':>5} {'LABEL':<12} {'TOP STRATEGY'}")
    print(f"  {'─'*80}")
    for r in rows:
        score_str = f"{r['firing_count']}/{r['qualified_total']}"
        print(f"  {r['scan_date']:<12} {r['bias']:<10} {score_str:>7} "
              f"{r['score_pct']:>4.0f}% {r['score_label']:<12} {r.get('top_strategy','')}")
    print()


# ══════════════════════════════════════════════════════════════════
# DEEP DIVE — calls query_agent for full output
# ══════════════════════════════════════════════════════════════════

def deep_dive(ticker: str, market: str, bias: Optional[str],
              timeframe: str, capital: float, max_risk: float,
              db_path: str) -> None:
    if not BUILDER_AVAILABLE:
        print(f"  Cannot import kanida_agent_builder — run query manually:")
        print(f"  python kanida_agent_builder.py --query {ticker} --market {market}")
        return
    query_agent(
        ticker=ticker, market=market,
        bias=bias, timeframe=timeframe,
        capital=capital, max_risk=max_risk,
        db_path=db_path,
    )


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description="KANIDA Fast Batch Scanner — DB-backed, no fingerprint recompute",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Actions
    ap.add_argument("--market",      default="NSE", choices=["NSE","US","ALL"])
    ap.add_argument("--bias",        default="bullish",
                    choices=["bullish","bearish","neutral"])
    ap.add_argument("--tickers",     default=None,
                    help="Comma-separated custom tickers")
    ap.add_argument("--timeframe",   default="1D",
                    choices=["30m","1H","4H","1D","1W","1M"])
    ap.add_argument("--min-score",   type=float, default=0,
                    help="Only show stocks with score%% >= this")
    ap.add_argument("--save",        default=None,
                    help="Save results to CSV")
    ap.add_argument("--deep",        default=None,
                    help="Deep dive full query after scan")
    ap.add_argument("--history",     default=None,
                    help="Show scan history for a stock")
    ap.add_argument("--ready",       action="store_true",
                    help="Show which stocks are built and ready")
    ap.add_argument("--db-summary",  action="store_true")

    # Options
    ap.add_argument("--capital",     type=float, default=500_000)
    ap.add_argument("--max-risk",    type=float, default=2.0)
    ap.add_argument("--delay",       type=float, default=0.3,
                    help="Seconds between API calls (default: 0.3)")
    ap.add_argument("--db",          default=DB_PATH)

    args = ap.parse_args()

    if not AGENT_AVAILABLE or not DB_AVAILABLE:
        print("❌  Missing dependencies. Check custom_agent.py and kanida_db.py are present.")
        sys.exit(1)

    init_db(args.db)

    # ── DB summary
    if args.db_summary:
        print_db_summary(args.db)
        return

    # ── Ready stocks
    if args.ready:
        show_ready(args.market, args.db)
        return

    # ── Scan history
    if args.history:
        show_history(args.history.upper(), args.market, args.bias, args.db)
        return

    # ── Build ticker list
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        market  = args.market if args.market != "ALL" else "NSE"
    else:
        if args.market.upper() == "NSE":
            universe = NSE_FNO
        elif args.market.upper() == "US":
            universe = US_OPTIONS
        else:
            universe = NSE_FNO + US_OPTIONS
        market = args.market if args.market != "ALL" else "NSE"

        # Only scan stocks that have been built
        conn  = get_conn(args.db)
        built = set(
            r["ticker"] for r in conn.execute(
                "SELECT ticker FROM agents WHERE status='ready' AND market=?",
                (market.upper(),)
            ).fetchall()
        )
        conn.close()
        tickers = [t for t in universe if t in built]

        if not tickers:
            print(f"\n  No built agents found for {market.upper()}.")
            print(f"  Build some first:")
            print(f"  python kanida_agent_builder.py --build HDFCBANK,RELIANCE --market {market.upper()}")
            print(f"  python kanida_agent_builder.py --build-universe {market.upper()}")
            return

    results = run_batch(
        tickers=tickers, market=market,
        bias=args.bias, timeframe=args.timeframe,
        capital=args.capital, max_risk=args.max_risk,
        delay=args.delay, db_path=args.db,
    )

    signals = print_ranked_table(results, args.bias, args.min_score)
    print_conviction_summary(signals, args.bias, market)

    if args.save:
        save_csv(results, args.save)

    if args.deep:
        deep_dive(
            ticker=args.deep.upper(), market=market,
            bias=args.bias, timeframe=args.timeframe,
            capital=args.capital, max_risk=args.max_risk,
            db_path=args.db,
        )


if __name__ == "__main__":
    main()
