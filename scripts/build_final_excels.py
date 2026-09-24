"""Assemble the two FINAL deliverable workbooks:
   1) Intraday_Trailing_Agent.xlsx   2) Dynamic_Agent.xlsx
Each: Strategy_Details, AutoTrade_Build_Prompt, Performance_Summary, Monthly_View, Yearly_View.
Reads verified numbers from the existing source workbooks (gross of costs, in-sample)."""
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
TRAIL_SRC = OUT / "Top5_Trailing_Stop.xlsx"
AGENT_SRC = OUT / "Intraday_Dynamic_Agent_Unfiltered.xlsx"


def norm_monthly_trail(df):
    return pd.DataFrame({
        "Year": df["year"], "Month": df["month"], "Days": df["trading_days"],
        "Win": df["winning_days"], "Loss": df["losing_days"], "Win%": df["win_rate_%"],
        "Avg_win%": df["avg_ret_win_%"], "Avg_loss%": df["avg_ret_loss_%"],
        "Sum_month%": df["sum_return_%"]})


def norm_monthly_agent(df):
    return pd.DataFrame({
        "Year": df["year"], "Month": df["month"], "Days": df["Days"], "Win": df["Win"],
        "Loss": df["Loss"], "Win%": df["Win%"], "Avg_win%": df["Avg_win"],
        "Avg_loss%": df["Avg_loss"], "Sum_month%": df["Sum_month"]})


def yearly_from_monthly(m):
    rows = []
    for y, g in m.groupby("Year"):
        win = g["Win"].sum(); loss = g["Loss"].sum(); days = g["Days"].sum()
        aw = (g["Avg_win%"] * g["Win"]).sum() / win if win else None
        al = (g["Avg_loss%"] * g["Loss"]).sum() / loss if loss else None
        sigma = g["Sum_month%"].sum()
        rows.append({"Year": int(y), "Days": int(days), "Win": int(win), "Loss": int(loss),
                     "Win%": round(win / days * 100, 1), "Avg_win%": round(aw, 2),
                     "Avg_loss%": round(al, 2), "Avg_day%": round(sigma / days, 3),
                     "Sum%": round(sigma, 1)})
    win = m["Win"].sum(); loss = m["Loss"].sum(); days = m["Days"].sum(); sigma = m["Sum_month%"].sum()
    rows.append({"Year": "ALL", "Days": int(days), "Win": int(win), "Loss": int(loss),
                 "Win%": round(win / days * 100, 1),
                 "Avg_win%": round((m["Avg_win%"] * m["Win"]).sum() / win, 2),
                 "Avg_loss%": round((m["Avg_loss%"] * m["Loss"]).sum() / loss, 2),
                 "Avg_day%": round(sigma / days, 3), "Sum%": round(sigma, 1)})
    return pd.DataFrame(rows)


def perf_summary(yearly, extra):
    allrow = yearly[yearly["Year"] == "ALL"].iloc[0]
    rows = [
        ("Backtest window", "2024-05-14 to 2026-06-15 (in-sample)"),
        ("Cost basis", "GROSS of brokerage/STT (deduct ~0.12% per leg for net)"),
        ("Total trading days", int(allrow["Days"])),
        ("Winning days", int(allrow["Win"])),
        ("Losing days", int(allrow["Loss"])),
        ("Win rate", f"{allrow['Win%']}%"),
        ("Avg return on WIN days", f"+{allrow['Avg_win%']}%"),
        ("Avg return on LOSS days", f"{allrow['Avg_loss%']}%"),
        ("Average return / day", f"+{allrow['Avg_day%']}%"),
        ("Total return (sum of daily, additive)", f"+{allrow['Sum%']}%"),
    ]
    for y in (2024, 2025, 2026):
        r = yearly[yearly["Year"] == y]
        if not r.empty:
            r = r.iloc[0]
            rows.append((f"{y}  (win% / avg-day / Sum)",
                         f"{r['Win%']}%  /  +{r['Avg_day%']}%/day  /  +{r['Sum%']}%"))
    rows += list(extra.items())
    return pd.DataFrame(rows, columns=["Metric", "Value"])


def text_sheet(lines, col="Detail"):
    return pd.DataFrame({col: lines})


# ----- TRAILING strategy content -----
TRAIL_DETAILS = [
    ("Strategy name", "Intraday Trailing Agent (Falcon Top-5)"),
    ("Type", "Same-day intraday, no overnight"),
    ("Universe", "Falcon engine Top-5 ranked Nifty-500 stocks (EOD signal, traded next day)"),
    ("Entry", "09:15 market open; equal capital split across the 5 names"),
    ("Profit engine", "PORTFOLIO lock +1%: once the combined basket reaches +1%, arm and trail"),
    ("Trailing", "Exit ALL when basket falls 0.75% below its intraday peak (floor = +1%)"),
    ("Hard stop", "-1.5% portfolio stop (exit all)"),
    ("Time exit", "Flat at 15:29 close if neither trail nor stop fires"),
    ("Capital basis", "Rs 5,00,000 (parameterised; scales linearly within capacity)"),
    ("Trail giveback options", "0.30 / 0.50 / 0.75 / 1.00% (0.75% is the recommended default)"),
    ("Capacity note", "Unfiltered universe ~Rs 30 lakh; liquid >=Rs100Cr + 15-min stagger ~Rs 2 Cr"),
    ("Status", "OOS-validated (train 2024-25 -> test 2026 holds). Deployable. Forward paper-trade next."),
]
TRAIL_PROMPT = """AUTOTRADE BUILD PROMPT — Intraday Trailing Agent (MTF-aware, parameterised)

Add a new INTRADAY BASKET strategy to the Falcon auto-trade system. Paper-mode
default, additive, behind a flag - do not touch the existing execution path.
Every value below is an operator setting (changeable per day), validated defaults shown.

WHAT IT DOES, each day:
 1. At entry_time (09:15) buy the Top N (5) picks, equal money split, product (MTF/CNC/MIS).
 2. Watch the WHOLE basket's P&L % live on ticks.
 3. STOP: if basket <= -1.5%, exit all.
 4. ARM + TRAIL: once basket hits +1%, lock that as a floor and ride it; exit all
    when it falls 0.75% below its peak (never below +1%).
 5. Square off everything by 15:29. Never overnight.

MTF / LEVERAGE:
 - Size each leg from the broker's per-stock margin (kite.order_margins), not a fixed multiple.
 - ALL % triggers (+1% / 0.75% / -1.5%) are measured on the NOTIONAL position value
   (qty x price) - i.e. the basket's price move - NOT on own funds.
 - Report both: notional return % and return-on-own-funds % (notional x leverage).

SETTINGS (validated defaults): basket_size=5, entry_time=09:15, product=MTF,
 arm_pct=+1.0, floor_pct=+1.0, trail_giveback_pct=0.75, stop_pct=-1.5, square_off_time=15:29.
 Store in a per-session config row; validate on save (floor<=arm, stop<0, 0<giveback).

SAFETY: route every order through the existing preflight + kill switch; persist
 {armed, peak, deployed, params} so a mid-day restart recovers; per-stock catastrophic
 backup stop if the monitor stalls; show session state on /power/autotrade.
 Ship paper-mode tests: trail exit, floor exit, stop, square-off, restart-recovery.

VALIDATED RESULT (gross, in-sample 2024-05..2026-06): ~75.8% green days, +1.01%/day,
 +516% sum; worst day -4.68%. At 2x MTF ~2%/day on own funds (downside scales; MTF interest applies)."""

# ----- DYNAMIC AGENT content -----
AGENT_DETAILS = [
    ("Strategy name", "Dynamic Intraday Agent (walk-forward trailing)"),
    ("Type", "Same-day intraday, no overnight; runs ALONGSIDE Falcon (separate system)"),
    ("Universe", "Falcon Top-10 pool, K=5 active slots (liquidity-filter optional)"),
    ("Profit engine", "PORTFOLIO lock +1% then trail (same proven engine as the Trailing Agent)"),
    ("WHAT'S DYNAMIC", "Trail giveback is SELECTED by walk-forward: monthly re-tune over the "
                       "trailing 120 days, choosing the best of {0.50, 0.75, 1.00%} for the regime"),
    ("Hard stop", "-1.5% portfolio stop; plus -2% per-stock breakdown cut"),
    ("Rotation", "Available but its net contribution is marginal; the main edge is the "
                 "auto-tuned trail value. OFF by default (enable only if re-validated)."),
    ("Learning loop", "Walk-forward trail-value selection (monthly re-tune over trailing 120d) "
                      "+ setup-outcome ledger. No lookahead; Day-N uses only pre-Day-N data."),
    ("Time exit", "Flat at 15:29; never overnight"),
    ("Key result", "Beats the fixed-0.75 Trailing Agent by tuning the trail per regime "
                   "(unfiltered: ~24.6%/mo in 2025 vs ~22%); edge is exit-tuning, not rotation"),
    ("Status", "Walk-forward OOS holds (2026 test +1.09%/day). In-sample; forward paper-trade next."),
]
AGENT_PROMPT = """AUTOTRADE BUILD PROMPT — Dynamic Intraday Agent (self-tuning trailing)

Add a new INTRADAY BASKET strategy to the Falcon auto-trade system. Paper-mode
default, additive, behind a flag - do not touch the existing execution path. This is
the Trailing Agent PLUS a self-tuning layer; it is FULLY specified below so it can be
built standalone. Every value is an operator setting; validated defaults shown.

WHAT IT DOES, each day:
 1. At entry_time (09:15) buy the Top N (5) names from the Falcon Top-10 pool, equal
    money split, product (MTF/CNC/MIS).
 2. Watch the WHOLE basket's P&L % live on ticks (portfolio level).
 3. STOP: if basket <= -1.5%, exit all.
 4. ARM + TRAIL: once basket hits +1%, lock that as a floor and ride it; exit all when
    it falls TRAIL% below its peak (never below +1%).  [TRAIL% is auto-tuned - see below]
 5. PER-STOCK CUT: if any single name falls <= -2% intraday, cut just that name (its
    slot goes to cash). Do NOT cut on normal early dips - only a real breakdown.
 6. Square off everything by 15:29. Never overnight.

THE SELF-TUNING LAYER (this is what makes it "dynamic"):
 A. TRAIL% is NOT fixed. Each day the agent uses one trail value from {0.50, 0.75, 1.00%}.
    It re-selects once a month (every ~21 trading days):
      - For each candidate trail value, compute its AVERAGE DAILY RETURN over the
        trailing 120 trading days (strictly PAST data - walk-forward, no lookahead).
      - Pick the trail value with the best trailing-window average; lock it for the next
        ~21 days. Log the chosen value and the date it was set.
 B. SETUP LEDGER (optional): tag each entry with a simple setup archetype; if an
    archetype's win rate over its last >=20 trades drops below 45%, stop taking it.
 C. ROTATION (optional, OFF by default): refill a cut/empty slot with a fresh "calm"
    name from the Top-10 pool before a cutoff time. Enable only if your own walk-forward
    test shows it helps - it was only marginal in backtest.

MTF / LEVERAGE (same as the Trailing Agent):
 - Size each leg from the broker's per-stock margin (kite.order_margins), not a fixed multiple.
 - ALL % triggers (+1% arm / TRAIL% / -1.5% stop / -2% cut) are on the NOTIONAL position
   value (qty x price) - the price move - NOT on own funds.
 - Report both notional return % and return-on-own-funds % (notional x leverage).

SETTINGS (validated defaults):
   basket_size=5, pool=Falcon Top-10, entry_time=09:15, product=MTF,
   arm_pct=+1.0, floor_pct=+1.0, trail_choices={0.50,0.75,1.00}, retune_every=21d,
   lookback=120d, per_stock_cut_pct=-2.0, stop_pct=-1.5, square_off_time=15:29,
   rotation=OFF, ledger=ON.

SAFETY: route every order through the existing preflight + kill switch; persist
 {selected_trail, last_retune_date, armed, peak, deployed, open_legs, params} so a
 mid-day restart recovers; per-stock catastrophic backup stop if the monitor stalls;
 show session state + the currently-selected trail value on /power/autotrade.
 Paper-mode tests: trail exit, floor exit, stop, per-stock cut, square-off, the monthly
 re-tune selecting only from past data, and restart-recovery.

VALIDATED RESULT (gross, unfiltered universe, in-sample 2024-05..2026-06): ~74.7% green
 days, +1.08%/day, +550% sum (vs the fixed-0.75 Trailing Agent +516%); 2025 ~+295% (~24.6%/mo).
 Walk-forward OOS (test 2026): +1.09%/day. Apply a >=Rs100Cr liquidity filter on the pool
 for ~Rs2Cr capacity at ~17.8%/mo (lower return, far higher scale)."""


def build(path, monthly, yearly, details, prompt, extra, daily_csv, exploded_csv):
    perf = perf_summary(yearly, extra)
    jdaily = pd.read_csv(daily_csv)
    jex = pd.read_csv(exploded_csv)
    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        pd.DataFrame(details, columns=["Item", "Detail"]).to_excel(xl, "Strategy_Details", index=False)
        text_sheet(prompt.split("\n"), "AutoTrade_Build_Prompt").to_excel(
            xl, "AutoTrade_Build_Prompt", index=False)
        perf.to_excel(xl, "Performance_Summary", index=False)
        monthly.to_excel(xl, "Monthly_View", index=False)
        yearly.to_excel(xl, "Yearly_View", index=False)
        jdaily.to_excel(xl, "Daily_Trade_Log", index=False)
        jex.to_excel(xl, "Trade_Journal_Exploded", index=False)
    import shutil
    if DESK.exists():
        shutil.copy(path, DESK / path.name)
    print(f"[*] built {path.name}  ({len(jdaily)} days, {len(jex)} legs in journal)")


import argparse
_ap = argparse.ArgumentParser()
_ap.add_argument("--suffix", default="")        # "" = originals; "_Reconciled" = clean-window
_ap.add_argument("--agent-src", default=str(AGENT_SRC))
_args = _ap.parse_args()
_sfx = _args.suffix
_agent_src = Path(_args.agent_src)

# TRAILING
tm = norm_monthly_trail(pd.read_excel(TRAIL_SRC, "M_Trail_giveback_0.75pc"))
ty = yearly_from_monthly(tm)
build(OUT / f"Intraday_Trailing_Agent{_sfx}.xlsx", tm, ty, TRAIL_DETAILS, TRAIL_PROMPT,
      {"Recommended trail": "0.75% giveback",
       "Best-return trail": "1.00% giveback (+1.13%/day gross)"},
      OUT / "Trailing_TradeLog_Daily.csv", OUT / "Trailing_TradeLog_Exploded.csv")

# DYNAMIC
am = norm_monthly_agent(pd.read_excel(_agent_src, "4_Agent_Monthly_GROSS"))
ay = yearly_from_monthly(am)
build(OUT / f"Dynamic_Agent{_sfx}.xlsx", am, ay, AGENT_DETAILS, AGENT_PROMPT,
      {"Universe shown": "Unfiltered (max return). Liquid >=Rs100Cr version ~17.8%/mo at ~Rs2Cr capacity",
       "vs Trailing Agent": "+34pp total return, ~same win rate"},
      OUT / "Agent_TradeLog_Daily_Unfiltered.csv", OUT / "Agent_TradeLog_Exploded_Unfiltered.csv")

print("[*] done")
