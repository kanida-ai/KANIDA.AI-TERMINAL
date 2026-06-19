"""
Falcon signal-time TIER DERIVATION  —  price-move x volume conditioning.

Reconstruction of the analysis run on 2026-06-18. Read-only; writes NOTHING to
any DB. Derives the tier map empirically:
  - reproduce the operator's 1-D price-bucket table (validation gate)
  - engineer a menu of signal-day VOLUME features and rank which discriminate WR
  - build the price x volume grid (N / WR / avg-ret / stop)
  - walk-forward OOS split (train <= 2025-12-31 / test 2026), N>=100 gate
  - emit the derived tier map + rejected cells

Run:
  "C:\\Users\\SPS\\anaconda3\\python.exe" scripts/tier_price_volume_derivation.py

Data sources (verified via PRAGMA, not assumed):
  RND  db: universe_engine/data/db/kanida_universe.db
           falcon_signal_day_study (outcomes: net_ret_pct, exit_reason)
           falcon_signal_day_context (signal_day price/vol fields, is_extension)
  OHLC db: data/db/kanida_universe.db -> ohlc_daily (symbol,trade_date,o,h,l,c,volume)
"""
import os
import sqlite3
import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RND_DB  = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
OHLC_DB = os.path.join(ROOT, "data", "db", "kanida_universe.db")

OOS_CUTOFF = "2025-12-31"      # train <= cutoff, test after (repo walk_forward_oos.py split)
MIN_IS_N   = 100               # in-sample sample-count gate
MIN_OOS_N  = 30                # out-of-sample sample-count gate

# ----------------------------------------------------------------------------
# 1. BASE COHORT  (study JOIN context, resolved & non-extension rows only)
# ----------------------------------------------------------------------------
def load_cohort():
    con = sqlite3.connect(RND_DB)
    df = pd.read_sql_query(
        """
        SELECT s.signal_date, s.symbol,
               x.signal_day_ret_pct      AS sret,
               x.signal_day_volume       AS vol,
               x.prev_day_volume         AS prev_vol,
               x.avg_20d_volume          AS avg20,
               x.signal_day_vol_ratio    AS rvol20_ctx,
               x.signal_day_close        AS close,
               s.net_ret_pct             AS net_ret,
               s.exit_reason             AS exit_reason
        FROM falcon_signal_day_study   s
        JOIN falcon_signal_day_context x
          ON s.signal_date = x.signal_date AND s.symbol = x.symbol
        WHERE s.net_ret_pct IS NOT NULL
          AND x.is_extension = 0
        """,
        con,
    )
    con.close()
    df["win"]  = (df["net_ret"] > 0).astype(int)
    df["stop"] = (df["exit_reason"] == "INIT_STOP").astype(int)
    return df


# ----------------------------------------------------------------------------
# 2. SIGNAL-DAY VOLUME FEATURES  (computed from ohlc, trade_date <= signal_date)
#    rvol20 (== ctx ratio), rvol50, volz, trend3_20, turn_pct(252d turnover rank)
# ----------------------------------------------------------------------------
def add_volume_features(df):
    syms = tuple(sorted(df["symbol"].unique()))
    con = sqlite3.connect(OHLC_DB)
    q = "SELECT symbol, trade_date, close, volume FROM ohlc_daily WHERE symbol IN (%s)" % \
        ",".join("?" * len(syms))
    ohlc = pd.read_sql_query(q, con, params=syms)
    con.close()
    ohlc = ohlc.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    g = ohlc.groupby("symbol", group_keys=False)

    # rolling stats use only completed/current bars -> signal-time safe (backward window)
    ohlc["avg20"] = g["volume"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    ohlc["avg50"] = g["volume"].transform(lambda s: s.rolling(50, min_periods=20).mean())
    ohlc["std20"] = g["volume"].transform(lambda s: s.rolling(20, min_periods=10).std())
    ohlc["avg3"]  = g["volume"].transform(lambda s: s.rolling(3,  min_periods=2).mean())
    ohlc["turn"]  = ohlc["close"] * ohlc["volume"]
    # percentile rank of today's turnover within trailing 252 sessions (incl. today)
    ohlc["turn_pct"] = g["turn"].transform(
        lambda s: s.rolling(252, min_periods=60)
                   .apply(lambda w: (w <= w[-1]).mean(), raw=True)
    )

    ohlc["rvol20"]    = ohlc["volume"] / ohlc["avg20"]
    ohlc["rvol50"]    = ohlc["volume"] / ohlc["avg50"]
    ohlc["volz"]      = (ohlc["volume"] - ohlc["avg20"]) / ohlc["std20"]
    ohlc["trend3_20"] = ohlc["avg3"] / ohlc["avg20"]

    feats = ohlc[["symbol", "trade_date", "rvol20", "rvol50", "volz",
                  "trend3_20", "turn_pct"]]
    out = df.merge(feats, left_on=["symbol", "signal_date"],
                   right_on=["symbol", "trade_date"], how="left")
    return out.drop(columns=["trade_date"])


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------
def stats(sub):
    n = len(sub)
    if n == 0:
        return (0, float("nan"), float("nan"), float("nan"))
    return (n, 100 * sub["win"].mean(), sub["net_ret"].mean(), 100 * sub["stop"].mean())

PRICE_BUCKETS = [
    ("<=0%",     lambda d: d["sret"] <= 0),
    ("0..+2%",   lambda d: (d["sret"] > 0)  & (d["sret"] <= 2)),
    ("+2..+5%",  lambda d: (d["sret"] > 2)  & (d["sret"] <= 5)),
    ("+5..+7%",  lambda d: (d["sret"] > 5)  & (d["sret"] <= 7)),
    ("+7..+10%", lambda d: (d["sret"] > 7)  & (d["sret"] <= 10)),
    ("+10..+15%",lambda d: (d["sret"] > 10) & (d["sret"] <= 15)),
    (">+15%",    lambda d: d["sret"] > 15),
]


def one_d_table(df):
    print("\n=== (a) 1-D PRICE TABLE (validation gate) ===")
    print(f"{'bucket':<11}{'N':>6}{'WR%':>7}{'ret%':>8}{'stop%':>7}")
    for name, f in PRICE_BUCKETS:
        n, wr, ret, st = stats(df[f(df)])
        print(f"{name:<11}{n:>6}{wr:>7.1f}{ret:>8.2f}{st:>7.1f}")
    # operator's GOLD row is actually sret<=2 (3526 + 2815 = 6341)
    n, wr, ret, st = stats(df[df["sret"] <= 2])
    print(f"{'<=+2% (GOLD)':<11}{n:>6}{wr:>7.1f}{ret:>8.2f}{st:>7.1f}   <- merged flat/modest")


def feature_ranking(df):
    print("\n=== (b) VOLUME-FEATURE DISCRIMINATION (WR spread across own quartiles) ===")
    rows = []
    for feat in ["turn_pct", "trend3_20", "rvol50", "rvol20", "volz"]:
        d = df.dropna(subset=[feat])
        try:
            q = pd.qcut(d[feat], 4, labels=False, duplicates="drop")
        except ValueError:
            continue
        wr = d.groupby(q)["win"].mean() * 100
        rows.append((feat, wr.max() - wr.min(), wr.iloc[0], wr.iloc[-1]))
    rows.sort(key=lambda r: -r[1])
    print(f"{'feature':<12}{'WR spread pp':>13}{'Q1 WR':>8}{'Q4 WR':>8}  direction")
    for feat, spread, q1, q4 in rows:
        direction = "LOW vol -> high WR" if q1 > q4 else "HIGH vol -> high WR"
        print(f"{feat:<12}{spread:>13.1f}{q1:>8.1f}{q4:>8.1f}  {direction}")


def price_vol_grid(df, feat, edges, labels):
    print(f"\n=== (c) PRICE x {feat} grid  (N / WR% / ret% / stop%) ===")
    header = f"{'price\\\\vol':<11}" + "".join(f"{l:>22}" for l in labels)
    print(header)
    for name, pf in PRICE_BUCKETS[:4]:
        cells = []
        for i in range(len(labels)):
            lo, hi = edges[i], edges[i + 1]
            m = pf(df) & (df[feat] >= lo) & (df[feat] < hi)
            n, wr, ret, st = stats(df[m])
            cells.append(f"{n}/{wr:.0f}/{ret:.1f}/{st:.0f}" if n else "-")
        print(f"{name:<11}" + "".join(f"{c:>22}" for c in cells))


# ----------------------------------------------------------------------------
# 4. DERIVED TIER MAP  (IS vs OOS)
# ----------------------------------------------------------------------------
TIERS = [
    ("PREMIUM",       lambda d: (d["sret"] <= 2) & (d["trend3_20"] < 0.9) & (d["volz"] < -0.3)),
    ("ENTERPRISE",    lambda d: (d["sret"] <= 2) & (d["trend3_20"] < 0.9)),
    ("GOLD",          lambda d: (d["sret"] <= 2) & (d["turn_pct"] < 0.75)),
    ("GOLD-baseline", lambda d: (d["sret"] <= 2)),
    ("STANDARD",      lambda d: (d["sret"] > 2) & (d["sret"] <= 5)),
    ("AVOID",         lambda d: (d["sret"] > 10) | ((d["sret"] > 5) & (d["turn_pct"] >= 0.75))),
]


def tier_map(df):
    is_df  = df[df["signal_date"] <= OOS_CUTOFF]
    oos_df = df[df["signal_date"] >  OOS_CUTOFF]
    print(f"\n=== (d) DERIVED TIER MAP   IS(train<= {OOS_CUTOFF})  vs  OOS(2026) ===")
    print(f"{'tier':<15}{'IS_N':>6}{'IS_WR':>7}{'OOS_N':>7}{'OOS_WR':>8}{'OOS_ret':>9}{'OOS_stp':>8}  gate")
    for name, f in TIERS:
        isn, iswr, _, _          = stats(is_df[f(is_df)])
        on, owr, oret, ost       = stats(oos_df[f(oos_df)])
        gate = "OK" if (isn >= MIN_IS_N and on >= MIN_OOS_N) else "THIN"
        print(f"{name:<15}{isn:>6}{iswr:>7.1f}{on:>7}{owr:>8.1f}{oret:>9.2f}{ost:>8.1f}  {gate}")


def rejected_cells(df):
    print("\n=== (e) REJECTED cells (looked good IS, failed OOS-N>=30 or 80% not stable) ===")
    is_df  = df[df["signal_date"] <= OOS_CUTOFF]
    oos_df = df[df["signal_date"] >  OOS_CUTOFF]
    candidates = [
        ("0<sret<=2 & turn_pct<0.25",
         lambda d: (d["sret"] > 0) & (d["sret"] <= 2) & (d["turn_pct"] < 0.25)),
        ("2<sret<=5 & turn_pct<0.25",
         lambda d: (d["sret"] > 2) & (d["sret"] <= 5) & (d["turn_pct"] < 0.25)),
        ("sret<=2 & trend3_20<0.7 (deeper dry-up)",
         lambda d: (d["sret"] <= 2) & (d["trend3_20"] < 0.7)),
    ]
    for name, f in candidates:
        isn, iswr, _, _    = stats(is_df[f(is_df)])
        on, owr, oret, ost = stats(oos_df[f(oos_df)])
        why = "OOS_N<30" if on < MIN_OOS_N else ("IS_N<100" if isn < MIN_IS_N else "no lift / unstable")
        print(f"  {name:<42} IS {isn}@{iswr:.0f}%  OOS {on}@{owr:.0f}%  -> reject ({why})")


def yearly_premium(df):
    print("\n=== PREMIUM cell per-year (shows 80% is a 2023 artifact) ===")
    f = TIERS[0][1]
    sub = df[f(df)].copy()
    sub["yr"] = sub["signal_date"].str[:4]
    for yr, grp in sub.groupby("yr"):
        n, wr, ret, st = stats(grp)
        print(f"  {yr}: N={n:<5} WR={wr:.1f}%  ret={ret:+.2f}%")


def main():
    print("Loading cohort ...")
    df = load_cohort()
    print(f"  cohort rows (resolved, non-extension): {len(df)}")
    print("Engineering signal-day volume features from ohlc_daily ...")
    df = add_volume_features(df)
    # sanity: derived rvol20 should track the context's signal_day_vol_ratio
    chk = df.dropna(subset=["rvol20", "rvol20_ctx"])
    corr = np.corrcoef(chk["rvol20"], chk["rvol20_ctx"])[0, 1]
    print(f"  rvol20 vs context signal_day_vol_ratio corr = {corr:.4f} (expect ~1.0)")

    one_d_table(df)
    feature_ranking(df)
    price_vol_grid(df, "rvol20", [0, 0.7, 1.3, 2.5, np.inf],
                   ["dry<0.7", "norm0.7-1.3", "elev1.3-2.5", "blow>2.5"])
    price_vol_grid(df, "turn_pct", [0, 0.25, 0.5, 0.75, 1.0001],
                   ["low<.25", "mid.25-.5", "hi.5-.75", "top>.75"])
    tier_map(df)
    rejected_cells(df)
    yearly_premium(df)
    print("\nDone. (read-only; no DB writes)")


if __name__ == "__main__":
    main()
