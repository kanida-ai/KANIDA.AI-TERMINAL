"""Data layer.

Canonical format everywhere in this engine is a LONG panel:
    date (datetime64), symbol (str), open, high, low, close, volume, sector (str)

Sorted by (symbol, date). One row = one symbol-day = one observation.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

REQUIRED = ["date", "symbol", "open", "high", "low", "close", "volume"]

# strength of the conditional edge planted into the synthetic market (see below)
BOOST = 0.008


def classify_symbols(df: pd.DataFrame, zero_vol_share: float = 0.9) -> pd.Series:
    """Split the panel into tradeable stocks and non-tradeable indices.

    An index reports no volume. A stock that reports no volume on 90% of days is
    not something you can trade either, so the same test covers both.

    This matters in two directions. Indices must be OUT of the tradeable universe
    -- you cannot buy NIFTY GS 10YR. But they must stay IN as context: the market
    return, sector returns and INDIA VIX are exactly what the relative-strength
    and breadth features need. Treating them as stocks also poisons the internal
    equal-weight market index, because bond indices barely move and would damp it.
    """
    g = df.groupby("symbol")
    zero_share = g["volume"].apply(lambda v: float((v.fillna(0) <= 0).mean()))
    kind = pd.Series(np.where(zero_share >= zero_vol_share, "index", "stock"),
                     index=zero_share.index, name="kind")
    return kind


def split_universe(df: pd.DataFrame, verbose: bool = True):
    """Returns (stocks, indices)."""
    kind = classify_symbols(df)
    stocks = df[df["symbol"].map(kind) == "stock"].copy()
    indices = df[df["symbol"].map(kind) == "index"].copy()
    if verbose:
        print(f"  universe split    : {kind.value_counts().get('stock', 0)} stocks, "
              f"{kind.value_counts().get('index', 0)} indices")
        if len(indices):
            names = sorted(indices["symbol"].unique())
            print(f"    indices excluded from trading: {', '.join(names[:6])}"
                  f"{'...' if len(names) > 6 else ''}")
    return stocks, indices


def diagnose(df: pd.DataFrame, verbose: bool = True) -> dict:
    """Data-quality report. Run this whenever you add symbols.

    Bad bars concentrated in a few symbols mean a broken feed for those names:
    drop the symbol. Bad bars spread thinly across many symbols mean occasional
    vendor glitches: drop the rows. The distinction matters, so it is reported
    rather than silently patched.
    """
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"])
    p = d[["open", "high", "low", "close"]]

    flags = {
        "high_lt_low": d["high"] < d["low"],
        "close_outside": (d["close"] > d["high"]) | (d["close"] < d["low"]),
        "open_outside": (d["open"] > d["high"]) | (d["open"] < d["low"]),
        "nonpositive": (p <= 0).any(axis=1),
        "null_price": p.isna().any(axis=1),
        "zero_volume": d["volume"] <= 0,
        "flat_bar": (d["high"] == d["low"]) & (d["volume"] > 0),
    }
    bad = flags["high_lt_low"] | flags["close_outside"] | flags["open_outside"] \
        | flags["nonpositive"] | flags["null_price"]

    g = d.groupby("symbol")
    ret = g["close"].pct_change().abs()
    flags["huge_move"] = ret > 0.35          # candidate unadjusted split/bonus
    dup = d.duplicated(subset=["symbol", "date"], keep=False)

    res = {"n_rows": len(d), "n_symbols": d["symbol"].nunique(),
           "n_bad": int(bad.sum()), "n_duplicates": int(dup.sum()),
           "flags": {k: int(v.sum()) for k, v in flags.items()},
           "bad_mask": bad}

    if verbose:
        print(f"\n{'=' * 72}\nDATA QUALITY   {len(d):,} rows, "
              f"{d['symbol'].nunique()} symbols\n{'=' * 72}")
        for k, v in res["flags"].items():
            mark = "  " if v == 0 else "!!"
            print(f"  {mark} {k:<16} {v:>7,}")
        print(f"  {'  ' if res['n_duplicates'] == 0 else '!!'} "
              f"{'duplicate rows':<16} {res['n_duplicates']:>7,}")

        if bad.any():
            per = d.loc[bad].groupby("symbol").size().sort_values(ascending=False)
            tot = g.size()
            share = (per / tot.reindex(per.index)).round(3)
            print(f"\n  {int(bad.sum())} malformed bars across "
                  f"{len(per)} symbols. Worst offenders:")
            print(f"    {'symbol':<14}{'bad':>6}{'of':>8}{'share':>8}")
            for sym in per.head(12).index:
                print(f"    {sym:<14}{per[sym]:>6}{int(tot[sym]):>8}{share[sym]:>8.1%}")
            heavy = share[share > 0.02]
            if len(heavy):
                print(f"\n  {len(heavy)} symbol(s) have >2% bad bars: "
                      f"{', '.join(heavy.index[:10])}")
                print("  That is a broken feed, not a glitch. Drop these symbols")
                print("  and re-fetch them rather than patching the rows.")
            else:
                print("\n  Spread thinly: vendor glitches. Safe to drop the rows")
                print("  with --drop-bad-bars.")
            print("\n  examples:")
            cols = ["date", "symbol", "open", "high", "low", "close", "volume"]
            print(d.loc[bad, cols].head(6).to_string(index=False))

        kind = classify_symbols(d)
        n_idx = int((kind == "index").sum())
        if n_idx:
            print(f"\n  {n_idx} of {d['symbol'].nunique()} symbols report no volume:")
            print("  these are INDICES, not stocks. They must be excluded from the")
            print("  tradeable universe but kept as market context. Use")
            print("  --exclude-indices (on by default in the loader).")

        if flags["huge_move"].sum():
            hm = d.loc[flags["huge_move"]].groupby("symbol").size().sort_values(
                ascending=False)
            stock_hm = [s for s in hm.index if kind.get(s) == "stock"]
            if stock_hm:
                print(f"\n  CORPORATE ACTIONS TO CHECK (stocks only): "
                      f"{', '.join(stock_hm[:10])}")
                print("  A >35% single-day move in a stock is usually an unadjusted")
                print("  split, bonus or demerger. Verify each one; the feature")
                print("  factory cannot tell it apart from a real crash.")
            print(f"\n  {int(flags['huge_move'].sum())} single-day moves over 35% "
                  f"in {len(hm)} symbols:")
            print(f"    {', '.join(f'{s}({n})' for s, n in hm.head(8).items())}")
            print("  Check these are real. An unadjusted split looks exactly like")
            print("  this, and the feature factory will happily mine it.")
    return res


def clean(df: pd.DataFrame, drop_symbols_above: float = 0.02,
          verbose: bool = True) -> pd.DataFrame:
    """Drop malformed bars; drop entire symbols whose feed is broken."""
    rep = diagnose(df, verbose=False)
    bad = rep["bad_mask"]
    d = df.copy()
    if bad.any():
        per = d.loc[bad].groupby("symbol").size()
        tot = d.groupby("symbol").size()
        share = per / tot.reindex(per.index)
        drop_syms = set(share[share > drop_symbols_above].index)
        if drop_syms:
            if verbose:
                print(f"  dropping {len(drop_syms)} symbols with a broken feed: "
                      f"{', '.join(sorted(drop_syms)[:8])}"
                      f"{'...' if len(drop_syms) > 8 else ''}")
            d = d[~d["symbol"].isin(drop_syms)]
            bad = bad.reindex(d.index).fillna(False)
        n = int(bad.sum())
        if n:
            if verbose:
                print(f"  dropping {n} malformed bars")
            d = d[~bad]
    d = d.drop_duplicates(subset=["symbol", "date"], keep="last")
    return d.sort_values(["symbol", "date"]).reset_index(drop=True)


def _validate(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"panel is missing columns: {missing}")
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    if "sector" not in df.columns:
        df["sector"] = "UNKNOWN"
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    bad = (df["high"] < df["low"]) | (df["close"] <= 0) | (df["open"] <= 0)
    if bad.any():
        raise ValueError(f"{int(bad.sum())} malformed bars (high<low or non-positive price)")
    return df


def load_csv(path: str) -> pd.DataFrame:
    """Load a long panel CSV. Columns: date,symbol,open,high,low,close,volume[,sector]"""
    return _validate(pd.read_csv(path))


def load_yfinance(symbols, start: str, end: str, sectors: dict | None = None) -> pd.DataFrame:
    """Optional convenience loader. Requires `pip install yfinance`.

    For NSE symbols use the .NS suffix, e.g. ['RELIANCE.NS','ONGC.NS','^NSEI'].
    """
    import yfinance as yf

    frames = []
    raw = yf.download(list(symbols), start=start, end=end, auto_adjust=False,
                      group_by="ticker", progress=False)
    for sym in symbols:
        try:
            sub = raw[sym].dropna().reset_index()
        except KeyError:
            continue
        sub.columns = [str(c).lower() for c in sub.columns]
        sub = sub.rename(columns={"adj close": "adj_close"})
        sub["symbol"] = sym
        sub["sector"] = (sectors or {}).get(sym, "UNKNOWN")
        frames.append(sub[["date", "symbol", "open", "high", "low", "close", "volume", "sector"]])
    if not frames:
        raise RuntimeError("yfinance returned nothing usable")
    return _validate(pd.concat(frames, ignore_index=True))


def make_synthetic_panel(n_symbols: int = 40, n_days: int = 1400, seed: int = 7,
                         n_sectors: int = 5) -> pd.DataFrame:
    """Generate a synthetic but *structured* market so the engine has something to find.

    Deliberately embeds three effects the engine should be able to rediscover:
      1. a market factor and sector factors (creates a real correlation graph)
      2. a lead-lag: within each sector, symbol #0 leads the others by one day
      3. a conditional edge: high relative volume + strong close location -> mild
         next-day drift. This is the "state" the miner ought to surface.

    If the engine cannot find (3) on synthetic data, it is broken. If it finds
    lots of "edges" on shuffled data, it is overfitting. Both are testable.
    """
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2020-01-01", periods=n_days)
    sectors = [f"SEC{i}" for i in range(n_sectors)]
    syms = [f"SYM{i:03d}" for i in range(n_symbols)]
    sym_sector = {s: sectors[i % n_sectors] for i, s in enumerate(syms)}
    leaders = {sec: [s for s in syms if sym_sector[s] == sec][0] for sec in sectors}

    market = rng.normal(0, 0.011, n_days)
    sector_f = {sec: rng.normal(0, 0.010, n_days) for sec in sectors}

    rows = []
    for i, sym in enumerate(syms):
        sec = sym_sector[sym]
        beta_m = rng.uniform(0.6, 1.4)
        beta_s = rng.uniform(0.4, 1.2)
        idio = rng.normal(0, 0.016, n_days)
        ret = beta_m * market + beta_s * sector_f[sec] + idio

        # lead-lag: followers inherit part of the leader's previous-day move
        if sym != leaders[sec]:
            lead_ret = beta_s * sector_f[sec]
            ret[1:] += 0.25 * lead_ret[:-1]

        close = 100.0 * np.exp(np.cumsum(ret))
        base_vol = rng.uniform(4e5, 4e6)
        volume = base_vol * np.exp(rng.normal(0, 0.45, n_days) + 3.0 * np.abs(ret))

        # open follows the PREVIOUS close plus a gap; the day's move happens
        # open -> close, which is what the engine actually trades
        prev_close = np.concatenate([[100.0], close[:-1]])
        gap = 0.35 * ret + rng.normal(0, 0.004, n_days)
        op = prev_close * np.exp(gap)
        # intraday excursion beyond the open/close body
        wick = np.abs(rng.normal(0, 0.009, n_days)) + 0.002
        wick_up = wick * rng.uniform(0.3, 1.0, n_days)
        wick_dn = wick * rng.uniform(0.3, 1.0, n_days)
        hi = np.maximum(op, close) * (1 + wick_up)
        lo = np.minimum(op, close) * (1 - wick_dn)

        rows.append(pd.DataFrame({
            "date": dates, "symbol": sym, "open": op, "high": hi,
            "low": lo, "close": close, "volume": volume, "sector": sec,
            "_wick_up": wick_up, "_wick_dn": wick_dn,
        }))

    df = pd.concat(rows, ignore_index=True).sort_values(["symbol", "date"]).reset_index(drop=True)

    # ---- inject the conditional edge (effect 3) ----
    #
    # The trigger is scale-invariant (a volume RATIO and a within-bar location),
    # so it survives the price rescaling applied below.
    #
    # The boost must land on the OPEN -> CLOSE move of day t+1, not on the whole
    # bar. If you scale the open and the high by the same factor, an entry taken
    # at the open sees nothing: the edge cancels out of the label. That is the
    # difference between a tradable drift and an untradable overnight gap.
    g = df.groupby("symbol", sort=False)
    relvol = df["volume"] / g["volume"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    clv = (df["close"] - df["low"]) / (df["high"] - df["low"]).replace(0, np.nan)
    trigger = ((relvol > 1.4) & (clv > 0.75)).fillna(False)

    # b[t] = drift added to day t's open->close move, fired by a trigger on t-1
    b = trigger.groupby(df["symbol"], sort=False).shift(1).fillna(False).astype(float) * BOOST

    # cumulative level multiplier, so the boost persists instead of reversing
    # the following day (a reversal would be a synthetic artefact, not an edge)
    cum = (1.0 + b).groupby(df["symbol"], sort=False).cumprod()
    cum_prev = cum.groupby(df["symbol"], sort=False).shift(1).fillna(1.0)

    df["open"] = df["open"] * cum_prev      # open carries yesterday's level only
    df["close"] = df["close"] * cum         # close carries today's boost
    body_hi = df[["open", "close"]].max(axis=1)
    body_lo = df[["open", "close"]].min(axis=1)
    df["high"] = body_hi * (1.0 + df["_wick_up"])
    df["low"] = body_lo * (1.0 - df["_wick_dn"])
    df = df.drop(columns=["_wick_up", "_wick_dn"])

    return _validate(df)


def build_index(df: pd.DataFrame) -> pd.DataFrame:
    """Equal-weight synthetic index + sector indices, computed cross-sectionally per date.

    Uses only same-day data, so it is causal at day T close.
    """
    d = df.copy()
    d["ret1"] = d.groupby("symbol", sort=False)["close"].pct_change(fill_method=None)
    mkt = d.groupby("date", observed=True)["ret1"].mean().rename("mkt_ret")
    sec = d.groupby(["date", "sector"], observed=True)["ret1"].mean().rename("sec_ret")
    d = d.merge(mkt, on="date", how="left").merge(sec, on=["date", "sector"], how="left")
    return d
