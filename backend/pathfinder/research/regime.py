"""
Market regime — a port of `Kanida_Falcon/scripts/regime.py` (Carter Ch.5, India edition).

Every internal is causal (rolling windows over the past only), so the state on date t uses
only bars up to t's close. The composite score and its three states are unchanged from the
R&D script:

    score = 25*nifty>200DMA + 10*nifty>50DMA + 0.25*breadth200 + 0.10*breadth50
          + 0.20*(100 - vix_pctile252) + 0.10*(clip(adv_dec10, -50, 50) + 50)
    state = RISK_OFF (<=45) | NEUTRAL (45..65] | RISK_ON (>65)

It is used for PROVENANCE ("regime this was computed in") and as a fact, never as a
forecast.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from .data import MarketData


@dataclass(frozen=True)
class RegimeSnapshot:
    date: str
    state: str                  # RISK_ON | NEUTRAL | RISK_OFF | UNKNOWN
    score: Optional[float]
    nifty_above_200dma: Optional[bool]
    breadth200_pct: Optional[float]
    breadth50_pct: Optional[float]
    vix: Optional[float]
    vix_pctile: Optional[float]
    adv_dec10: Optional[float]
    n_names: int

    @property
    def label(self) -> str:
        """The provenance string. Engine text — may carry digits."""
        if self.score is None:
            return "UNKNOWN (insufficient history for the regime internals)"
        return f"{self.state} (risk score {self.score:.0f}/100, breadth>200DMA {self.breadth200_pct:.0f}%)"


def _vix_pctile(w: np.ndarray) -> float:
    return float((w[-1] >= w).mean() * 100.0)


def build_regime(md: MarketData) -> pd.DataFrame:
    """Date-indexed internals + score + state for every session in the sealed frame."""
    c = md.close_wide()
    ma200 = c.rolling(200).mean()
    ma50 = c.rolling(50).mean()
    valid200 = c.notna() & ma200.notna()
    valid50 = c.notna() & ma50.notna()
    breadth200 = ((c > ma200) & valid200).sum(1) / valid200.sum(1).clip(lower=1) * 100
    breadth50 = ((c > ma50) & valid50).sum(1) / valid50.sum(1).clip(lower=1) * 100
    chg = c.pct_change(fill_method=None)
    adv = (chg > 0).sum(1)
    dec = (chg < 0).sum(1)
    tot = (adv + dec).clip(lower=1)
    adv_dec10 = ((adv - dec) / tot * 100).rolling(10).mean()

    idx = c.index
    nifty = md.index_close.reindex(idx).ffill()
    vix = md.vix_close.reindex(idx).ffill()
    nifty_trend = (nifty > nifty.rolling(200).mean()).astype(float)
    nifty_trend50 = (nifty > nifty.rolling(50).mean()).astype(float)
    vix_pct = vix.rolling(252).apply(_vix_pctile, raw=True)

    df = pd.DataFrame({
        "nifty": nifty, "nifty_trend": nifty_trend, "nifty_trend50": nifty_trend50,
        "breadth200": breadth200, "breadth50": breadth50, "vix": vix, "vix_pct": vix_pct,
        "adv_dec10": adv_dec10, "n_names": c.notna().sum(1),
    })
    df["score"] = (
        25 * df["nifty_trend"] + 10 * df["nifty_trend50"]
        + 0.25 * df["breadth200"] + 0.10 * df["breadth50"]
        + 0.20 * (100 - df["vix_pct"])
        + 0.10 * (df["adv_dec10"].clip(-50, 50) + 50)
    )
    df["state"] = pd.cut(df["score"], [-1, 45, 65, 101], labels=["RISK_OFF", "NEUTRAL", "RISK_ON"]).astype(object)
    # Fail-loud cross-check. The regime label goes on EVERY card, so the breadth on the
    # last session is recomputed by an independent path (the long frame, per symbol) and
    # the scan aborts if the two disagree. Added after one run in three produced a
    # breadth of 10% from a frame that was byte-identical to the runs that produced 56%.
    last = df.index[-1]
    if not pd.isna(df.loc[last, "score"]):
        alt = _breadth200_long(md)
        if alt is not None and abs(alt - float(df.loc[last, "breadth200"])) > 1.0:
            raise RuntimeError(
                f"regime cross-check failed on {last}: wide-frame breadth {df.loc[last, 'breadth200']:.1f}% "
                f"vs long-frame {alt:.1f}%; refusing to publish a regime label")
    return df


def _breadth200_long(md: MarketData) -> Optional[float]:
    """Share of names above their 200-session mean, from the long frame (independent path)."""
    tail = md.df.groupby("symbol", sort=False).tail(200)
    cnt = tail.groupby("symbol")["close"].count()
    mean = tail.groupby("symbol")["close"].mean()
    today = md.today_rows().set_index("symbol")["close"]
    ok = cnt[cnt == 200].index.intersection(today.index)
    if len(ok) == 0:
        return None
    return float((today.loc[ok] > mean.loc[ok]).mean() * 100.0)


def regime_on(reg: pd.DataFrame, d: str) -> RegimeSnapshot:
    if d not in reg.index:
        return RegimeSnapshot(d, "UNKNOWN", None, None, None, None, None, None, None, 0)
    r = reg.loc[d]
    if pd.isna(r["score"]):
        return RegimeSnapshot(d, "UNKNOWN", None, None, None, None, None, None, None, int(r["n_names"]))
    return RegimeSnapshot(
        date=d, state=str(r["state"]), score=float(r["score"]),
        nifty_above_200dma=bool(r["nifty_trend"] > 0),
        breadth200_pct=float(r["breadth200"]), breadth50_pct=float(r["breadth50"]),
        vix=(None if pd.isna(r["vix"]) else float(r["vix"])),
        vix_pctile=(None if pd.isna(r["vix_pct"]) else float(r["vix_pct"])),
        adv_dec10=(None if pd.isna(r["adv_dec10"]) else float(r["adv_dec10"])),
        n_names=int(r["n_names"]),
    )
