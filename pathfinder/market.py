"""Read-only registered sources and causal daily feature computation."""
from collections import defaultdict
import csv
from datetime import date, datetime, time
import math
from pathlib import Path
import sqlite3
from statistics import mean
from zoneinfo import ZoneInfo

from .models import digest


def load_bars(path, as_of, max_bars=250_000, symbols=None, start="2000-01-01"):
    date.fromisoformat(as_of)
    date.fromisoformat(start)
    path = Path(path).resolve(strict=True)
    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8-sig") as f:
            rows = []
            for row in csv.DictReader(f):
                day = str(row.get("date", row.get("bar_time", "")))[:10]
                if start <= day <= as_of and (not symbols or row["symbol"] in symbols):
                    rows.append(row)
                    if len(rows) > max_bars:
                        raise ValueError("Data budget exceeded; narrow dates or symbols")
    else:
        # Fixed table/columns; never execute SQL supplied by a model.
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as c:
            c.row_factory = sqlite3.Row
            query = "SELECT symbol,bar_time AS date,open,high,low,close,volume FROM ohlc_daily WHERE bar_time >= ? AND bar_time < ?"
            params = [start, as_of + " 23:59:59"]
            if symbols:
                query += " AND symbol IN (" + ",".join("?" for _ in symbols) + ")"
                params.extend(symbols)
            query += " ORDER BY bar_time,symbol LIMIT ?"
            rows = [dict(r) for r in c.execute(query, params + [max_bars + 1])]
    if len(rows) > max_bars:
        raise ValueError("Data budget exceeded; narrow dates or symbols")
    result, seen = [], set()
    for raw in rows:
        day = str(raw.get("date", raw.get("bar_time", "")))[:10]
        date.fromisoformat(day)
        symbol = str(raw["symbol"])
        if not symbol or len(symbol) > 100:
            raise ValueError("Invalid symbol")
        key = (day, symbol)
        if key in seen:
            raise ValueError(f"Duplicate bar: {key}")
        seen.add(key)
        bar = {k: float(raw[k]) for k in ("open", "high", "low", "close", "volume")}
        if any(not math.isfinite(v) for v in bar.values()):
            raise ValueError(f"Non-finite data: {key}")
        if min(bar[k] for k in ("open", "high", "low", "close")) <= 0 or bar["volume"] < 0:
            raise ValueError(f"Invalid price/volume: {key}")
        if not bar["low"] <= min(bar["open"], bar["close"]) <= max(bar["open"], bar["close"]) <= bar["high"]:
            raise ValueError(f"Inconsistent OHLC: {key}")
        result.append(dict(bar, date=day, symbol=symbol))
    return sorted(result, key=lambda r: (r["date"], r["symbol"]))


class Market:
    def __init__(self, bars):
        if not bars:
            raise ValueError("No market data available")
        self.bars = bars
        self.days = sorted({r["date"] for r in bars})
        self.by_day = defaultdict(dict)
        histories = defaultdict(list)
        self.features = defaultdict(dict)
        for b in bars:
            day, symbol = b["date"], b["symbol"]
            self.by_day[day][symbol] = b
            h = histories[symbol]
            if len(h) >= 20:
                up = down = declines = 0
                seq = h + [b]
                for i in range(len(seq)-1, 0, -1):
                    if seq[i]["close"] > seq[i-1]["close"]: up += 1
                    else: break
                for i in range(len(seq)-1, 0, -1):
                    if seq[i]["close"] < seq[i-1]["close"]: down += 1
                    else: break
                for i in range(len(seq)-1, 0, -1):
                    if seq[i]["volume"] < seq[i-1]["volume"]: declines += 1
                    else: break
                self.features[day][symbol] = {
                    "return_1": b["close"]/h[-1]["close"]-1,
                    "return_3": b["close"]/h[-3]["close"]-1,
                    "volume_ratio": b["volume"]/max(1, mean(x["volume"] for x in h[-20:])),
                    "up_streak": up, "down_streak": down,
                    "breakout_20": b["close"]/max(x["high"] for x in h[-20:])-1,
                    "gap": b["open"]/h[-1]["close"]-1,
                    "volume_declines": declines,
                }
            h.append(b)
        for day, rows in self.features.items():
            ranked = sorted(rows, key=lambda s: (-rows[s]["return_1"], s))
            breadth = mean(r["return_1"] > 0 for r in rows.values())
            market_return = mean(r["return_1"] for r in rows.values())
            for i, symbol in enumerate(ranked):
                rows[symbol].update(gainer_rank=(i+1)/len(ranked), breadth=breadth, market_return=market_return)
        self.fingerprints = {d: digest(list(self.by_day[d].values())) for d in self.days}

    def context(self):
        day = self.days[-1]
        rows = list(self.features.get(day, {}).values())
        if not rows:
            raise ValueError("At least 21 bars per symbol needed for context")
        r = rows[0]
        return {"as_of": day, "symbols": len(rows), "breadth": r["breadth"],
                "market_return": r["market_return"],
                "regime": "rising" if r["market_return"] > .003 else "falling" if r["market_return"] < -.003 else "sideways",
                "high_volume_fraction": mean(x["volume_ratio"] >= 1.5 for x in rows),
                "scope": "Equal-weight observed sample, not an official market index"}


def live_clock_check(as_of, latest, max_age):
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    requested = date.fromisoformat(as_of)
    if requested > now.date() or (requested == now.date() and now.time() < time(16, 0)):
        raise ValueError("Daily live cycle requires a completed session (after 16:00 India time)")
    if (now.date() - date.fromisoformat(latest)).days > max_age:
        raise ValueError("Registered data is stale; update the source before live research")
