"""Adjustment-basis mixing: detect it from the store, never from a list.

Why this module exists
----------------------
The repair pass (``market_data/repair/``) re-fetched only *disputed windows*.
Every re-fetched row carries the vendor's current basis id
(``kite-eod-adjusted``); every row it did not touch keeps the id the legacy seed
gave it (``legacy_unknown``).  So after the repair almost every symbol carries
both labels -- and for most of them that is harmless, because the vendor's
values agreed with the legacy ones bar for bar.

A label difference is therefore **not** evidence of a problem.  The thing that
breaks a return is a *level* difference: if the two labels sit on different
corporate-action adjustment bases, a return computed across the boundary picks
up the ratio between them as a fake move.

So this module never trusts a label, and never trusts a hand-written list of
symbols.  It measures:

* for each ``bar_start`` we hold on **both** labels (the legacy revision and a
  later re-fetched revision), the ratio ``legacy_close / vendor_close``;
* if that ratio is ~1 for effectively every overlapping bar, the two labels are
  the same basis and the symbol is fine however the labels are spread;
* if it is a *constant* ratio away from 1 across effectively every overlapping
  bar, the legacy rows are on another basis, and every bar still resolving to
  a legacy row is a landmine.

``VERDICT_MIXED`` is the only verdict that calls for a full-history re-fetch.

Nothing here writes; it is safe against a store opened read-only and against
the live loop.
"""

from __future__ import annotations

import statistics
from dataclasses import asdict, dataclass, field
from typing import Iterable, Sequence

#: The id the legacy seed stamped on rows whose adjustment basis nobody
#: recorded.  It is a statement of ignorance, not of a basis.
LEGACY_BASIS = "legacy_unknown"

#: Ratio distance from 1.0 at which two prices are a different *level* rather
#: than float noise.  0.1% is an order of magnitude above tick rounding on any
#: NSE price we hold and an order of magnitude below the smallest real
#: corporate-action ratio in the repair evidence (2.39%).
DEFAULT_TOLERANCE = 0.001

#: Share of the overlapping bars that must be shifted before we call it a basis
#: difference rather than a handful of bad prints.  The repair's own
#: ``source_error`` rows are isolated bars; a basis shift is the whole series.
DEFAULT_MIN_SHIFTED_SHARE = 0.5

#: How far a shifted ratio may sit from the median and still count as part of
#: the *same* constant level shift (the reconcile step used the same idea per
#: session).
DEFAULT_MAX_RATIO_SPREAD = 0.01

#: Share of the shifted ratios that must fall inside that band.  A raw min/max
#: spread is not usable here: ZEEL's 5005 shifted bars are all within 0.006% of
#: 1.023892 except for a single bad print in 2019 at 1.156, and a min/max rule
#: lets that one bar veto a repair the other 5004 bars prove is needed.
DEFAULT_MIN_RATIO_CONCENTRATION = 0.99

VERDICT_UNIFIED = "unified"                    # one basis at the latest revision
VERDICT_MIXED = "mixed"                        # two bases, measured level shift
VERDICT_LABEL_ONLY = "label_mixed_same_level"  # two labels, same prices
VERDICT_LEGACY_ONLY = "legacy_only"            # nothing re-fetched; basis unknown
VERDICT_INCONCLUSIVE = "inconclusive"          # shifted bars, but not one ratio


@dataclass(frozen=True)
class BasisEvidence:
    """What the store itself says about one symbol's adjustment basis."""

    symbol: str
    verdict: str
    rows_by_basis: dict = field(default_factory=dict)   # latest revision only
    legacy_rows: int = 0
    vendor_rows: int = 0
    legacy_first: str | None = None
    legacy_last: str | None = None
    overlap_bars: int = 0
    shifted_bars: int = 0
    shifted_share: float | None = None
    median_ratio: float | None = None
    min_ratio: float | None = None
    max_ratio: float | None = None
    ratio_spread: float | None = None
    ratio_concentration: float | None = None
    off_ratio_bars: int = 0
    reason: str = ""

    @property
    def mixed(self) -> bool:
        return self.verdict == VERDICT_MIXED

    def to_dict(self) -> dict:
        return asdict(self)


def latest_revision_bases(store, symbol: str) -> dict:
    """``{adjustment_basis_id: {rows, first_bar, last_bar}}`` at latest-wins.

    Old revisions are deliberately ignored: they are history, not what a reader
    of the store gets back.
    """
    rows = store.con.execute(
        "SELECT adjustment_basis_id AS basis, COUNT(*) AS rows, "
        "       MIN(bar_start) AS first_bar, MAX(bar_start) AS last_bar FROM ("
        "  SELECT adjustment_basis_id, bar_start,"
        "         ROW_NUMBER() OVER (PARTITION BY instrument_id, bar_start"
        "                            ORDER BY revision DESC) AS rn"
        "  FROM candles_15m WHERE symbol=?"
        ") WHERE rn=1 GROUP BY basis", (symbol,)).fetchall()
    return {r["basis"]: {"rows": int(r["rows"]), "first_bar": r["first_bar"],
                         "last_bar": r["last_bar"]} for r in rows}


def overlap_ratios(store, symbol: str, *, legacy_basis: str = LEGACY_BASIS,
                   vendor_basis: str | None = None) -> list[tuple[str, float]]:
    """``(bar_start, legacy_close / vendor_close)`` for bars held on both bases.

    Only pairs where the vendor row is a *later* revision of the same bar are
    used -- that is the pair the repair actually created, and it is the only
    pair where "same bar, two bases" is unambiguous.
    """
    sql = ("SELECT a.bar_start, a.close, b.close FROM candles_15m a "
           "JOIN candles_15m b ON a.instrument_id=b.instrument_id "
           " AND a.bar_start=b.bar_start AND b.revision>a.revision "
           "WHERE a.symbol=? AND a.adjustment_basis_id=? "
           "  AND b.adjustment_basis_id<>a.adjustment_basis_id "
           "  AND a.close>0 AND b.close>0")
    params: list = [symbol, legacy_basis]
    if vendor_basis is not None:
        sql += " AND b.adjustment_basis_id=?"
        params.append(vendor_basis)
    return [(r[0], r[1] / r[2]) for r in store.con.execute(sql, params)]


def classify(symbol: str, bases: dict, ratios: Sequence[tuple[str, float]], *,
             legacy_basis: str = LEGACY_BASIS,
             tolerance: float = DEFAULT_TOLERANCE,
             min_shifted_share: float = DEFAULT_MIN_SHIFTED_SHARE,
             max_ratio_spread: float = DEFAULT_MAX_RATIO_SPREAD,
             min_ratio_concentration: float = DEFAULT_MIN_RATIO_CONCENTRATION
             ) -> BasisEvidence:
    """Pure classifier -- the part that is worth a unit test."""
    legacy_rows = bases.get(legacy_basis, {}).get("rows", 0)
    vendor_rows = sum(v["rows"] for k, v in bases.items() if k != legacy_basis)
    legacy_first = bases.get(legacy_basis, {}).get("first_bar")
    legacy_last = bases.get(legacy_basis, {}).get("last_bar")
    values = [r for _, r in ratios]
    shifted = [r for r in values if abs(r - 1.0) > tolerance]
    common = dict(
        symbol=symbol, rows_by_basis=bases, legacy_rows=legacy_rows,
        vendor_rows=vendor_rows, legacy_first=legacy_first, legacy_last=legacy_last,
        overlap_bars=len(values), shifted_bars=len(shifted),
        shifted_share=(len(shifted) / len(values)) if values else None,
        median_ratio=statistics.median(values) if values else None,
    )
    if shifted:
        centre = statistics.median(shifted)
        inside = sum(abs(r / centre - 1.0) <= max_ratio_spread for r in shifted)
        common["min_ratio"] = min(shifted)
        common["max_ratio"] = max(shifted)
        common["ratio_spread"] = (max(shifted) - min(shifted)) / centre
        common["ratio_concentration"] = inside / len(shifted)
        common["off_ratio_bars"] = len(shifted) - inside

    if legacy_rows == 0:
        return BasisEvidence(verdict=VERDICT_UNIFIED, reason=(
            "every bar resolves to the vendor's basis at the latest revision"),
            **common)
    if vendor_rows == 0:
        return BasisEvidence(verdict=VERDICT_LEGACY_ONLY, reason=(
            "the whole series is still on the legacy basis; nothing has been "
            "re-fetched, so the basis is unknown but at least consistent"),
            **common)
    if not values:
        return BasisEvidence(verdict=VERDICT_INCONCLUSIVE, reason=(
            "both bases are present but no bar is held on both, so the level "
            "relationship between them cannot be measured from the store"),
            **common)
    share = len(shifted) / len(values)
    if share < min_shifted_share:
        return BasisEvidence(verdict=VERDICT_LABEL_ONLY, reason=(
            f"{len(shifted)}/{len(values)} overlapping bars differ in level "
            f"({share:.2%}); the labels differ but the prices do not, so this is "
            f"re-fetch bookkeeping, not two bases"), **common)
    concentration = common.get("ratio_concentration") or 0.0
    if concentration < min_ratio_concentration:
        return BasisEvidence(verdict=VERDICT_INCONCLUSIVE, reason=(
            f"{share:.2%} of overlapping bars differ but by no single ratio: only "
            f"{concentration:.2%} of them sit within {max_ratio_spread:.2%} of the "
            f"median {statistics.median(shifted):.6f}. That is not a constant level "
            f"shift and a re-basing would be a guess"), **common)
    return BasisEvidence(verdict=VERDICT_MIXED, reason=(
        f"{len(shifted)}/{len(values)} overlapping bars ({share:.2%}) differ by a "
        f"constant ratio of {statistics.median(shifted):.6f} "
        f"({concentration:.2%} of them within {max_ratio_spread:.2%} of it, "
        f"{common['off_ratio_bars']} off it); {legacy_rows} bars still resolve to "
        f"the legacy basis over {legacy_first} .. {legacy_last}, so a return "
        f"computed across that boundary is wrong"), **common)


def evidence_for(store, symbol: str, **kw) -> BasisEvidence:
    """Measure one symbol."""
    bases = latest_revision_bases(store, symbol)
    ratios = overlap_ratios(store, symbol,
                            legacy_basis=kw.get("legacy_basis", LEGACY_BASIS))
    return classify(symbol, bases, ratios, **kw)


def scan(store, symbols: Iterable[str] | None = None, *,
         progress=None, **kw) -> list[BasisEvidence]:
    """Measure every symbol (or the given ones).  One pass, read-only."""
    names = list(symbols) if symbols is not None else [
        r[0] for r in store.con.execute(
            "SELECT DISTINCT symbol FROM candles_15m ORDER BY symbol")]
    out: list[BasisEvidence] = []
    for i, name in enumerate(names, 1):
        out.append(evidence_for(store, name, **kw))
        if progress:
            progress(i, len(names), out[-1])
    return out


def mixed_symbols(store, symbols: Iterable[str] | None = None, **kw) -> list[BasisEvidence]:
    """Only the symbols that genuinely need a full-history re-fetch."""
    return [e for e in scan(store, symbols, **kw) if e.mixed]
