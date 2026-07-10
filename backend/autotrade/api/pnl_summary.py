"""AutoTrade P&L aggregation — the Performance dashboard's pure service layer.

Builds realised-P&L rollups from autotrade_sessions + autotrade_positions ONLY
(never falcon_position_state). Groups CLOSED trades by STRATEGY (the primary
grouping), attributes SEGMENT (Equity/Futures/Options) + PRODUCT (CNC/MIS/MTF/
NRML), and buckets by the position's closed_at (IST) into a requested period.

DATA-ISOLATION: read-only. Reuses the existing per-position charge math from
autotrade.charges.estimate_charges (GROSS realised_pnl − estimated charges = NET)
— it does NOT reimplement it. All P&L is GROSS in the DB (autotrade_positions.
realised_pnl); NET = GROSS − charges is computed here for display, additively.

TESTABILITY: build_pnl_summary()/collect_trades()/iter_csv_rows() are pure — they
take a live sqlite3 connection + explicit scoping/period params (and an injectable
now_ist for determinism), so they unit-test without any HTTP layer.

STRATEGY TAXONOMY (derived per session — config_json + ladder_id):
  ladder_id IS NOT NULL                                   → "ladder"     Auto-Ladder (Monthly)
  strategy=="portfolio_kill_switch"                       → "killswitch" Kill-Switch Basket
  strategy=="intraday_basket" AND square_off_enabled      → "intraday"   Intraday Basket
  strategy=="intraday_basket" AND NOT square_off_enabled  → "positional" Positional Basket
(ladder_id takes precedence over strategy.)
"""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from ..charges import estimate_charges

log = logging.getLogger("kanida.autotrade.pnl")
IST = timezone(timedelta(hours=5, minutes=30))


# ── Constants ─────────────────────────────────────────────────────────────────

_STRATEGY_NAMES = {
    "intraday":   "Intraday Basket",
    "positional": "Positional Basket",
    "ladder":     "Auto-Ladder (Monthly)",
    "killswitch": "Kill-Switch Basket",
}

_SEGMENT_LABELS = {
    "EQ":  "Equity",
    "FUT": "Futures",
    "CE":  "Options",
    "PE":  "Options",
}

_VALID_PERIODS = {"yesterday", "1w", "2w", "mtd", "ytd", "custom"}


# ── Strategy / segment / product derivation ───────────────────────────────────

def derive_strategy(cfg: Dict[str, Any],
                    ladder_id: Optional[str]) -> Tuple[str, str]:
    """Return (strategy_id, strategy_name) for one session.

    ladder_id takes precedence over the config strategy. An unrecognised/absent
    strategy on a NON-ladder session defaults to the kill-switch bucket (a legacy
    session with no strategy in its config loads as portfolio_kill_switch — see
    config.from_dict), matching the engine's own default.
    """
    if ladder_id is not None and str(ladder_id).strip() != "":
        return "ladder", _STRATEGY_NAMES["ladder"]
    strategy = str((cfg or {}).get("strategy") or "portfolio_kill_switch")
    if strategy == "portfolio_kill_switch":
        return "killswitch", _STRATEGY_NAMES["killswitch"]
    if strategy == "intraday_basket":
        square_off = bool((cfg or {}).get("square_off_enabled", True))
        if square_off:
            return "intraday", _STRATEGY_NAMES["intraday"]
        return "positional", _STRATEGY_NAMES["positional"]
    # Unknown strategy → kill-switch bucket (defensive; never drop a trade).
    return "killswitch", _STRATEGY_NAMES["killswitch"]


def _segment_label(instrument_type: Optional[str]) -> str:
    return _SEGMENT_LABELS.get(str(instrument_type or "EQ").upper(), "Equity")


def _product_label(cfg: Dict[str, Any]) -> str:
    p = str((cfg or {}).get("order_product") or "CNC").upper()
    return p if p in ("CNC", "MIS", "NRML", "MTF") else "CNC"


# ── Period window resolution (IST) ────────────────────────────────────────────

def resolve_window(period: str,
                   from_date: Optional[str],
                   to_date: Optional[str],
                   now_ist: Optional[datetime] = None) -> Tuple[str, str]:
    """Resolve (from_iso, to_iso) inclusive DATE bounds (YYYY-MM-DD) for a period.

    All bounds are IST dates. closed_at is stored as an IST ISO string, so we
    bucket on its date prefix (substr 1..10) — no tz-parse pitfalls.

    * yesterday → the previous NSE trading day (single day)
    * 1w        → last 7 calendar days .. today
    * 2w        → last 14 calendar days .. today
    * mtd       → 1st of this month .. today
    * ytd       → Jan 1 .. today
    * custom    → the given from/to (defaults to today for a missing bound)
    """
    now = now_ist or datetime.now(IST)
    today = now.date()

    if period == "custom":
        f = (from_date or today.isoformat())[:10]
        t = (to_date or today.isoformat())[:10]
        if f > t:
            f, t = t, f
        return f, t

    if period == "yesterday":
        # Previous NSE trading day (strictly before today). Falls back to the
        # weekday-before if the calendar can't confirm — never raises.
        try:
            from ..trading_calendar import previous_trading_day
            prev = previous_trading_day(today, inclusive=False)
        except Exception:  # pragma: no cover - calendar import/coverage guard
            prev = today - timedelta(days=1)
        iso = prev.isoformat()
        return iso, iso

    if period == "1w":
        return (today - timedelta(days=7)).isoformat(), today.isoformat()
    if period == "2w":
        return (today - timedelta(days=14)).isoformat(), today.isoformat()
    if period == "mtd":
        return today.replace(day=1).isoformat(), today.isoformat()
    if period == "ytd":
        return today.replace(month=1, day=1).isoformat(), today.isoformat()

    # Unknown period → treat as last 7 days (never raise).
    return (today - timedelta(days=7)).isoformat(), today.isoformat()


def _date_label(iso_str: Optional[str]) -> str:
    """'2026-06-24T15:29:00+05:30' → 'Jun 24'. Empty on unparseable input."""
    if not iso_str:
        return ""
    try:
        d = datetime.strptime(str(iso_str)[:10], "%Y-%m-%d").date()
        return d.strftime("%b %d")
    except (ValueError, TypeError):
        return ""


# ── DB reads (scoped) ─────────────────────────────────────────────────────────

def _load_scoped_sessions(con: sqlite3.Connection,
                          *,
                          viewer_user_id: Optional[str],
                          is_admin: bool,
                          filter_user_id: Optional[str],
                          mode: str) -> Dict[str, Dict[str, Any]]:
    """Return {session_id: session-meta} for every session visible to the caller.

    Scoping:
      * admin              → all sessions; if filter_user_id given, only that user
      * non-admin          → strictly WHERE user_id = viewer_user_id (NULL-owned /
                             operator sessions are invisible; no cross-user probe)
    mode: 'live' | 'paper' | 'all' (default live) filters autotrade_sessions.mode.
    """
    con.row_factory = sqlite3.Row
    where: List[str] = []
    args: List[Any] = []

    if is_admin:
        if filter_user_id is not None and str(filter_user_id).strip() != "":
            where.append("user_id = ?")
            args.append(str(filter_user_id))
    else:
        # Non-admin: locked to own id. A NULL viewer id can own nothing.
        if viewer_user_id is None:
            return {}
        where.append("user_id = ?")
        args.append(str(viewer_user_id))

    m = str(mode or "live").lower()
    if m in ("live", "paper"):
        where.append("mode = ?")
        args.append(m)
    # 'all' → no mode filter.

    sql = "SELECT session_id, config_json, ladder_id, invested_basis, " \
          "total_allocated_capital, mode, started_at, created_at, user_id " \
          "FROM autotrade_sessions"
    if where:
        sql += " WHERE " + " AND ".join(where)

    out: Dict[str, Dict[str, Any]] = {}
    for row in con.execute(sql, args).fetchall():
        try:
            cfg = json.loads(row["config_json"]) if row["config_json"] else {}
            if not isinstance(cfg, dict):
                cfg = {}
        except (json.JSONDecodeError, TypeError):
            cfg = {}
        strat_id, strat_name = derive_strategy(cfg, row["ladder_id"])
        out[row["session_id"]] = {
            "session_id":     row["session_id"],
            "ladder_id":      row["ladder_id"],
            "strategy_id":    strat_id,
            "strategy_name":  strat_name,
            "product":        _product_label(cfg),
            "invested_basis": float(row["invested_basis"] or 0.0),
            "total_allocated_capital": float(row["total_allocated_capital"] or 0.0),
            "mode":           row["mode"],
            "date_label":     _date_label(row["started_at"] or row["created_at"]),
            "date_key":       str(row["started_at"] or row["created_at"] or "")[:10],
        }
    return out


def _load_closed_trades(con: sqlite3.Connection,
                        session_ids: Iterable[str],
                        from_iso: str,
                        to_iso: str) -> List[sqlite3.Row]:
    """Closed positions across the given sessions whose closed_at DATE falls in
    [from_iso, to_iso] inclusive. Realised P&L only (status CLOSED)."""
    ids = list(session_ids)
    if not ids:
        return []
    con.row_factory = sqlite3.Row
    rows: List[sqlite3.Row] = []
    # Chunk the IN() list so a very large session set can't overflow SQLite's
    # variable limit.
    CHUNK = 400
    for i in range(0, len(ids), CHUNK):
        chunk = ids[i:i + CHUNK]
        placeholders = ",".join("?" for _ in chunk)
        sql = (
            f"SELECT session_id, symbol, instrument_type, direction, qty, "
            f"avg_price, exit_price, realised_pnl, close_reason, opened_at, "
            f"closed_at, entry_order_id, exit_order_id "
            f"FROM autotrade_positions "
            f"WHERE session_id IN ({placeholders}) "
            f"AND status = 'CLOSED' AND closed_at IS NOT NULL "
            # A REAL trade only. Excludes qty=0 stale-cleanups
            # (STALE_CLEANUP_BROKER_FLAT, realised NULL) and entry-reject phantoms
            # (never held: qty 0, or exit_price NULL AND realised 0). A genuine trade
            # has qty>0, a computed realised_pnl, and EITHER a real exit price OR
            # non-zero P&L (some real closes record realised_pnl with a NULL
            # exit_price field — those are kept). So phantoms never inflate the trade
            # count / drag win rate, but no real trade is dropped.
            f"AND qty > 0 AND realised_pnl IS NOT NULL "
            f"AND (exit_price > 0 OR realised_pnl != 0) "
            f"AND substr(closed_at, 1, 10) >= ? AND substr(closed_at, 1, 10) <= ?"
        )
        rows.extend(con.execute(sql, (*chunk, from_iso, to_iso)).fetchall())
    return rows


def _load_ladder_capitals(con: sqlite3.Connection,
                          ladder_ids: Iterable[str]) -> Dict[str, float]:
    """{ladder_id: total_capital} from autotrade_ladders. Missing ids omitted —
    the caller falls back to summing that campaign's children."""
    ids = [x for x in ladder_ids if x]
    if not ids:
        return {}
    con.row_factory = sqlite3.Row
    out: Dict[str, float] = {}
    placeholders = ",".join("?" for _ in ids)
    try:
        rows = con.execute(
            f"SELECT ladder_id, total_capital FROM autotrade_ladders "
            f"WHERE ladder_id IN ({placeholders})", ids).fetchall()
        for r in rows:
            out[r["ladder_id"]] = float(r["total_capital"] or 0.0)
    except sqlite3.Error as e:  # table absent / query issue → empty (fallback path)
        log.warning("_load_ladder_capitals: %s", e)
    return out


# ── Core: enrich every closed trade with strategy/segment/product/net ─────────

def collect_trades(con: sqlite3.Connection,
                   *,
                   viewer_user_id: Optional[str],
                   is_admin: bool,
                   period: str,
                   from_date: Optional[str] = None,
                   to_date: Optional[str] = None,
                   mode: str = "live",
                   filter_user_id: Optional[str] = None,
                   now_ist: Optional[datetime] = None
                   ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]], str, str]:
    """Return (trades, sessions_meta, from_iso, to_iso).

    `trades` is a list of enriched dicts (one per CLOSED position in-window),
    each carrying its strategy/segment/product/gross/charges/net. Never raises on
    a bad row — it is skipped + logged. `sessions_meta` is the scoped session map.
    """
    from_iso, to_iso = resolve_window(period, from_date, to_date, now_ist)
    sessions = _load_scoped_sessions(
        con, viewer_user_id=viewer_user_id, is_admin=is_admin,
        filter_user_id=filter_user_id, mode=mode)

    trades: List[Dict[str, Any]] = []
    if not sessions:
        return trades, sessions, from_iso, to_iso

    for row in _load_closed_trades(con, sessions.keys(), from_iso, to_iso):
        try:
            sid = row["session_id"]
            meta = sessions.get(sid)
            if meta is None:
                continue
            qty = int(row["qty"] or 0)
            avg_price = float(row["avg_price"] or 0.0)
            exit_price = (float(row["exit_price"])
                          if row["exit_price"] is not None else None)
            gross = (float(row["realised_pnl"])
                     if row["realised_pnl"] is not None else 0.0)

            product = meta["product"]
            instrument = str(row["instrument_type"] or "EQ").upper()
            # Lifecycle#10 — a reconcile-flat / unknown-price close booked the exit
            # at the ENTRY mark (no real fill: RECONCILED_FLAT / CLOSED_EXTERNAL_FLAT
            # / a stale cleanup), so it never actually traded at a known price. It
            # carries NO real brokerage — charging it (charges on avg==exit) would
            # show a phantom charge-only "loss". Skip its charges AND flag it so the
            # win/loss stats exclude it.
            _cr = str(row["close_reason"] or "").upper()
            reconcile_flat = (
                "RECONCILED_FLAT" in _cr or "EXTERNAL_FLAT" in _cr
                or "STALE" in _cr
                or (exit_price is not None and avg_price > 0
                    and abs(exit_price - avg_price) < 1e-9
                    and abs(gross) < 1e-9))
            charges_total = 0.0
            if (not reconcile_flat and exit_price is not None
                    and avg_price > 0 and qty > 0):
                # DIRECTION-AWARE legs (real-money charge accuracy): STT (MIS
                # sell-side / F&O sell-side) and stamp (buy-side) are leg-specific.
                # For a LONG the entry (avg_price) is the BUY and the exit is the
                # SELL; for a SHORT it inverts — entry is the SELL-to-open, exit is
                # the BUY-to-cover. Passing them swapped would charge STT/stamp on
                # the wrong leg's value. buy_value = the BUY leg, sell_value = SELL.
                _open_val = qty * avg_price
                _close_val = qty * exit_price
                _is_short = str(row["direction"] or "long").lower() == "short"
                _buy_value = _close_val if _is_short else _open_val
                _sell_value = _open_val if _is_short else _close_val
                ch = estimate_charges(
                    product=product,
                    buy_value=_buy_value,
                    sell_value=_sell_value,
                    legs=2,
                    instrument_type=instrument)
                charges_total = float(ch.get("total", 0.0))
            net = round(gross - charges_total, 2)

            is_campaign = bool(meta["ladder_id"])
            trades.append({
                "strategy_id":    meta["strategy_id"],
                "strategy_name":  meta["strategy_name"],
                "session_id":     sid,
                "ladder_id":      meta["ladder_id"],
                # drill-down grouping key: campaign groups children by ladder_id
                "drill_key":      meta["ladder_id"] if is_campaign else sid,
                "kind":           "campaign" if is_campaign else "session",
                "date_label":     meta["date_label"],
                "date_key":       meta["date_key"],
                "symbol":         row["symbol"],
                "segment":        _segment_label(row["instrument_type"]),
                "product":        product,
                "side":           str(row["direction"] or "long"),
                "qty":            qty,
                "entry_price":    avg_price,
                "exit_price":     exit_price,
                "opened_at":      row["opened_at"],
                "closed_at":      row["closed_at"],
                "gross":          round(gross, 2),
                "charges":        round(charges_total, 2),
                "net":            net,
                "close_reason":   row["close_reason"],
                "entry_order_id": row["entry_order_id"],
                "exit_order_id":  row["exit_order_id"],
                "invested_basis": meta["invested_basis"],
                # Lifecycle#10: excluded from charge-bearing win/loss stats.
                "reconcile_flat": reconcile_flat,
            })
        except Exception as e:  # never let one bad row break the read path
            log.warning("collect_trades: skipping bad row (%s): %s",
                        type(e).__name__, e)
            continue

    return trades, sessions, from_iso, to_iso


# ── Aggregation helpers ───────────────────────────────────────────────────────

def _win_rate(wins: int, trades: int) -> float:
    return round(wins / trades * 100.0, 2) if trades else 0.0


def _dominant(labels: List[str]) -> Optional[str]:
    if not labels:
        return None
    counts: Dict[str, int] = {}
    for x in labels:
        counts[x] = counts.get(x, 0) + 1
    # dominant = highest count, tie-broken by first appearance order
    best = None
    best_n = -1
    seen_order = list(dict.fromkeys(labels))
    for x in seen_order:
        if counts[x] > best_n:
            best, best_n = x, counts[x]
    return best


def _bucket_stats(trade_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    gross = round(sum(t["gross"] for t in trade_list), 2)
    charges = round(sum(t["charges"] for t in trade_list), 2)
    net = round(sum(t["net"] for t in trade_list), 2)
    n = len(trade_list)
    # Lifecycle#10 — win/loss/win_rate are computed over the CHARGE-BEARING trades
    # only. Reconcile-flat / unknown-price closes (exit booked at the entry mark,
    # no real fill) carry no real P&L or charges; counting them would show phantom
    # charge-only losses. gross/charges/net totals are unchanged (they are ~0 for
    # those rows anyway). When there are none, this is byte-identical (n_real==n).
    real = [t for t in trade_list if not t.get("reconcile_flat")]
    n_real = len(real)
    wins = sum(1 for t in real if t["net"] > 0)
    losses = n_real - wins
    return {
        "net":      net,
        "gross":    gross,
        "charges":  charges,
        "trades":   n,
        "wins":     wins,
        "losses":   losses,
        "win_rate": _win_rate(wins, n_real),
    }


def _drill_row(drill_key: str, trade_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    stats = _bucket_stats(trade_list)
    kind = trade_list[0]["kind"]
    products = sorted({t["product"] for t in trade_list})
    segments = sorted({t["segment"] for t in trade_list})
    seg = _dominant([t["segment"] for t in trade_list]) or (segments[0] if segments else "Equity")
    prod = " · ".join(products) if products else ""
    # date_label: for a campaign use the earliest child date; a session already
    # has one shared date. Fall back to the closed_at date.
    date_keys = sorted(t["date_key"] for t in trade_list if t.get("date_key"))
    date_label = trade_list[0]["date_label"]
    if kind == "campaign":
        earliest = next((t for t in sorted(trade_list, key=lambda x: x.get("date_key") or "z")
                         if t.get("date_key")), None)
        if earliest:
            date_label = earliest["date_label"]
    if not date_label:
        date_label = _date_label(trade_list[0]["closed_at"])
    name = (f"Auto-Ladder · {date_label}" if kind == "campaign"
            else f"{date_label} · {prod}" if date_label else prod or drill_key)
    return {
        "id":        drill_key,
        "kind":      kind,
        "name":      name,
        "date_label": date_label,
        "net":       stats["net"],
        "gross":     stats["gross"],
        "charges":   stats["charges"],
        "trades":    stats["trades"],
        "win_rate":  stats["win_rate"],
        "segment":   seg,
        "product":   prod,
    }


def build_pnl_summary(con: sqlite3.Connection,
                      *,
                      viewer_user_id: Optional[str],
                      is_admin: bool,
                      period: str,
                      from_date: Optional[str] = None,
                      to_date: Optional[str] = None,
                      mode: str = "live",
                      filter_user_id: Optional[str] = None,
                      now_ist: Optional[datetime] = None) -> Dict[str, Any]:
    """Build the EXACT summary contract for GET /api/power/autotrade/pnl/summary.

    Empty period → strategies:[] + zeroed totals + capital_deployed 0. Never
    raises on data (a bad row is skipped in collect_trades)."""
    now = now_ist or datetime.now(IST)
    trades, sessions, from_iso, to_iso = collect_trades(
        con, viewer_user_id=viewer_user_id, is_admin=is_admin, period=period,
        from_date=from_date, to_date=to_date, mode=mode,
        filter_user_id=filter_user_id, now_ist=now)

    # ── capital_deployed: count each capital POOL ONCE ────────────────────────
    #   Σ(distinct ladder.total_capital of ladders active in window)
    #  + Σ(total_allocated_capital of distinct STANDALONE non-ladder sessions)
    # A ladder that spawned N children counts its total_capital ONCE (not N×) —
    # the old per-session sum double-counted reused campaign capital. Residual
    # caveat: capital REUSED across separate campaigns/periods still sums (that's
    # defensible as "total allocated"). Contributing = ≥1 closed trade in-window.
    contributing_sids = {t["session_id"] for t in trades
                         if t["session_id"] in sessions}
    standalone_cap = sum(
        sessions[sid]["total_allocated_capital"]
        for sid in contributing_sids if not sessions[sid]["ladder_id"])
    contributing_ladders = {sessions[sid]["ladder_id"]
                            for sid in contributing_sids
                            if sessions[sid]["ladder_id"]}
    ladder_caps = _load_ladder_capitals(con, contributing_ladders)
    ladder_total = 0.0
    for lid in contributing_ladders:
        if lid in ladder_caps:
            ladder_total += ladder_caps[lid]        # campaign pool, counted once
        else:
            # No ladder row (edge): fall back to summing this campaign's
            # contributing children's total_allocated_capital.
            ladder_total += sum(
                sessions[sid]["total_allocated_capital"]
                for sid in contributing_sids
                if sessions[sid]["ladder_id"] == lid)
    capital_deployed = round(standalone_cap + ladder_total, 2)

    # ── Group by strategy_id ───────────────────────────────────────────────────
    by_strategy: Dict[str, List[Dict[str, Any]]] = {}
    for t in trades:
        by_strategy.setdefault(t["strategy_id"], []).append(t)

    # Stable display order for the strategy cards.
    _ORDER = ["intraday", "positional", "ladder", "killswitch"]
    ordered_ids = ([s for s in _ORDER if s in by_strategy]
                   + [s for s in by_strategy if s not in _ORDER])

    strategies: List[Dict[str, Any]] = []
    for sid in ordered_ids:
        tlist = by_strategy[sid]
        stats = _bucket_stats(tlist)
        products = sorted({t["product"] for t in tlist})
        segments = sorted({t["segment"] for t in tlist})
        dom_seg = _dominant([t["segment"] for t in tlist]) or (segments[0] if segments else "Equity")

        # drill-down rows
        drills: Dict[str, List[Dict[str, Any]]] = {}
        for t in tlist:
            drills.setdefault(t["drill_key"], []).append(t)
        drill_rows = [_drill_row(k, v) for k, v in drills.items()]
        drill_rows.sort(key=lambda r: r["net"], reverse=True)

        best_row = max(drill_rows, key=lambda r: r["net"]) if drill_rows else None
        worst_row = min(drill_rows, key=lambda r: r["net"]) if drill_rows else None

        def _bw(r: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
            if r is None:
                return None
            lbl = r["date_label"]
            if r["product"]:
                lbl = f"{lbl} · {r['product']}" if lbl else r["product"]
            return {"pnl": r["net"], "label": lbl}

        strategies.append({
            "id":       sid,
            "name":     _STRATEGY_NAMES.get(sid, tlist[0]["strategy_name"]),
            "segment":  dom_seg,
            "segments": segments,
            "product":  " · ".join(products),
            "products": products,
            "net":      stats["net"],
            "gross":    stats["gross"],
            "charges":  stats["charges"],
            "trades":   stats["trades"],
            "wins":     stats["wins"],
            "losses":   stats["losses"],
            "win_rate": stats["win_rate"],
            "avg":      round(stats["net"] / stats["trades"], 2) if stats["trades"] else 0.0,
            "best":     _bw(best_row),
            "worst":    _bw(worst_row),
            "sessions": drill_rows,
        })

    totals = _bucket_stats(trades)

    return {
        "period":           period,
        "from":             from_iso,
        "to":               to_iso,
        "as_of":            now.isoformat(),
        "capital_deployed": capital_deployed,
        "totals":           {
            "net":      totals["net"],
            "gross":    totals["gross"],
            "charges":  totals["charges"],
            "trades":   totals["trades"],
            "wins":     totals["wins"],
            "losses":   totals["losses"],
            "win_rate": totals["win_rate"],
        },
        "strategies":       strategies,
    }


# ── CSV export ────────────────────────────────────────────────────────────────

_CSV_HEADER = [
    "strategy", "session_or_campaign", "symbol", "segment", "product", "side",
    "qty", "entry_price", "exit_price", "entry_time_ist", "exit_time_ist",
    "gross_pnl", "charges", "net_pnl", "close_reason", "entry_order_id",
    "exit_order_id",
]


def _csv_cell(v: Any) -> str:
    """CSV-escape one cell (RFC-4180). None → empty. Never raises."""
    if v is None:
        return ""
    s = str(v)
    if any(c in s for c in (',', '"', '\n', '\r')):
        return '"' + s.replace('"', '""') + '"'
    return s


def iter_csv_rows(con: sqlite3.Connection,
                  *,
                  viewer_user_id: Optional[str],
                  is_admin: bool,
                  period: str,
                  from_date: Optional[str] = None,
                  to_date: Optional[str] = None,
                  mode: str = "live",
                  filter_user_id: Optional[str] = None,
                  now_ist: Optional[datetime] = None) -> Iterator[str]:
    """Yield CSV lines (with trailing newline) — one row per CLOSED trade in the
    window, header first. Never raises on a bad row (skips + logs)."""
    yield ",".join(_CSV_HEADER) + "\r\n"
    trades, _sessions, _f, _t = collect_trades(
        con, viewer_user_id=viewer_user_id, is_admin=is_admin, period=period,
        from_date=from_date, to_date=to_date, mode=mode,
        filter_user_id=filter_user_id, now_ist=now_ist)
    for t in trades:
        try:
            cells = [
                t["strategy_name"],
                t["ladder_id"] if t["kind"] == "campaign" else t["session_id"],
                t["symbol"],
                t["segment"],
                t["product"],
                t["side"],
                t["qty"],
                t["entry_price"],
                t["exit_price"],
                t["opened_at"],
                t["closed_at"],
                t["gross"],
                t["charges"],
                t["net"],
                t["close_reason"],
                t["entry_order_id"],
                t["exit_order_id"],
            ]
            yield ",".join(_csv_cell(c) for c in cells) + "\r\n"
        except Exception as e:  # pragma: no cover - defensive skip
            log.warning("iter_csv_rows: skipping bad row (%s): %s",
                        type(e).__name__, e)
            continue
