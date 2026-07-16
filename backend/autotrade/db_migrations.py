"""AutoTrade DB migrations — idempotent, additive only.

Runs against the Falcon universe DB via falcon.db.falcon_conn (same DB the
existing trade layer uses). Safe to call repeatedly (IF NOT EXISTS + ALTER
guards). Called once at app boot from autotrade.api.autotrade_routes import,
and also exposed for tests / manual invocation.

DATA-ISOLATION (CRITICAL): autotrade session positions live in their OWN table,
`autotrade_positions`, keyed by (session_id, symbol[, broker_profile]). The
autotrade system must NEVER read/write/upsert/lock falcon_position_state for
session positions — that table belongs to the existing Falcon swing system and
its PRIMARY KEY is `symbol`, so a paper session writing into it would overwrite
a real held position. See the regression test in tests/autotrade.

This module ADDS the following without touching any existing table definition:
  * autotrade_positions         — ISOLATED session position store (the fix)
  * falcon_position_state.exit_lock          (INTEGER 0/1)  — legacy, now unused
  * falcon_position_state.exit_initiated_by  (TEXT)         — legacy, now unused
  * falcon_position_state.session_id         (TEXT)         — legacy, now unused
  * falcon_position_state.broker_profile     (TEXT)         — legacy, now unused
  * falcon_position_state.total_allocated_capital (REAL)    — legacy, now unused
  * falcon_position_state.unrealised_pnl     (REAL)         — legacy, now unused
    (These columns are kept as-is — harmless — and are NOT dropped, but the
     autotrade code no longer touches falcon_position_state at all.)
  * autotrade_sessions          — TradingSessionConfig persistence + status
                                  (+ invested_basis: frozen Σ qty*avg_price)
  * autotrade_config_presets    — named saved config presets
  * autotrade_broker_profiles   — BrokerProfile rows (no plaintext secrets)
  * autotrade_slippage          — per-fill slippage record
  * autotrade_portfolio_snapshots — periodic gross_return snapshots
  * autotrade_kill_switch_log   — kill-switch fire audit
  * broker_accounts             — PHASE-2 multi-tenant credential vault
                                  (per user×broker×account; Fernet-encrypted)
  * autotrade_sessions.user_id / .broker_account_id  (TEXT, NULLABLE) — bind a
                                  session to a portal user + vaulted account;
                                  NULL = operator/global creds path (today)
  * autotrade_positions.broker_account_id  (TEXT, NULLABLE) — per-account audit
  * broker_accounts.refresh_token_enc / token_expires_at / last_health_at /
    last_health_status / last_error / redirect_state  — Stage-0 broker-agnostic
    auth foundation (all NULLABLE; Kite path unchanged)
"""
from __future__ import annotations

import logging
from typing import List

from falcon.db import falcon_conn

log = logging.getLogger("kanida.autotrade.migrations")


# Columns we add to the EXISTING falcon_position_state table. (name, ddl_type, default)
_POSITION_STATE_ADD_COLUMNS = [
    ("exit_lock",                "INTEGER", "0"),
    ("exit_initiated_by",        "TEXT",    "NULL"),
    ("session_id",               "TEXT",    "NULL"),
    ("broker_profile",           "TEXT",    "NULL"),
    ("total_allocated_capital",  "REAL",    "NULL"),
    ("unrealised_pnl",           "REAL",    "NULL"),
]


def _existing_columns(con, table: str) -> List[str]:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def _table_exists(con, table: str) -> bool:
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def run_migrations() -> dict:
    """Apply all idempotent migrations. Returns a manifest of what was added."""
    added_cols: List[str] = []
    created_tables: List[str] = []

    with falcon_conn() as con:
        # ── 1. ALTER-guard the two (+helper) columns on falcon_position_state ──
        # Only ALTER if the base table already exists (it is created by the
        # existing Falcon schema extensions). If it does not exist yet we skip
        # silently — the existing schema bootstrap will create it, and a later
        # migration run picks the columns up. We never CREATE it here.
        if _table_exists(con, "falcon_position_state"):
            have = set(_existing_columns(con, "falcon_position_state"))
            for name, ddl_type, default in _POSITION_STATE_ADD_COLUMNS:
                if name not in have:
                    default_clause = "" if default == "NULL" else f" DEFAULT {default}"
                    con.execute(
                        f"ALTER TABLE falcon_position_state "
                        f"ADD COLUMN {name} {ddl_type}{default_clause}"
                    )
                    added_cols.append(name)

        # ── 2. New tables (all IF NOT EXISTS) ────────────────────────────────
        con.executescript(_SCHEMA_SQL)

        # ── 2b. ALTER-guard the GTT-OCO columns on autotrade_positions ───────
        # FEATURE 1: broker-held per-position OCO backup. gtt_id is the Kite GTT
        # trigger id returned by kite.place_gtt; gtt_stop/gtt_target the levels.
        # Additive + idempotent — only added when missing.
        if _table_exists(con, "autotrade_positions"):
            have = set(_existing_columns(con, "autotrade_positions"))
            for name, ddl_type in (("gtt_id", "TEXT"),
                                   ("gtt_stop", "REAL"),
                                   ("gtt_target", "REAL")):
                if name not in have:
                    con.execute(
                        f"ALTER TABLE autotrade_positions ADD COLUMN {name} {ddl_type}")
                    added_cols.append(name)

        # ── 2b-oid. ALTER-guard the ORDER-ID tracking columns on
        # autotrade_positions (RECONCILIATION FRAMEWORK Phase 1). entry_order_id
        # is the broker order-id of the ENTRY fill; exit_order_id the broker
        # order-id of the EXIT fill (or the GTT-fired order). Both NULLABLE — every
        # existing row stays NULL (the reconcilers handle absent ids), so this is
        # byte-for-byte unchanged for legacy rows. These enable PER-SESSION
        # attribution of a broker order (never the account aggregate). Additive +
        # idempotent.
        if _table_exists(con, "autotrade_positions"):
            have = set(_existing_columns(con, "autotrade_positions"))
            for name in ("entry_order_id", "exit_order_id"):
                if name not in have:
                    con.execute(
                        f"ALTER TABLE autotrade_positions ADD COLUMN {name} TEXT")
                    added_cols.append(name)

        # ── 2b-coid. ALTER-guard client_order_id on autotrade_positions ───────
        # SPRINT CLUSTER 2 (CAP 1): the FULL client_order_id minted for the ENTRY
        # placement BEFORE broker submission ("FAL-<sess8>-<sym>-<epochms>-<attempt>").
        # Its compact_tag() rides on the broker order as Kite's `tag`; storing the
        # full id here makes it the DURABLE ownership key that maps to the broker
        # order-id (entry_order_id) and lets kill_switch cancel OUR OWN orders
        # tag-precisely. NULLABLE → every existing / paper / pre-migration row stays
        # NULL (byte-for-byte unchanged). Additive + idempotent.
        if _table_exists(con, "autotrade_positions"):
            have = set(_existing_columns(con, "autotrade_positions"))
            if "client_order_id" not in have:
                con.execute(
                    "ALTER TABLE autotrade_positions ADD COLUMN client_order_id TEXT")
                added_cols.append("client_order_id")

        # ── 2b-prod. ALTER-guard `product` on autotrade_positions ─────────────
        # SPRINT CLUSTER 3 (ITEM 4): the Kite PRODUCT (CNC/MIS/NRML/MTF) this leg
        # was opened under. Persisted PER POSITION so the reconciler buckets a
        # position by ITS OWN product (a session whose broker_profiles mix products
        # — e.g. a CNC leg and an MTF leg — no longer mis-buckets a leg under the
        # single session-level order_product). NULLABLE → every existing / paper /
        # pre-migration row stays NULL and the reconciler falls back to the session
        # config product (byte-for-byte unchanged for those rows). Additive +
        # idempotent.
        if _table_exists(con, "autotrade_positions"):
            have = set(_existing_columns(con, "autotrade_positions"))
            if "product" not in have:
                con.execute(
                    "ALTER TABLE autotrade_positions ADD COLUMN product TEXT")
                added_cols.append("product")

        # ── 2b-xcoid. ALTER-guard `exit_client_order_id` on autotrade_positions ─
        # SPRINT CLUSTER 3 (ITEM 3b): the durable client_order_id minted for a
        # position's EXIT intent, persisted the moment the first exit is placed so
        # the broker tag (compact_tag) is STABLE across a retry / restart. On a
        # RETRY or a RESTART-resume the exit path queries the broker orderbook for
        # this id's tag BEFORE placing — if the tagged order already exists it is
        # ADOPTED (zero new orders), closing the cross-process exactly-once window.
        # NULLABLE → every existing / paper row stays NULL (no query, place as
        # today — byte-for-byte unchanged). Additive + idempotent.
        if _table_exists(con, "autotrade_positions"):
            have = set(_existing_columns(con, "autotrade_positions"))
            if "exit_client_order_id" not in have:
                con.execute(
                    "ALTER TABLE autotrade_positions "
                    "ADD COLUMN exit_client_order_id TEXT")
                added_cols.append("exit_client_order_id")

        # ── 2b-dir. ALTER-guard `direction` on autotrade_positions ────────────
        # FUTURES long/short. Per-position direction so the P&L sign + exit side
        # invert ONLY for shorts. DEFAULT 'long' → every existing row + every
        # equity/long position is byte-for-byte unchanged (sign +1). Additive +
        # idempotent. "long" | "short".
        if _table_exists(con, "autotrade_positions"):
            have = set(_existing_columns(con, "autotrade_positions"))
            if "direction" not in have:
                con.execute(
                    "ALTER TABLE autotrade_positions "
                    "ADD COLUMN direction TEXT NOT NULL DEFAULT 'long'")
                added_cols.append("direction")

        # ── 2b-psl. ALTER-guard PER-STOCK step-lock state on autotrade_positions
        # STEP-LOCK SCOPE == "stock" (intraday_basket): each position ratchets its
        # OWN trail. pos_trail_armed: 0/1 — has this stock's per-slice return
        # crossed its arm threshold. pos_trail_peak: the high-water g_stock
        # (position_uPnL / capital slice). Both NULLABLE / default-0 → every
        # existing row + every basket-scope session is byte-for-byte unchanged
        # (these are read only when step_lock_scope=="stock"). Additive +
        # idempotent — only added when missing.
        if _table_exists(con, "autotrade_positions"):
            have = set(_existing_columns(con, "autotrade_positions"))
            for name, ddl_type, default in (
                    ("pos_trail_armed", "INTEGER", "0"),
                    ("pos_trail_peak", "REAL", "NULL")):
                if name not in have:
                    default_clause = "" if default == "NULL" else f" DEFAULT {default}"
                    con.execute(
                        f"ALTER TABLE autotrade_positions "
                        f"ADD COLUMN {name} {ddl_type}{default_clause}")
                    added_cols.append(name)

        # ── 2b-mark. ALTER-guard ltp_as_of on autotrade_positions (Lifecycle#8) ─
        # The IST timestamp a position's mark (ltp) was last refreshed, so a
        # stalled tick (stale mark) is VISIBLE in status(). NULLABLE → every
        # existing row unchanged until its next refresh_ltps stamps it.
        if _table_exists(con, "autotrade_positions"):
            have = set(_existing_columns(con, "autotrade_positions"))
            if "ltp_as_of" not in have:
                con.execute(
                    "ALTER TABLE autotrade_positions ADD COLUMN ltp_as_of TEXT")
                added_cols.append("ltp_as_of")

        # ── 2c. ALTER-guard invested_basis on autotrade_sessions ─────────────
        # INVESTED-CAPITAL-BASIS feature: the FROZEN sum of qty*avg_price across
        # the session's positions AT ENTRY. This is the product-aware capital
        # actually put to work (MTF = leveraged invested value; CNC = deployed
        # cash) and is the denominator the kill switch / gross return measure
        # against. Captured ONCE in session._fire_entries after orders are
        # placed; never updated as positions close. Additive + idempotent.
        if _table_exists(con, "autotrade_sessions"):
            have = set(_existing_columns(con, "autotrade_sessions"))
            if "invested_basis" not in have:
                con.execute(
                    "ALTER TABLE autotrade_sessions ADD COLUMN invested_basis REAL")
                added_cols.append("invested_basis")

        # ── 2c-ebn. ALTER-guard entry_basket_notional on autotrade_sessions ───
        # PER-STOCK STEP-LOCK Fix A (2026-07-10): the FROZEN entry basket notional
        # = Σ(qty*avg_price) over ALL originally-filled positions. The per-stock
        # capital-slice denominator (session._run_per_stock_step_lock). Frozen ONCE
        # in _fire_entries and NEVER shrunk as names close — the bug was using the
        # live Σ-over-OPEN, which inflated survivors' slices so their per-stock stop
        # fired at a progressively larger rupee loss. NULLABLE → a session created
        # before this column falls back to the live Σ (pre-Fix-A behaviour). Read
        # only in step_lock_scope=="stock". Additive + idempotent.
        if _table_exists(con, "autotrade_sessions"):
            have = set(_existing_columns(con, "autotrade_sessions"))
            if "entry_basket_notional" not in have:
                con.execute(
                    "ALTER TABLE autotrade_sessions "
                    "ADD COLUMN entry_basket_notional REAL")
                added_cols.append("entry_basket_notional")

        # ── 2d. ALTER-guard the INTRADAY-BASKET trail state on autotrade_sessions
        # strategy=="intraday_basket" persists its trailing-profit state here so a
        # resumed RUNNING session restores armed+peak and the trail continues
        # mid-day. trail_armed: 0/1; trail_peak: the high-water gross return G.
        # Additive + idempotent; inert for portfolio_kill_switch sessions.
        if _table_exists(con, "autotrade_sessions"):
            have = set(_existing_columns(con, "autotrade_sessions"))
            for name, ddl_type, default in (
                    ("trail_armed", "INTEGER", "0"),
                    ("trail_peak", "REAL", "NULL")):
                if name not in have:
                    default_clause = "" if default == "NULL" else f" DEFAULT {default}"
                    con.execute(
                        f"ALTER TABLE autotrade_sessions "
                        f"ADD COLUMN {name} {ddl_type}{default_clause}")
                    added_cols.append(name)

        # ── 2e-mt. PHASE-2 MULTI-TENANT scoping columns (additive, NULLABLE) ──
        # user_id + broker_account_id on autotrade_sessions bind a session to a
        # portal user and a specific vaulted broker account. BOTH default NULL →
        # a NULL on either behaves EXACTLY as today (operator/global creds path).
        # Existing sessions (no user_id/broker_account_id) are byte-for-byte
        # unaffected. broker_account_id on autotrade_positions carries the bound
        # account onto each position for per-account audit (also NULLABLE).
        if _table_exists(con, "autotrade_sessions"):
            have = set(_existing_columns(con, "autotrade_sessions"))
            for name in ("user_id", "broker_account_id"):
                if name not in have:
                    con.execute(
                        f"ALTER TABLE autotrade_sessions ADD COLUMN {name} TEXT")
                    added_cols.append(name)
        if _table_exists(con, "autotrade_positions"):
            have = set(_existing_columns(con, "autotrade_positions"))
            if "broker_account_id" not in have:
                con.execute(
                    "ALTER TABLE autotrade_positions ADD COLUMN broker_account_id TEXT")
                added_cols.append("broker_account_id")

        # ── 2e-vault. BROKER-AGNOSTIC AUTH FOUNDATION (Stage 0) ───────────────
        # Additive, NULLABLE columns on broker_accounts for refresh-token brokers
        # + connection health + the OAuth CSRF `state` round-trip. All nullable
        # and default-absent → the existing Kite (request-token, no-refresh) path
        # is byte-for-byte unchanged; these are populated only by brokers that use
        # them. refresh_token_enc is FERNET-ENCRYPTED at rest (same as the other
        # *_enc columns); token_expires_at is an absolute ISO-IST stamp for
        # non-daily brokers; last_health_* record the most recent validate() ping.
        if _table_exists(con, "broker_accounts"):
            have = set(_existing_columns(con, "broker_accounts"))
            for name, ddl_type in (
                    ("refresh_token_enc", "BLOB"),
                    ("token_expires_at", "TEXT"),
                    ("last_health_at", "TEXT"),
                    ("last_health_status", "TEXT"),
                    ("last_error", "TEXT"),
                    ("redirect_state", "TEXT")):
                if name not in have:
                    con.execute(
                        f"ALTER TABLE broker_accounts ADD COLUMN {name} {ddl_type}")
                    added_cols.append(name)

        # ── 2e. ALTER-guard the SPEED-PASS latency observability columns ──────
        # entry_latency_ms: fire start → all legs settled (asyncio.gather done).
        # exit_latency_ms: flatten trigger → all positions flat. Persisted so the
        # operator can SEE the end-to-end speed in status(). Additive + idempotent
        # + inert for any existing session. last_tick_age_ms is computed live in
        # status() (now − newest tick used), not persisted.
        if _table_exists(con, "autotrade_sessions"):
            have = set(_existing_columns(con, "autotrade_sessions"))
            for name in ("entry_latency_ms", "exit_latency_ms"):
                if name not in have:
                    con.execute(
                        f"ALTER TABLE autotrade_sessions ADD COLUMN {name} INTEGER")
                    added_cols.append(name)

        # ── 2f. LADDER ORCHESTRATOR — tag each spawned child session with its
        # parent campaign. NULLABLE ladder_id on autotrade_sessions: NULL (the
        # default for every existing/standalone session) means "not part of a
        # ladder" → byte-for-byte unchanged. Set only on children the ladder
        # daily-tick spawns. Additive + idempotent.
        if _table_exists(con, "autotrade_sessions"):
            have = set(_existing_columns(con, "autotrade_sessions"))
            if "ladder_id" not in have:
                con.execute(
                    "ALTER TABLE autotrade_sessions ADD COLUMN ladder_id TEXT")
                added_cols.append("ladder_id")
            # Index the ladder tag AFTER the column exists (executescript above
            # runs before this ALTER, so the index can't live in _SCHEMA_SQL).
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_autotrade_sessions_ladder "
                "ON autotrade_sessions(ladder_id)")

        # ── 2g. LIVE CONFIG EDIT — config_version on autotrade_sessions ───────
        # A monotone counter bumped every time an operator applies a live
        # risk/exit config edit (PATCH .../session/{id}/config). A long-lived
        # in-memory session compares the persisted config_version to the version
        # it last loaded; a higher value triggers maybe_reload_config() which
        # re-parses config_json and updates ONLY the whitelisted risk/exit fields
        # (never invested_basis / trail state / positions). Additive + idempotent;
        # DEFAULT 0 → every existing session starts unversioned = no reload, i.e.
        # byte-for-byte unchanged. NOT NULL DEFAULT 0 is a valid SQLite ADD COLUMN.
        if _table_exists(con, "autotrade_sessions"):
            have = set(_existing_columns(con, "autotrade_sessions"))
            if "config_version" not in have:
                con.execute(
                    "ALTER TABLE autotrade_sessions "
                    "ADD COLUMN config_version INTEGER NOT NULL DEFAULT 0")
                added_cols.append("config_version")

        # ── 2h. LIVE CONFIG EDIT (ladder) — child-config template + version ───
        # child_config_json: OPTIONAL JSON overriding the risk/exit knobs of the
        #   positional child config the ladder spawns (future baskets). NULL (the
        #   default for every existing ladder) → the built-in validated positional
        #   preset, byte-for-byte unchanged. Only the whitelisted risk/exit keys
        #   are ever written here; capital/product/duration stay on their own
        #   columns. config_version: bumped on each template edit (parity with the
        #   session counter). Additive + idempotent.
        if _table_exists(con, "autotrade_ladders"):
            have = set(_existing_columns(con, "autotrade_ladders"))
            if "child_config_json" not in have:
                con.execute(
                    "ALTER TABLE autotrade_ladders ADD COLUMN child_config_json TEXT")
                added_cols.append("child_config_json")
            if "config_version" not in have:
                con.execute(
                    "ALTER TABLE autotrade_ladders "
                    "ADD COLUMN config_version INTEGER NOT NULL DEFAULT 0")
                added_cols.append("ladder_config_version")

        # ── 2i. RMS (SPRINT CLUSTER 4, CAP 3) — MIS broker-side protective SL-M ─
        # slm_order_id: the broker order-id of the Falcon-owned DAY SL-M (stop-
        # market) protective order placed on an MIS leg (which gets NO GTT). It is
        # a HARD broker-side downside cap even if the monitor process is down, and
        # is cancelled by Falcon at exit/square-off (day-valid auto-cancels anyway).
        # NULLABLE → every existing / paper / non-MIS row stays NULL (byte-for-byte
        # unchanged; SL-M placement is gated by config.mis_protective_slm_enabled +
        # _live_allowed). Additive + idempotent.
        if _table_exists(con, "autotrade_positions"):
            have = set(_existing_columns(con, "autotrade_positions"))
            if "slm_order_id" not in have:
                con.execute(
                    "ALTER TABLE autotrade_positions ADD COLUMN slm_order_id TEXT")
                added_cols.append("slm_order_id")

        # ── 2k. FALCON INTRADAY MAGNIFIER — split-entry / deferred-arm state ──
        # A Magnifier session (strategy=='intraday_magnifier') fills its basket in
        # TWO legs (50% @ 09:15, 50% @ 09:16) and arms NO stop/trail until BOTH
        # legs are in (the core edge). These NULLABLE columns make that two-phase
        # entry restart-durable:
        #   mag_entry_complete (INT 0/1): 0 while only the 09:15 leg is filled (the
        #     tick keeps the trail/stop DORMANT); 1 once the 09:16 leg is in and the
        #     basket trail is armed on the BLENDED cost. DEFAULT 0.
        #   mag_leg2_plan_json (TEXT): the second-leg plan (per-name remaining qty +
        #     broker_profile) captured at 09:15 so a restart between 09:15 and 09:16
        #     can still complete the second leg. NULL for every non-Magnifier row.
        # Additive + idempotent; every existing/non-Magnifier session ignores them.
        if _table_exists(con, "autotrade_sessions"):
            have = set(_existing_columns(con, "autotrade_sessions"))
            if "mag_entry_complete" not in have:
                con.execute(
                    "ALTER TABLE autotrade_sessions "
                    "ADD COLUMN mag_entry_complete INTEGER NOT NULL DEFAULT 0")
                added_cols.append("mag_entry_complete")
            if "mag_leg2_plan_json" not in have:
                con.execute(
                    "ALTER TABLE autotrade_sessions "
                    "ADD COLUMN mag_leg2_plan_json TEXT")
                added_cols.append("mag_leg2_plan_json")

        # ── 2l. CAMPAIGN TYPE on the ladder — positional (default) vs magnifier ─
        # campaign_type distinguishes the existing POSITIONAL Auto-Ladder (default,
        # byte-for-byte unchanged) from the INTRADAY MAGNIFIER monthly campaign
        # (MIS intraday children squared off same day). NULL/'positional' = the
        # existing behaviour. Additive + idempotent.
        if _table_exists(con, "autotrade_ladders"):
            have = set(_existing_columns(con, "autotrade_ladders"))
            if "campaign_type" not in have:
                con.execute(
                    "ALTER TABLE autotrade_ladders "
                    "ADD COLUMN campaign_type TEXT NOT NULL DEFAULT 'positional'")
                added_cols.append("campaign_type")

        # ── 2j. CLUSTER 9 ITEM 5 — order_events dedup key now includes the
        # broker_profile (COALESCE'') so the SAME broker_order_id on two different
        # accounts/brokers never false-dedups. The new profile-scoped UNIQUE index
        # is created by _SCHEMA_SQL above (idempotent); here we DROP the pre-ITEM-5
        # 2-col index for existing DBs. Idempotent (DROP IF EXISTS). SQLite treats
        # the COALESCE expression index as a valid conflict target for INSERT OR
        # IGNORE, so append_event's dedup keeps working unchanged for single-account.
        if _table_exists(con, "autotrade_order_events"):
            con.execute("DROP INDEX IF EXISTS uq_autotrade_order_events_bid_type")

        for t in (
            "autotrade_positions",
            "autotrade_sessions", "autotrade_config_presets",
            "autotrade_broker_profiles", "autotrade_slippage",
            "autotrade_portfolio_snapshots", "autotrade_kill_switch_log",
            "broker_accounts", "autotrade_ladders", "autotrade_recon_alerts",
            "autotrade_order_events", "autotrade_alerts", "autotrade_claims",
            "autotrade_config_edits", "autotrade_session_account_allocations",
            "strategy_visibility",
        ):
            if _table_exists(con, t):
                created_tables.append(t)
        con.commit()

    manifest = {"added_columns": added_cols, "tables_present": created_tables}
    log.info("AutoTrade migrations OK: +cols=%s tables=%s",
             added_cols, created_tables)
    return manifest


_SCHEMA_SQL = """
-- ISOLATED autotrade session positions. The autotrade system reads/writes ONLY
-- this table for session positions — never falcon_position_state. Composite
-- uniqueness on (session_id, symbol, broker_profile) so a paper session can
-- never collide with (or overwrite) a real Falcon swing position keyed by
-- `symbol` in falcon_position_state.
CREATE TABLE IF NOT EXISTS autotrade_positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL,
    broker_profile  TEXT,
    symbol          TEXT NOT NULL,
    instrument_type TEXT NOT NULL DEFAULT 'EQ',
    exchange        TEXT,
    qty             INTEGER NOT NULL DEFAULT 0,
    avg_price       REAL NOT NULL DEFAULT 0,
    sl_level        REAL,
    target_price    REAL,
    ltp             REAL,
    unrealised_pnl  REAL,
    status          TEXT NOT NULL DEFAULT 'OPEN',  -- OPEN | CLOSED | EXIT_FAILED
    exit_lock       INTEGER NOT NULL DEFAULT 0,
    exit_initiated_by TEXT,
    opened_at       TEXT,
    closed_at       TEXT,
    exit_price      REAL,
    realised_pnl    REAL,
    close_reason    TEXT,
    -- FEATURE 1: broker-held per-position GTT-OCO backup (LIVE only). gtt_id is
    -- the Kite GTT trigger id; gtt_stop/gtt_target the placed/intended levels.
    gtt_id          TEXT,
    gtt_stop        REAL,
    gtt_target      REAL,
    -- RECONCILIATION FRAMEWORK (Phase 1): the broker order-ids for this position.
    -- entry_order_id = the ENTRY fill's order id; exit_order_id = the EXIT fill's
    -- order id (or the GTT-fired order). Both NULLABLE — enable PER-SESSION
    -- attribution of a broker order by order-id (never the account aggregate).
    entry_order_id  TEXT,
    exit_order_id   TEXT,
    -- FUTURES long/short. 'long' (default) = entry BUY, exit SELL, P&L
    -- (ltp-avg)*qty. 'short' = entry SELL, exit BUY-to-cover, P&L (avg-ltp)*qty.
    -- Every equity/long row is 'long' → byte-for-byte unchanged.
    direction       TEXT NOT NULL DEFAULT 'long',
    -- STEP-LOCK SCOPE == "stock": this position's OWN ratcheting trail state.
    -- pos_trail_armed 0/1; pos_trail_peak = high-water g_stock (uPnL / slice).
    -- Read only when step_lock_scope=="stock"; default-0/NULL otherwise.
    pos_trail_armed INTEGER DEFAULT 0,
    pos_trail_peak  REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_autotrade_positions_sess_sym_prof
    ON autotrade_positions(session_id, symbol, broker_profile);
CREATE INDEX IF NOT EXISTS idx_autotrade_positions_session
    ON autotrade_positions(session_id, status);

-- Sessions: one row per TradingSession. config_json holds the full
-- TradingSessionConfig (sans secrets). status drives the state machine.
CREATE TABLE IF NOT EXISTS autotrade_sessions (
    session_id      TEXT PRIMARY KEY,             -- uuid4 hex
    created_at      TEXT NOT NULL,                -- ISO IST
    started_at      TEXT,
    closed_at       TEXT,
    status          TEXT NOT NULL DEFAULT 'CREATED',
        -- CREATED | RUNNING | KILLING | CLOSED | FAILED
    mode            TEXT NOT NULL DEFAULT 'paper',   -- 'paper' | 'live'
    total_allocated_capital REAL NOT NULL,
    -- FROZEN invested (notional) capital basis = Σ(qty*avg_price) at entry.
    -- Product-aware (MTF leveraged value / CNC cash). The kill-switch + gross
    -- return denominator. NULL until _fire_entries captures it. (Also added via
    -- idempotent ALTER for DBs created before this column.)
    invested_basis  REAL,
    -- PER-STOCK STEP-LOCK Fix A: FROZEN entry basket notional = Σ(qty*avg_price)
    -- over ALL originally-filled positions. The per-stock capital-slice denominator
    -- (session._run_per_stock_step_lock); frozen once at entry, never shrinks as
    -- names close (fixes survivors' slices inflating → stop firing at a larger loss).
    entry_basket_notional REAL,
    -- INTRADAY-BASKET trailing-profit state (strategy=="intraday_basket").
    -- trail_armed: 0/1 — has the trail armed (G crossed +arm_pct). trail_peak:
    -- the high-water gross return G since arming. Persisted each tick + restored
    -- on boot-resume so the trail continues correctly mid-day. (Also added via
    -- idempotent ALTER for DBs created before these columns.)
    trail_armed     INTEGER DEFAULT 0,
    trail_peak      REAL,
    -- SPEED-PASS latency observability. entry_latency_ms: fire start → all legs
    -- settled. exit_latency_ms: flatten trigger → all positions flat. (Also
    -- added via idempotent ALTER for DBs created before these columns.)
    entry_latency_ms INTEGER,
    exit_latency_ms  INTEGER,
    -- LIVE CONFIG EDIT: bumped on every applied risk/exit config edit so a
    -- long-lived in-memory session hot-reloads the whitelisted knobs. DEFAULT 0.
    config_version  INTEGER NOT NULL DEFAULT 0,
    config_json     TEXT NOT NULL,
    last_gross_return REAL,
    kill_reason     TEXT,
    notes           TEXT
);

-- Named, reusable config presets (UI "save preset").
CREATE TABLE IF NOT EXISTS autotrade_config_presets (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    created_at  TEXT NOT NULL,
    config_json TEXT NOT NULL
);

-- Broker profiles. SECRETS ARE NOT STORED HERE — api_key/secret/token live in
-- env / the existing kite_tokens table. We persist only routing + non-secret
-- profile fields, plus a boolean for whether creds are configured.
CREATE TABLE IF NOT EXISTS autotrade_broker_profiles (
    profile_id      TEXT PRIMARY KEY,
    broker_name     TEXT NOT NULL,                -- zerodha | fyers | upstox | angel | dhan
    allocated_capital REAL NOT NULL DEFAULT 0,
    symbols_json    TEXT,                         -- NULL = use Falcon top_n
    rank_low        INTEGER,
    rank_high       INTEGER,
    order_product   TEXT NOT NULL DEFAULT 'CNC',
    instrument_type TEXT NOT NULL DEFAULT 'EQ',
    enabled         INTEGER NOT NULL DEFAULT 1,
    creds_configured INTEGER NOT NULL DEFAULT 0,  -- 1 if env/token present
    created_at      TEXT NOT NULL,
    updated_at      TEXT
);

-- Per-fill slippage record.
CREATE TABLE IF NOT EXISTS autotrade_slippage (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT,
    broker_profile  TEXT,
    symbol          TEXT NOT NULL,
    expected_price  REAL NOT NULL,
    actual_price    REAL NOT NULL,
    slippage_pct    REAL NOT NULL,
    qty             INTEGER NOT NULL,
    recorded_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_autotrade_slippage_session
    ON autotrade_slippage(session_id, recorded_at DESC);

-- Periodic portfolio snapshots (gross return over time, for charts/audit).
CREATE TABLE IF NOT EXISTS autotrade_portfolio_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL,
    snapped_at      TEXT NOT NULL,
    gross_return    REAL NOT NULL,
    total_unrealised REAL NOT NULL,
    total_allocated_capital REAL NOT NULL,
    n_open_positions INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_autotrade_snap_session
    ON autotrade_portfolio_snapshots(session_id, snapped_at DESC);

-- PHASE-2 MULTI-TENANT credential vault. One row per (user, broker, account).
-- api_secret_enc / access_token_enc are FERNET-ENCRYPTED at rest (the key lives
-- outside the DB, via vault.KeyProvider). Secrets are NEVER returned over any
-- API (only status + masked previews). A user holds MANY accounts via distinct
-- account_label values; UNIQUE(user_id,broker,account_label) blocks dup labels
-- but allows unlimited accounts. Per-user isolation: every read is WHERE
-- user_id=?. This table is INDEPENDENT of the legacy kite_tokens table (which
-- the legacy /falcon path keeps using untouched).
CREATE TABLE IF NOT EXISTS broker_accounts (
    broker_account_id TEXT PRIMARY KEY,           -- uuid4 hex (stable handle)
    user_id           TEXT NOT NULL,              -- portal user
    broker            TEXT NOT NULL,              -- zerodha|upstox|angel|dhan|fyers
    account_label     TEXT NOT NULL,              -- user-chosen, e.g. "Main Kite"
    api_key           TEXT NOT NULL,              -- broker app api_key
    api_secret_enc    BLOB,                       -- Fernet-ENCRYPTED api_secret
    access_token_enc  BLOB,                       -- Fernet-ENCRYPTED access_token
    token_date        TEXT,                       -- IST date the token was minted
    token_expiry      TEXT,                       -- ISO IST expiry (broker-specific)
    status            TEXT NOT NULL DEFAULT 'PENDING',
        -- PENDING | ACTIVE | EXPIRED | REVOKED | ERROR
    created_at        TEXT NOT NULL,              -- ISO IST
    updated_at        TEXT,
    last_login_at     TEXT,
    -- BROKER-AGNOSTIC AUTH FOUNDATION (Stage 0), all NULLABLE / default-absent:
    refresh_token_enc  BLOB,   -- Fernet-ENCRYPTED refresh token (brokers that have one)
    token_expires_at   TEXT,   -- absolute ISO-IST expiry (non-daily brokers)
    last_health_at     TEXT,   -- ISO-IST of the last validate() ping
    last_health_status TEXT,   -- ACTIVE | EXPIRED | REVOKED | ERROR
    last_error         TEXT,   -- last health/refresh error detail
    redirect_state     TEXT,   -- CSRF `state` for the OAuth round-trip
    UNIQUE (user_id, broker, account_label)
);
CREATE INDEX IF NOT EXISTS idx_broker_accounts_user
    ON broker_accounts(user_id, status);

-- Kill-switch fire audit.
CREATE TABLE IF NOT EXISTS autotrade_kill_switch_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT NOT NULL,
    fired_at        TEXT NOT NULL,
    trigger_reason  TEXT NOT NULL,
    gross_return    REAL,
    n_positions     INTEGER,
    n_exited_ok     INTEGER,
    n_exit_failed   INTEGER,
    mode            TEXT NOT NULL DEFAULT 'paper',
    detail_json     TEXT
);

-- ── LADDER ORCHESTRATOR (campaign layer) ────────────────────────────────────
-- A "set once, run for a month" campaign that opens/manages/rolls positional
-- baskets every trading day with no further operator input. Each opened basket
-- is a normal positional AutoTrade session (autotrade_sessions) TAGGED with this
-- ladder's ladder_id. This table holds ONLY the campaign-level state; the child
-- sessions + their positions live in the existing isolated tables (never
-- falcon_position_state). per_basket_capital is FROZEN at create (= total/3).
-- Capital accounting is on the TRADER-MONEY (margin) basis: free capital =
-- total_capital − Σ(child.total_allocated_capital of OPEN children); MTF leverage
-- lives INSIDE each child and never inflates this ceiling.
CREATE TABLE IF NOT EXISTS autotrade_ladders (
    ladder_id           TEXT PRIMARY KEY,             -- uuid4 hex
    user_id             TEXT,                         -- portal user (NULL = operator)
    broker_account_id   TEXT,                         -- vaulted account (NULL = global)
    mode                TEXT NOT NULL DEFAULT 'paper', -- 'paper' | 'live' (children inherit)
    total_capital       REAL NOT NULL,                -- the deployed-capital ceiling
    order_product       TEXT NOT NULL DEFAULT 'CNC',  -- CNC | MTF only (MIS rejected)
    per_basket_capital  REAL NOT NULL,                -- FROZEN at create = total/3
    status              TEXT NOT NULL DEFAULT 'RUNNING',
        -- RUNNING | PAUSED | ENDED | COMPLETED
    start_date          TEXT,                         -- ISO date the campaign began
    end_date            TEXT,                         -- ISO date (last trading day of
                                                      -- start month) or NULL = manual-only
    mode_kill           TEXT,                         -- flatten_now | stop_new_let_finish (on KILL)
    -- 5-DAY DOWNTURN ALERT state. rolling window of per-trading-day realized
    -- returns (JSON list of {date, ret}); alert_active latches on a down-crossing
    -- so we fire alerts.send() at most once per crossing (surfaced in status()).
    daily_returns_json  TEXT,                         -- JSON [{"date","ret"}, ...]
    alert_active        INTEGER NOT NULL DEFAULT 0,   -- 0/1 down-crossing latch
    last_tick_date      TEXT,                         -- ISO date of the last daily tick (idempotency)
    -- LIVE CONFIG EDIT: optional risk/exit override for FUTURE spawned children
    -- (NULL = built-in validated positional preset) + a monotone template
    -- version bumped on each edit. Capital/product/duration stay on their own
    -- columns above; only whitelisted risk/exit keys are written here.
    child_config_json   TEXT,
    config_version      INTEGER NOT NULL DEFAULT 0,
    created_at          TEXT NOT NULL,                -- ISO IST
    updated_at          TEXT
);
CREATE INDEX IF NOT EXISTS idx_autotrade_ladders_user
    ON autotrade_ladders(user_id, status);

-- ── RECONCILIATION FRAMEWORK (Phase 3): lightweight recon alert log ──────────
-- The ORDER-ID-DRIVEN reconciler (monitoring/position_reconciler.py) writes a
-- row here whenever it detects a broker/DB divergence it CANNOT attribute to one
-- of our order-ids and therefore REFUSES to auto-correct: an UNATTRIBUTED_CLOSE
-- (broker holds fewer than we track, no filled order explains it — e.g. an RMS
-- auto-square with no order we tracked) or an ORPHAN_AT_BROKER (broker holds more
-- than we track). These rows are OBSERVABILITY ONLY — they NEVER mutate a
-- position; ops reviews and reconciles by hand. Additive + idempotent.
CREATE TABLE IF NOT EXISTS autotrade_recon_alerts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,                 -- ISO IST when detected
    session_id  TEXT,                          -- reconciling session (may be NULL)
    symbol      TEXT NOT NULL,
    product     TEXT,                          -- CNC | MIS | NRML | MTF
    kind        TEXT NOT NULL,                 -- UNATTRIBUTED_CLOSE | ORPHAN_AT_BROKER
    detail      TEXT                           -- human-readable divergence detail
);
CREATE INDEX IF NOT EXISTS idx_autotrade_recon_alerts_ts
    ON autotrade_recon_alerts(ts DESC);

-- ── LIVE CONFIG-EDIT AUDIT (SPRINT CLUSTER 9b ITEM 10) ──────────────────────
-- One row per SUCCESSFULLY APPLIED live config edit (session or ladder), for a
-- durable who/when/what audit of every hot-reload of a running session's risk/
-- exit knobs. The PATCH endpoint uses OPTIMISTIC CONCURRENCY (the caller carries
-- the config_version it based its edit on) so two operators can't clobber each
-- other; a stale edit is 409-rejected and writes NO row here. Additive +
-- idempotent (CREATE IF NOT EXISTS). Observability only — never gates an edit.
-- DATA-ISOLATION: autotrade-only; never touches falcon_position_state.
CREATE TABLE IF NOT EXISTS autotrade_config_edits (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,                 -- ISO IST when applied
    target_type  TEXT NOT NULL,                 -- 'session' | 'ladder'
    target_id    TEXT NOT NULL,                 -- session_id | ladder_id
    user_id      TEXT,                          -- the caller (who)
    from_version INTEGER,                       -- config_version before the edit
    to_version   INTEGER,                       -- config_version after the edit
    field_changes TEXT,                         -- JSON {field: applied_value}
    detail       TEXT
);
CREATE INDEX IF NOT EXISTS idx_autotrade_config_edits_target
    ON autotrade_config_edits(target_type, target_id, ts DESC);

-- ── PER-ACCOUNT COMMITTED-CAPITAL ATTRIBUTION (CLUSTER 9d FIX F2) ────────────
-- A session's total_allocated_capital is a SESSION-LEVEL number, but a MULTI-
-- ACCOUNT session (profiles bound to DIFFERENT broker accounts) must attribute
-- its reserved capital PER broker_account_id so the RMS committed-capital ledger
-- (risk_manager.committed_capital) budgets each account correctly — otherwise the
-- WHOLE session's capital lands on ONE account and the other account is invisible
-- to its own budget. One row per (session, broker_account_id), written at fire
-- time ONLY for sessions that actually span >1 distinct account. A SINGLE-account
-- session writes NOTHING here → committed_capital falls back to the session-level
-- total_allocated_capital keyed by the session's own account (byte-identical).
-- broker_account_id NULL = the operator/global account. Additive + idempotent.
-- DATA-ISOLATION: autotrade-only; never touches falcon_position_state.
CREATE TABLE IF NOT EXISTS autotrade_session_account_allocations (
    session_id        TEXT NOT NULL,
    broker_account_id TEXT,                       -- NULL = operator/global account
    allocated_capital REAL NOT NULL DEFAULT 0,
    updated_at        TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_atsaa_session_account
    ON autotrade_session_account_allocations(
        session_id, COALESCE(broker_account_id,''));

-- ── DURABLE ORDER-EVENT LEDGER (SPRINT CLUSTER 2) ───────────────────────────
-- Append-only lifecycle trail for every order our sessions place. One row per
-- lifecycle transition (ORDER_CREATED intent → ORDER_SUBMITTED → FILLED/PARTIAL,
-- EXIT_PLACED → EXIT_FILLED → POSITION_CLOSED, plus REJECTED / EXIT_FAILED /
-- RECONCILE_CLOSE). This converts exactly-once + reconciliation from in-process
-- accidents into a DURABLE, cross-process, cross-day record:
--   * CAP 2 — reconstructable ordered event trail for any position.
--   * CAP 3 — a durable ORDER_CREATED intent written BEFORE the broker call, so a
--     crash between broker-accept and the position-row insert leaves an orphan the
--     recovery/reconcile path can find (via the persisted client_order_id).
--   * CAP 5 — cross-day attribution: the reconciler consults a persisted
--     EXIT_FILLED/close event before the single-day orderbook.
-- IDEMPOTENT: the UNIQUE(broker_order_id, event_type) index + INSERT OR IGNORE
-- make a replayed broker event (same broker order-id + type) a no-op. Rows with a
-- NULL broker_order_id (paper orders + pre-submission intent) never conflict
-- (SQLite NULLs are distinct in a UNIQUE index) — correct, since paper/intent have
-- no broker order-id to dedup on; they are correlated by client_order_id.
-- DATA-ISOLATION: this table is autotrade-only; never touches falcon_position_state.
CREATE TABLE IF NOT EXISTS autotrade_order_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              TEXT NOT NULL,                 -- ISO IST
    session_id      TEXT,
    position_ref    TEXT,                          -- position id or "session:symbol"
    symbol          TEXT,
    product         TEXT,
    broker_profile  TEXT,
    broker_order_id TEXT,                          -- NULL for paper / pre-submission
    client_order_id TEXT,                          -- our durable ownership key
    event_type      TEXT NOT NULL,
    qty             INTEGER,
    price           REAL,
    source          TEXT,                          -- entry|exit|kill|reconcile|...
    detail          TEXT
);
-- Dedup real broker events; NULL broker_order_id rows are exempt (distinct NULLs).
-- CLUSTER 9 ITEM 5 (2026-07-11): the dedup key includes broker_profile so the SAME
-- broker_order_id on TWO different broker accounts/brokers (each broker mints its
-- own order-id space — the ids CAN collide across accounts) never false-dedups into
-- ONE row. COALESCE(broker_profile,'') keeps NULL-profile rows deduping as before
-- (a plain 3-col index would make NULL profiles distinct → lost dedup for the
-- global/operator path). The pre-ITEM-5 2-col index is dropped in db_migrations.
CREATE UNIQUE INDEX IF NOT EXISTS uq_autotrade_order_events_bid_type_prof
    ON autotrade_order_events(broker_order_id, event_type,
                             COALESCE(broker_profile,''));
CREATE INDEX IF NOT EXISTS idx_autotrade_order_events_session
    ON autotrade_order_events(session_id, symbol);
CREATE INDEX IF NOT EXISTS idx_autotrade_order_events_coid
    ON autotrade_order_events(client_order_id);

-- ── OBSERVABILITY / ALERTING (SPRINT CLUSTER 6) ─────────────────────────────
-- Durable record for every alert the notifier (autotrade/alerts.py) raises: the
-- MANUAL-EXIT-REQUIRED / naked-position / reconcile-divergence / staleness /
-- daily-loss-breaker pages that must reach a human. One row per fired alert.
--   incident_id  — stable (kind|session|symbol) dedup key so a condition true on
--                  every ~5s tick pages at most once per window (send_urgent_deduped).
--   acknowledged — 1 once an operator acks it (POST .../alerts/{id}/ack).
--   escalated    — 1 once an UNACKED urgent alert older than the threshold was
--                  re-pushed (alerts.escalate_stale_alerts).
--   pushed / push_result — whether the web_push dispatch succeeded + its result
--                  (a push failure NEVER blocks persistence; the row is still written).
-- OBSERVABILITY ONLY — never mutates a position. Additive + idempotent.
CREATE TABLE IF NOT EXISTS autotrade_alerts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,                 -- ISO IST when raised
    incident_id  TEXT,                          -- dedup key (kind|session|symbol)
    severity     TEXT NOT NULL DEFAULT 'info',  -- info | warn | critical | urgent
    kind         TEXT NOT NULL,                 -- EXIT_FAILED | NAKED_POSITION | ...
    session_id   TEXT,                          -- owning session (may be NULL)
    symbol       TEXT,                          -- affected symbol (may be NULL)
    detail       TEXT,                          -- human-readable detail
    acknowledged INTEGER NOT NULL DEFAULT 0,    -- 0/1 operator ack
    escalated    INTEGER NOT NULL DEFAULT 0,    -- 0/1 re-pushed after threshold
    pushed       INTEGER NOT NULL DEFAULT 0,    -- 0/1 web_push dispatch succeeded
    push_result  TEXT                           -- transport result / error string
);
CREATE INDEX IF NOT EXISTS idx_autotrade_alerts_ts
    ON autotrade_alerts(ts DESC);
CREATE INDEX IF NOT EXISTS idx_autotrade_alerts_incident
    ON autotrade_alerts(incident_id, ts);
CREATE INDEX IF NOT EXISTS idx_autotrade_alerts_session
    ON autotrade_alerts(session_id, ts DESC);

-- SPRINT CLUSTER 8 ITEM 2 — cross-process DURABLE CLAIMS (CAS + lease). Backs the
-- in-process fire_guard (_FIRED/_ENTRY_CLAIMED) and the exit single-flight so a
-- claim survives a restart and holds ACROSS processes. Keyed by claim_key
-- ("fire:<sid>" | "entry:<sid>" | "exitflight:<sid>:<sym>"); leased_until is an ISO
-- IST timestamp — a claim whose lease has passed is takeable by a new claimant.
CREATE TABLE IF NOT EXISTS autotrade_claims (
    claim_key    TEXT PRIMARY KEY,   -- unique claim identity
    holder       TEXT,               -- optional holder tag (pid/host/diag)
    leased_until TEXT NOT NULL,      -- ISO IST; claim is live while now < this
    created_at   TEXT NOT NULL       -- ISO IST when (re)claimed
);

-- ── STRATEGY VISIBILITY (admin-controlled per-strategy show/hide) ────────────
-- An admin toggle deciding whether a given strategy is offered to NON-admin
-- power users in the create-form strategy list. A row here is an EXPLICIT admin
-- override; when NO row exists the effective visibility falls back to the
-- registry descriptor's default_visible (see strategy_registry.py) — so an
-- experimental strategy (Falcon BTST Oscillator, Falcon Intraday Magnifier)
-- ships HIDDEN until an admin flips it, while the long-shipped strategies stay
-- visible. Admins ALWAYS see every strategy regardless of this table.
CREATE TABLE IF NOT EXISTS strategy_visibility (
    strategy_id             TEXT PRIMARY KEY,       -- registry strategy_id
    visible_to_power_users  INTEGER NOT NULL DEFAULT 0,  -- 0 hidden | 1 visible
    updated_by              TEXT,                   -- admin user_id / 'operator'
    updated_at              TEXT
);
"""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run_migrations())
