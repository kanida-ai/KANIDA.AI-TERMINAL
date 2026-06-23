"""
Schema for the persona engine (spec §5), SQLite-translated.

Postgres → SQLite mapping applied:
  BIGSERIAL PRIMARY KEY  -> INTEGER PRIMARY KEY AUTOINCREMENT
  BOOLEAN                -> INTEGER (0/1)
  TIMESTAMP DEFAULT NOW()-> TEXT DEFAULT (datetime('now'))
  VARCHAR(n)/DATE/FLOAT  -> TEXT / REAL

All tables are ADDITIVE — they sit alongside the existing falcon_* tables in the
RND DB and never alter them. Re-running is idempotent.
"""
from __future__ import annotations

import sqlite3

DDL = [
    # ── F&O Trader: daily predictions ──────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fo_daily_predictions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        prediction_date TEXT NOT NULL,
        direction       TEXT NOT NULL,        -- LONG / SHORT
        rank            INTEGER NOT NULL,     -- 1..10
        symbol          TEXT NOT NULL,
        sector          TEXT,
        long_score      REAL,
        short_score     REAL,
        top_features    TEXT,                 -- JSON
        top_rules       TEXT,                 -- JSON
        model_version   TEXT,
        created_at      TEXT DEFAULT (datetime('now')),
        UNIQUE (prediction_date, direction, symbol)
    )""",
    "CREATE INDEX IF NOT EXISTS ix_fo_pred_date ON fo_daily_predictions(prediction_date)",
    "CREATE INDEX IF NOT EXISTS ix_fo_pred_sym  ON fo_daily_predictions(symbol)",

    # ── F&O Trader: actual outcomes ────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fo_prediction_outcomes (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        prediction_date     TEXT NOT NULL,
        outcome_date        TEXT NOT NULL,
        symbol              TEXT NOT NULL,
        direction           TEXT,
        predicted_rank      INTEGER,
        actual_return       REAL,             -- full next-day open->close %
        actual_rank_gainers INTEGER,
        actual_rank_losers  INTEGER,
        in_top10_gainers    INTEGER,
        in_top10_losers     INTEGER,
        hit                 INTEGER,          -- correct direction + in actual top 10
        miss_reason         TEXT,
        created_at          TEXT DEFAULT (datetime('now')),
        UNIQUE (prediction_date, direction, symbol)
    )""",
    "CREATE INDEX IF NOT EXISTS ix_fo_out_date ON fo_prediction_outcomes(prediction_date)",

    # ── Long-Term Investor: daily predictions ──────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS lt_daily_predictions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        prediction_date TEXT NOT NULL,
        rank            INTEGER NOT NULL,
        symbol          TEXT NOT NULL,
        sector          TEXT,
        lt_score        REAL,
        top_features    TEXT,
        model_version   TEXT,
        created_at      TEXT DEFAULT (datetime('now')),
        UNIQUE (prediction_date, symbol)
    )""",
    "CREATE INDEX IF NOT EXISTS ix_lt_pred_date ON lt_daily_predictions(prediction_date)",

    # ── Long-Term Investor: actual outcomes ────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS lt_prediction_outcomes (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        prediction_date TEXT NOT NULL,
        symbol          TEXT NOT NULL,
        predicted_rank  INTEGER,
        ret_4wk         REAL,
        ret_8wk         REAL,
        actual_rank_4wk INTEGER,
        actual_rank_8wk INTEGER,
        in_top10_4wk    INTEGER,
        in_top10_8wk    INTEGER,
        is_multibagger  INTEGER,              -- 4wk or 8wk >= 40%
        miss_reason     TEXT,
        created_at      TEXT DEFAULT (datetime('now')),
        UNIQUE (prediction_date, symbol)
    )""",
    "CREATE INDEX IF NOT EXISTS ix_lt_out_date ON lt_prediction_outcomes(prediction_date)",

    # ── Miss analysis (spec §4.1 ANALYSE) ──────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fo_miss_analysis (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        outcome_date    TEXT NOT NULL,
        direction       TEXT NOT NULL,        -- LONG / SHORT
        symbol          TEXT NOT NULL,        -- the actual mover we missed
        actual_return   REAL,
        actual_rank     INTEGER,
        root_cause      TEXT,                 -- categorised reason
        features_at_pred TEXT,               -- JSON snapshot on prediction date
        created_at      TEXT DEFAULT (datetime('now')),
        UNIQUE (outcome_date, direction, symbol)
    )""",
    """
    CREATE TABLE IF NOT EXISTS lt_miss_analysis (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        prediction_date TEXT NOT NULL,
        window          TEXT NOT NULL,        -- 4wk / 8wk
        symbol          TEXT NOT NULL,
        actual_return   REAL,
        actual_rank     INTEGER,
        is_multibagger  INTEGER,
        root_cause      TEXT,
        features_at_pred TEXT,
        created_at      TEXT DEFAULT (datetime('now')),
        UNIQUE (prediction_date, window, symbol)
    )""",

    # ── Shared learning tables (spec §5.3) ─────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS learning_proposals (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        persona         TEXT NOT NULL,        -- FO / LT
        week_ending     TEXT NOT NULL,
        rule_name       TEXT,
        rule_logic      TEXT,
        n_occurrences   INTEGER,
        hit_rate        REAL,
        avg_return      REAL,
        regimes_tested  TEXT,
        n_years         INTEGER,
        n_sectors       INTEGER,
        worst_year_hr   REAL,
        consistency     REAL,
        status          TEXT DEFAULT 'TESTING',  -- TESTING/ACTIVE/DEMOTED/RETIRED
        proposed_change TEXT,
        human_approved  INTEGER DEFAULT 0,
        approved_at     TEXT,
        expires_at      TEXT,
        created_at      TEXT DEFAULT (datetime('now')),
        UNIQUE (persona, rule_name)
    )""",
    "CREATE INDEX IF NOT EXISTS ix_lp_persona ON learning_proposals(persona, status)",

    """
    CREATE TABLE IF NOT EXISTS model_weight_history (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        persona         TEXT,
        applied_date    TEXT,
        feature_name    TEXT,
        old_weight      REAL,
        new_weight      REAL,
        reason          TEXT,
        proposal_id     INTEGER,
        created_at      TEXT DEFAULT (datetime('now'))
    )""",

    # Current ACTIVE model weights (one row per persona+direction+feature).
    # The walk-forward reads this; the learning loop proposes changes that, once
    # human-approved, are written here and logged to model_weight_history.
    """
    CREATE TABLE IF NOT EXISTS persona_model_weights (
        persona      TEXT NOT NULL,           -- FO_LONG / FO_SHORT / LT
        feature_name TEXT NOT NULL,
        weight       REAL NOT NULL,
        updated_at   TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (persona, feature_name)
    )""",

    # Human review log (spec §4.1 APPROVE)
    """
    CREATE TABLE IF NOT EXISTS learning_review_log (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id  INTEGER,
        persona      TEXT,
        decision     TEXT,                    -- APPROVED / REJECTED / EXPIRED
        reviewer     TEXT,
        notes        TEXT,
        decided_at   TEXT DEFAULT (datetime('now'))
    )""",

    # ── Per-stock signal features (spec Step 2: "new signal_features table") ────
    # Persona-specific features computed at EOD per stock per day, point-in-time.
    # Kept separate from falcon_features (which we also reuse) so we never touch it.
    """
    CREATE TABLE IF NOT EXISTS persona_signal_features (
        symbol            TEXT NOT NULL,
        trade_date        TEXT NOT NULL,
        sector            TEXT,
        -- next-day-oriented (F&O)
        sig_ret_pct       REAL,   -- signal-day close-vs-prevclose %
        two_day_ret_pct   REAL,
        vol_ratio_20d     REAL,   -- today vol / 20d avg
        vol_5d_vs_20d     REAL,
        rs_index_5d       REAL,   -- relative strength vs Nifty over 5d
        rs_index_20d      REAL,
        rs_sector_5d      REAL,
        sector_rank_5d    REAL,   -- 0..1 percentile within sector by 5d ret
        gap_pct           REAL,
        close_loc         REAL,   -- (close-low)/(high-low)
        rsi_14            REAL,
        roc_5             REAL,
        roc_20            REAL,
        dist_high_20      REAL,
        dist_sma_20       REAL,
        atr_20_pct        REAL,
        n_distrib_5d      REAL,   -- distribution days (down on above-avg vol) last 5
        -- long-term-oriented
        consol_days       REAL,   -- consecutive days range<5% (rolling 10)
        obv_slope_20d     REAL,   -- OBV slope while price flat = accumulation
        rs_index_60d      REAL,
        trend_5_20_50_200 REAL,   -- count of up-aligned timeframes (0..4)
        prior_4w_ret      REAL,
        breakout_quality  REAL,   -- breakout vol vs 20d avg (0 if no breakout)
        avg_lift          REAL,   -- from falcon engine (NULL if <10 fires)
        created_at        TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (symbol, trade_date)
    )""",
    "CREATE INDEX IF NOT EXISTS ix_psf_date ON persona_signal_features(trade_date)",

    # ── Daily / weekly review (spec §2.8 / §3.6) ───────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fo_daily_review (
        prediction_date TEXT PRIMARY KEY,
        outcome_date    TEXT,
        review_json     TEXT,                 -- structured §2.8 fields
        review_text     TEXT,                 -- plain-English narrative
        long_hit        INTEGER,
        short_hit       INTEGER,
        combined_hit    INTEGER,
        created_at      TEXT DEFAULT (datetime('now'))
    )""",
    """
    CREATE TABLE IF NOT EXISTS lt_weekly_review (
        week_ending     TEXT PRIMARY KEY,
        review_json     TEXT,
        review_text     TEXT,
        hit_4wk         REAL,
        hit_8wk         REAL,
        created_at      TEXT DEFAULT (datetime('now'))
    )""",

    # ── F&O universe snapshot (populated at runtime from Kite) ──────────────────
    # fo_stock_master already exists (empty). We add a dated membership table so the
    # walk-forward can ask "was X F&O-eligible on date D" point-in-time.
    """
    CREATE TABLE IF NOT EXISTS fo_universe_membership (
        symbol        TEXT NOT NULL,
        as_of_date    TEXT NOT NULL,
        lot_size      INTEGER,
        source        TEXT,                   -- kite / fallback
        created_at    TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (symbol, as_of_date)
    )""",

    # Walk-forward run bookkeeping
    """
    CREATE TABLE IF NOT EXISTS persona_run_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        persona     TEXT,
        stage       TEXT,                     -- PREDICT/MEASURE/LEARN
        run_date    TEXT,
        n_dates     INTEGER,
        notes       TEXT,
        created_at  TEXT DEFAULT (datetime('now'))
    )""",
]


def create_all(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    for stmt in DDL:
        cur.execute(stmt)
    con.commit()


def drop_all(con: sqlite3.Connection) -> None:
    """Dev helper — drop persona tables only (never touches falcon_*)."""
    tables = [
        "fo_daily_predictions", "fo_prediction_outcomes", "lt_daily_predictions",
        "lt_prediction_outcomes", "fo_miss_analysis", "lt_miss_analysis",
        "learning_proposals", "model_weight_history", "persona_model_weights",
        "learning_review_log", "persona_signal_features", "fo_daily_review",
        "lt_weekly_review", "fo_universe_membership", "persona_run_log",
    ]
    for t in tables:
        con.execute(f"DROP TABLE IF EXISTS {t}")
    con.commit()


if __name__ == "__main__":
    from persona_engine import db

    con = db.connect()
    create_all(con)
    n = con.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND ("
        "name LIKE 'fo_%' OR name LIKE 'lt_%' OR name LIKE 'persona_%' "
        "OR name='learning_proposals' OR name='model_weight_history' "
        "OR name='learning_review_log')"
    ).fetchone()[0]
    print(f"DB: {db.resolve_db_path()}")
    print(f"persona-related tables present: {n}")
    con.close()
