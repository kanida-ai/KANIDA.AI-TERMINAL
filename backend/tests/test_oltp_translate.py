"""Regression tests for the OLTP SQLite->Postgres translation (oltp_db).

The translation is the concentrated risk of the whole cutover — one wrong rule
corrupts live order/position writes. These lock the behaviour with a MOCK PK
registry so no database is needed.
"""
import os
import sys

import pytest

_BACKEND = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

import oltp_db  # noqa: E402


@pytest.fixture(autouse=True)
def _mock_pks():
    saved = oltp_db._PK_CACHE
    oltp_db._PK_CACHE = {
        "autotrade_claims": ["claim_key"],
        "power_user_users": ["id"],
        "broker_accounts": ["broker_account_id"],
        "ohlc_daily": ["symbol", "trade_date"],
    }
    yield
    oltp_db._PK_CACHE = saved


def t(sql):
    return oltp_db.translate(sql)[0]


def test_qmark_becomes_pct():
    assert t("SELECT * FROM power_user_users WHERE id=?") == \
        "SELECT * FROM power_user_users WHERE id=%s"


def test_insert_or_ignore_single_pk():
    out = t("INSERT OR IGNORE INTO autotrade_claims(claim_key,holder) VALUES(?,?)")
    assert 'ON CONFLICT ("claim_key") DO NOTHING' in out
    assert "OR IGNORE" not in out and "?" not in out


def test_insert_or_replace_composite_pk_excludes_pk_cols():
    out = t("INSERT OR REPLACE INTO ohlc_daily (symbol,trade_date,close) VALUES (?,?,?)")
    assert 'ON CONFLICT ("symbol", "trade_date") DO UPDATE SET "close"=EXCLUDED."close"' in out
    # PK columns must NOT appear in the SET clause
    assert '"symbol"=EXCLUDED' not in out and '"trade_date"=EXCLUDED' not in out


def test_literal_percent_escaped_even_in_string():
    # psycopg2 scans string literals for % when binding params.
    out = t("SELECT * FROM x WHERE name LIKE '50%' AND id=?")
    assert "'50%%'" in out and out.endswith("id=%s")


def test_datetime_now_becomes_now():
    assert "NOW()" in t("UPDATE t SET updated_at=datetime('now') WHERE id=?")


def test_plain_insert_flags_lastrowid_append():
    _, want_returning = oltp_db.translate("INSERT INTO power_user_users(email) VALUES(?)")
    assert want_returning is True


def test_select_does_not_flag_returning():
    _, want_returning = oltp_db.translate("SELECT 1")
    assert want_returning is False


@pytest.mark.parametrize("bad", [
    "SELECT strftime('%Y', ts) FROM t",
    "PRAGMA table_info(t)",
])
def test_fail_loud_on_untranslatable(bad):
    with pytest.raises(oltp_db.OltpTranslateError):
        oltp_db.translate(bad)


def test_unknown_table_conflict_raises():
    with pytest.raises(oltp_db.OltpTranslateError):
        oltp_db.translate("INSERT OR IGNORE INTO unknown_tbl(a) VALUES(?)")
