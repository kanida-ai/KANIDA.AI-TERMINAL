"""
Token wallet — prepaid balance, charged by real backtest compute (like Claude credits).
Standalone impl uses a small SQLite table. In production, point WALLET_DB at the same DB as power_user and
the balance lives alongside power_user_users (see INTEGRATION.md for the one-line migration). All charges
are atomic: a backtest is only run AFTER the wallet confirms sufficient balance.
"""
from __future__ import annotations
import os, sqlite3, threading
_LOCK = threading.Lock()
WALLET_DB = os.environ.get("WALLET_DB", os.path.join(os.path.dirname(__file__), "wallet.db"))


def _con():
    con = sqlite3.connect(WALLET_DB)
    con.execute("CREATE TABLE IF NOT EXISTS agent_builder_wallet "
                "(user_id TEXT PRIMARY KEY, balance INTEGER NOT NULL DEFAULT 0, updated TEXT)")
    con.execute("CREATE TABLE IF NOT EXISTS agent_builder_ledger "
                "(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, delta INTEGER, reason TEXT, ts TEXT)")
    return con


def balance(user_id: str) -> int:
    con = _con(); r = con.execute("SELECT balance FROM agent_builder_wallet WHERE user_id=?", (user_id,)).fetchone()
    con.close(); return int(r[0]) if r else 0


def topup(user_id: str, tokens: int, reason="purchase") -> int:
    with _LOCK:
        con = _con()
        con.execute("INSERT INTO agent_builder_wallet(user_id,balance,updated) VALUES(?,?,datetime('now')) "
                    "ON CONFLICT(user_id) DO UPDATE SET balance=balance+?, updated=datetime('now')",
                    (user_id, tokens, tokens))
        con.execute("INSERT INTO agent_builder_ledger(user_id,delta,reason,ts) VALUES(?,?,?,datetime('now'))",
                    (user_id, tokens, reason))
        con.commit(); b = con.execute("SELECT balance FROM agent_builder_wallet WHERE user_id=?", (user_id,)).fetchone()[0]
        con.close(); return int(b)


def charge(user_id: str, tokens: int, reason="backtest"):
    """Atomically deduct if sufficient. Returns (ok, balance_after)."""
    with _LOCK:
        con = _con(); r = con.execute("SELECT balance FROM agent_builder_wallet WHERE user_id=?", (user_id,)).fetchone()
        bal = int(r[0]) if r else 0
        if tokens > bal:
            con.close(); return False, bal
        con.execute("UPDATE agent_builder_wallet SET balance=balance-?, updated=datetime('now') WHERE user_id=?", (tokens, user_id))
        con.execute("INSERT INTO agent_builder_ledger(user_id,delta,reason,ts) VALUES(?,?,?,datetime('now'))",
                    (user_id, -tokens, reason))
        con.commit(); con.close(); return True, bal - tokens
