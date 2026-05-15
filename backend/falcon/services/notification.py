"""Notification dispatch — in-app + email.

In-app: writes a row to falcon_notifications_out with channel='inapp'. The
frontend polls /api/falcon/admin/inbox or pulls latest signals.

Email: SMTP — config via env vars (FALCON_SMTP_*). If SMTP not configured,
silently skips email but still queues the in-app notification.
"""
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List

from ..config import (
    NOTIFY_EMAIL_FROM, NOTIFY_EMAIL_TO,
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS,
)
from ..db import falcon_conn


def queue_signal_notification(signal_summary: Dict) -> None:
    """Write in-app notification for a signal run; attempt email if configured."""
    n_picks = signal_summary.get("n_picks", 0)
    sd = signal_summary.get("signal_date", "?")
    ed = signal_summary.get("entry_date", "?")
    top = signal_summary.get("top", [])[:10]

    subject = f"Falcon — {n_picks} picks for {ed or sd}"
    body_md = _build_md(signal_summary)
    body_html = _build_html(signal_summary)

    payload = {
        "kind":        "falcon_signals",
        "signal_date": sd,
        "entry_date":  ed,
        "n_picks":     n_picks,
        "top10":       [{"rank": i+1, "symbol": t["symbol"], "sector": t.get("sector"),
                            "score": round(t["score"], 1)} for i, t in enumerate(top)],
    }

    # Queue in-app
    with falcon_conn() as con:
        con.execute("""
            INSERT INTO falcon_notifications_out
              (channel, subject, body_md, body_html, payload_json, status)
            VALUES ('inapp', ?, ?, ?, ?, 'sent')
        """, (subject, body_md, body_html, json.dumps(payload)))
        con.commit()

    # Try email
    if SMTP_HOST and NOTIFY_EMAIL_TO:
        try:
            _send_email(subject, body_html)
            with falcon_conn() as con:
                con.execute("""
                    INSERT INTO falcon_notifications_out
                      (channel, subject, body_md, body_html, payload_json, status, sent_at)
                    VALUES ('email', ?, ?, ?, ?, 'sent', datetime('now'))
                """, (subject, body_md, body_html, json.dumps(payload)))
                con.commit()
        except Exception as e:
            with falcon_conn() as con:
                con.execute("""
                    INSERT INTO falcon_notifications_out
                      (channel, subject, body_md, body_html, status, error)
                    VALUES ('email', ?, ?, ?, 'failed', ?)
                """, (subject, body_md, body_html, str(e)))
                con.commit()


def _build_md(s: Dict) -> str:
    L = [f"# Falcon picks for {s.get('entry_date') or s.get('signal_date')}", ""]
    L.append(f"- Engine version: {s.get('engine_version', '?')}")
    L.append(f"- Signal date: {s.get('signal_date')}")
    L.append(f"- Entry day: {s.get('entry_date')}")
    L.append(f"- Eligible patterns: {s.get('n_eligible_patterns', '?')}")
    L.append(f"- Qualifying stocks: {s.get('n_qualifying', '?')}")
    L.append(f"- Top picks: {s.get('n_picks', 0)}")
    L.append("")
    L.append("| Rank | Symbol | Sector | Score | n_fires |")
    L.append("|---|---|---|---|---|")
    for i, t in enumerate(s.get("top", [])[:25], 1):
        L.append(f"| {i} | **{t['symbol']}** | {t.get('sector','—')} | "
                 f"{t['score']:.0f} | {t['n_fires']} |")
    return "\n".join(L)


def _build_html(s: Dict) -> str:
    rows = []
    for i, t in enumerate(s.get("top", [])[:25], 1):
        rows.append(f"<tr><td>{i}</td><td><strong>{t['symbol']}</strong></td>"
                     f"<td>{t.get('sector','')}</td><td>{t['score']:.0f}</td>"
                     f"<td>{t['n_fires']}</td></tr>")
    return f"""<!doctype html><html><body style="font-family:Inter,system-ui,sans-serif">
<h2>Falcon picks for {s.get('entry_date') or s.get('signal_date')}</h2>
<p>Signal date: <code>{s.get('signal_date')}</code> ·
   Entry day: <code>{s.get('entry_date')}</code> ·
   Picks: <strong>{s.get('n_picks', 0)}</strong></p>
<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse">
<thead><tr><th>Rank</th><th>Symbol</th><th>Sector</th><th>Score</th><th>n_fires</th></tr></thead>
<tbody>{"".join(rows)}</tbody></table>
<p style="color:#888">Falcon V{s.get('engine_version','?')} — KANIDA.AI Terminal</p>
</body></html>"""


def _send_email(subject: str, html: str) -> None:
    if not (SMTP_HOST and NOTIFY_EMAIL_FROM and NOTIFY_EMAIL_TO):
        raise RuntimeError("SMTP not configured")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = NOTIFY_EMAIL_FROM
    msg["To"]      = NOTIFY_EMAIL_TO
    msg.attach(MIMEText(html, "html"))
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        if SMTP_USER and SMTP_PASS:
            server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(NOTIFY_EMAIL_FROM, [a.strip() for a in NOTIFY_EMAIL_TO.split(",") if a.strip()],
                          msg.as_string())
