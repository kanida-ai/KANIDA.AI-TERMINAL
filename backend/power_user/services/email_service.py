"""Transactional email — provider-abstracted, best-effort sender (Launch M8).

This module is the ONLY place that talks to an email provider. The signup flow
(M5) and the Razorpay webhook (M3) call the three public functions below;
everything provider-shaped (Resend / SES) stays here.

Functions (CONTRACT §5 / MASTER_SPEC M8):
  send_welcome(email)                                — on open signup success
  send_payment_receipt(email, amount, period_end)    — on subscription.charged/activated
  send_payment_failed(email)                         — on subscription.halted/cancelled

Design notes / invariants:
  * BEST-EFFORT EVERYWHERE (the #1 correctness rule for M8). Every public send
    function is wrapped so it NEVER raises into its caller — a missing provider,
    a missing API key, an unimportable SDK, or a provider-side error all
    log-and-noop. Signup and the webhook MUST NOT break because email is down.
  * Provider chosen by EMAIL_PROVIDER env: 'resend' (uses RESEND_API_KEY) or
    'ses'. The SDK is LAZILY imported inside the send path, so importing this
    module never fails in an environment without the package (this build/test
    env has neither).
  * If unconfigured (no EMAIL_PROVIDER, no key, no EMAIL_FROM, or SDK absent),
    we log at INFO/WARNING and return — never raise.
  * EMAIL_FROM is the sender address. No marketing — these are transactional only.
  * No secrets in code (INV5): RESEND_API_KEY / EMAIL_FROM come from env.
  * IST everywhere (INV4): any rendered date uses Asia/Kolkata.
  * PII discipline: every log line masks the recipient via redact_email() —
    matches services/auth.py.

Templates live in backend/power_user/email_templates/ (welcome, receipt,
payment_failed) as paired .html + .txt files with {placeholder} fields.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

from .auth import redact_email

log = logging.getLogger("kanida.power_user.email")
IST = timezone(timedelta(hours=5, minutes=30))

_TEMPLATE_DIR = Path(__file__).parent.parent / "email_templates"

# Subjects are owned here (not in the template files) so they stay in one place.
_SUBJECTS: Dict[str, str] = {
    "welcome":        "Welcome to Kanida.AI",
    "receipt":        "Your Kanida.AI payment receipt",
    "payment_failed": "Action needed: your Kanida.AI subscription",
}


# ──────────────────────────────────────────────────────────────────────────
#  Config (lazy, fail-graceful — mirrors billing_service._env conventions)
# ──────────────────────────────────────────────────────────────────────────

def _provider() -> str:
    """EMAIL_PROVIDER — 'resend' or 'ses'. Empty/unknown → unconfigured."""
    return os.environ.get("EMAIL_PROVIDER", "").strip().lower()


def _email_from() -> str:
    """EMAIL_FROM — the sender address (e.g. team@kanida.ai)."""
    return os.environ.get("EMAIL_FROM", "").strip()


def is_configured() -> bool:
    """True iff a provider + sender + the provider's key are all present.

    Cheap pre-check used by the send path to decide log-and-noop vs. attempt.
    Does NOT import any SDK (that happens only on an actual send).
    """
    prov = _provider()
    if not _email_from():
        return False
    if prov == "resend":
        return bool(os.environ.get("RESEND_API_KEY", "").strip())
    if prov == "ses":
        # boto3 picks up AWS creds from the standard chain (env / role / file);
        # we only require the provider + sender to be declared here.
        return True
    return False


# ──────────────────────────────────────────────────────────────────────────
#  Template loading + rendering
# ──────────────────────────────────────────────────────────────────────────

def _render(template: str, fields: Dict[str, str]) -> Optional[tuple[str, str]]:
    """Load template `<name>.html` + `<name>.txt` and .format() with `fields`.

    Returns (html, text) or None if either file is missing/unreadable. Never
    raises — the caller treats None as "could not render" and noops.
    """
    try:
        html = (_TEMPLATE_DIR / f"{template}.html").read_text(encoding="utf-8")
        text = (_TEMPLATE_DIR / f"{template}.txt").read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as e:
        log.warning("email: template '%s' unreadable — %s", template, e)
        return None
    try:
        return html.format(**fields), text.format(**fields)
    except (KeyError, IndexError, ValueError) as e:
        # A placeholder mismatch should never break a host flow — log + noop.
        log.warning("email: template '%s' render failed — %s", template, e)
        return None


# ──────────────────────────────────────────────────────────────────────────
#  Provider send paths (lazy SDK import — never at module import)
# ──────────────────────────────────────────────────────────────────────────

def _send_via_resend(*, to: str, subject: str, html: str, text: str) -> None:
    """Send through Resend. Raises on any failure (caller wraps + swallows)."""
    import resend  # noqa: WPS433  (lazy — absent in this build env)

    resend.api_key = os.environ["RESEND_API_KEY"].strip()
    resend.Emails.send({
        "from":    _email_from(),
        "to":      [to],
        "subject": subject,
        "html":    html,
        "text":    text,
    })


def _send_via_ses(*, to: str, subject: str, html: str, text: str) -> None:
    """Send through Amazon SES (boto3). Raises on failure (caller wraps)."""
    import boto3  # noqa: WPS433  (lazy — absent in this build env)

    client = boto3.client("ses")
    client.send_email(
        Source=_email_from(),
        Destination={"ToAddresses": [to]},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {
                "Html": {"Data": html, "Charset": "UTF-8"},
                "Text": {"Data": text, "Charset": "UTF-8"},
            },
        },
    )


def _dispatch(template: str, *, to: str, fields: Dict[str, str]) -> bool:
    """Render + send one transactional email. BEST-EFFORT — never raises.

    Returns True if the provider accepted the send, False on any
    unconfigured / render / provider failure (all logged, none raised).

    This is the single choke point that every public send_* function funnels
    through, so the "never raise into the caller" guarantee lives in exactly
    one place.
    """
    to = (to or "").strip()
    if not to:
        log.warning("email: %s skipped — empty recipient", template)
        return False

    if not is_configured():
        log.info("email: %s NOT sent (provider unconfigured) to %s",
                 template, redact_email(to))
        return False

    rendered = _render(template, fields)
    if rendered is None:
        return False
    html, text = rendered

    subject = _SUBJECTS.get(template, "Kanida.AI")
    prov = _provider()
    try:
        if prov == "resend":
            _send_via_resend(to=to, subject=subject, html=html, text=text)
        elif prov == "ses":
            _send_via_ses(to=to, subject=subject, html=html, text=text)
        else:
            # is_configured() already screened this, but be defensive.
            log.warning("email: unknown EMAIL_PROVIDER '%s' — %s not sent",
                        prov, template)
            return False
    except ImportError as e:
        log.warning("email: %s SDK not installed — %s not sent (%s)",
                    prov, template, e)
        return False
    except Exception as e:  # noqa: BLE001 — best-effort: swallow EVERYTHING
        log.warning("email: %s send failed for %s — %s",
                    template, redact_email(to), e)
        return False

    log.info("email: %s sent to %s", template, redact_email(to))
    return True


# ──────────────────────────────────────────────────────────────────────────
#  Public API — the three transactional emails (all best-effort)
# ──────────────────────────────────────────────────────────────────────────

def send_welcome(email: str) -> bool:
    """Welcome email on open-signup success. Best-effort; never raises.

    Only ever called for NEW signups (M5) — never for founding members
    (no marketing, no lifecycle mail to existing power users).
    """
    return _dispatch("welcome", to=email, fields={"email": email})


def send_payment_receipt(email: str,
                         amount: Optional[float] = None,
                         period_end: Optional[str] = None) -> bool:
    """Payment-success receipt. Best-effort; never raises.

    Args:
        amount     — charged amount in RUPEES (not paise). Razorpay sends paise;
                     the caller converts. None → shown as the plan price label.
        period_end — access-valid-until, an IST ISO string from the subscription
                     entity (or None if unknown).
    """
    amount_str = _format_amount(amount)
    period_str = _format_period_end(period_end)
    return _dispatch("receipt", to=email, fields={
        "email":       email,
        "amount":      amount_str,
        "period_end":  period_str,
    })


def send_payment_failed(email: str) -> bool:
    """Payment-failed / subscription-halted-or-cancelled recovery notice.

    Best-effort; never raises. Sent once per halting/cancellation event (the
    webhook's idempotency gate ensures the event applies once → so this fires
    once too).
    """
    return _dispatch("payment_failed", to=email, fields={"email": email})


# ──────────────────────────────────────────────────────────────────────────
#  Formatting helpers (IST dates, ₹ amounts)
# ──────────────────────────────────────────────────────────────────────────

def _format_amount(amount: Optional[float]) -> str:
    """Render a rupee amount, e.g. 999.0 → '₹999'. None → the plan label."""
    if amount is None:
        return "₹999 (monthly plan)"
    try:
        val = float(amount)
    except (TypeError, ValueError):
        return "₹999 (monthly plan)"
    if val == int(val):
        return f"₹{int(val):,}"
    return f"₹{val:,.2f}"


def _format_period_end(period_end: Optional[str]) -> str:
    """Render the access-valid-until date in IST, human-readable.

    Accepts an ISO-8601 string (what the subscription entity stores). Falls back
    to the raw string if it can't be parsed, and to a generic phrase if None.
    """
    if not period_end:
        return "the end of your current billing cycle"
    try:
        dt = datetime.fromisoformat(period_end)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        return dt.astimezone(IST).strftime("%d %b %Y")
    except (ValueError, TypeError):
        return str(period_end)
