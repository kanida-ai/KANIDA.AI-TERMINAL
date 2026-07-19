"""LLM correlation (wrapped) + a deterministic fallback that ALWAYS works.

The orchestrator asks: "given these subsystem HealthSignals, what is the ONE
root incident?" — so a broker-token expiry that is ALSO causing entry failures
and latency alerts pages ONCE ("token expired → entries failing"), not three
times.

TWO PATHS, and the deterministic one is the floor:
  * correlate_llm(signals)  — a WRAPPED Anthropic call. On ANY failure (no key,
    package missing, timeout, HTTP error, unparseable JSON) it returns None and
    the caller falls back. The health layer must NEVER go blind because the LLM
    is down.
  * correlate_deterministic(signals) — pure code: worst-status wins, known causal
    chains collapse, a templated page. This runs when the LLM is unavailable AND
    is what the tests assert against for the LLM-down case.

SECURITY: sanitize_signals() is the ONLY thing sent to the LLM. It strips every
metric whose KEY looks secret/PII (token/secret/key/password/proxy/url/ip/email/
auth/cred) and truncates strings, so a token/credential/egress-IP can never reach
the model. Only subsystem, status, summary, and scrubbed numeric/bool/short-text
metrics are forwarded.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

from .signals import Status, rank

log = logging.getLogger("kanida.sysagents.llm")

# Keys whose VALUE may be a secret / credential / PII — dropped before the LLM.
_DENY_KEY_SUBSTRINGS = (
    "token", "secret", "key", "password", "passwd", "proxy", "url", "ip",
    "email", "auth", "cred", "session_id", "user_id", "account_id",
)

_DEFAULT_MODEL = "claude-haiku-4-5-20251001"


def _model() -> str:
    return os.environ.get("SYSAGENTS_LLM_MODEL", _DEFAULT_MODEL).strip() \
        or _DEFAULT_MODEL


def _scrub_value(v: Any) -> Any:
    """Keep numbers/bools/None; truncate strings; drop nested structures to a
    short repr (metrics are meant to be flat)."""
    if isinstance(v, (int, float, bool)) or v is None:
        return v
    if isinstance(v, str):
        return v[:120]
    # A list/dict metric is not expected; forward a bounded, type-only hint.
    return f"<{type(v).__name__}>"


def sanitize_signals(signals: List[Any]) -> List[Dict[str, Any]]:
    """Return LLM-safe signal dicts: subsystem/status/summary + scrubbed metrics
    (secret/PII keys removed). Accepts HealthSignal objects or their dicts."""
    out: List[Dict[str, Any]] = []
    for s in signals:
        d = s.to_dict() if hasattr(s, "to_dict") else dict(s)
        raw_metrics = d.get("metrics") or {}
        safe_metrics: Dict[str, Any] = {}
        for k, v in raw_metrics.items():
            kl = str(k).lower()
            if any(bad in kl for bad in _DENY_KEY_SUBSTRINGS):
                continue
            safe_metrics[str(k)[:40]] = _scrub_value(v)
        out.append({
            "subsystem": str(d.get("subsystem"))[:40],
            "status": str(d.get("status")),
            "summary": str(d.get("summary") or "")[:200],
            "metrics": safe_metrics,
        })
    return out


# ── Deterministic correlation (the always-available floor) ───────────────────
# Known causal chains: if the CAUSE subsystem is non-OK and an EFFECT subsystem is
# non-OK in the same batch, they collapse into one incident rooted at the cause.
_CAUSAL_CHAINS = [
    # (cause, [effects], root-cause template)
    ("broker-health", ["execution-quality", "market-data", "risk-rms"],
     "broker connectivity/token degraded → downstream execution & data impact"),
    ("data-freshness", ["trading-stats"],
     "stale signal data → trading-stats unreliable"),
    ("platform-health", ["market-data", "execution-quality", "queue-latency"],
     "platform resource pressure → latency/throughput impact"),
]

# Priority order for choosing the root when no chain matches (earlier = rootier).
_ROOT_PRIORITY = [
    "broker-health", "data-freshness", "platform-health", "market-data",
    "execution-quality", "risk-rms", "queue-latency", "trading-stats",
    "agent-watcher",
]


def _worst_status(signals: List[Dict[str, Any]]) -> str:
    best = Status.OK
    for s in signals:
        st = s.get("status")
        if rank(st) > rank(best):
            best = st
    # If everything was UNKNOWN/NA/OK, reflect that honestly.
    return best


def correlate_deterministic(signals: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Pure-code correlation. Returns an incident dict:
        {severity, root_cause, summary, impacted, source='deterministic'}.
    Collapses known cause→effect chains into one incident; else roots at the
    highest-priority non-OK subsystem. Never raises."""
    non_ok = [s for s in signals
              if s.get("status") in (Status.WARN, Status.ALERT, Status.CRITICAL)]
    overall = _worst_status(signals)
    if not non_ok:
        return {
            "severity": overall if overall in (Status.OK, Status.WARN) else Status.OK,
            "root_cause": "all subsystems nominal",
            "summary": "All monitored subsystems are OK (or not-applicable).",
            "impacted": [],
            "source": "deterministic",
        }
    non_ok_names = {s["subsystem"] for s in non_ok}
    # 1. Causal-chain collapse.
    for cause, effects, template in _CAUSAL_CHAINS:
        if cause in non_ok_names and any(e in non_ok_names for e in effects):
            impacted = [cause] + [e for e in effects if e in non_ok_names]
            sev = max((s["status"] for s in non_ok if s["subsystem"] in impacted),
                      key=rank)
            cause_sig = next(s for s in non_ok if s["subsystem"] == cause)
            return {
                "severity": sev,
                "root_cause": template,
                "summary": (f"Correlated incident rooted at {cause} "
                            f"({cause_sig.get('summary')}). Impacted: "
                            f"{', '.join(impacted)}."),
                "impacted": impacted,
                "source": "deterministic",
            }
    # 2. No chain — root at the highest-priority non-OK subsystem.
    def _prio(name: str) -> int:
        return _ROOT_PRIORITY.index(name) if name in _ROOT_PRIORITY else 999
    root = sorted(non_ok, key=lambda s: (_prio(s["subsystem"]), -rank(s["status"])))[0]
    impacted = sorted(non_ok_names, key=_prio)
    sev = max((s["status"] for s in non_ok), key=rank)
    return {
        "severity": sev,
        "root_cause": f"{root['subsystem']}: {root.get('summary')}",
        "summary": (f"{len(non_ok)} subsystem(s) non-OK; worst is "
                    f"{root['subsystem']} ({root['status']}). "
                    f"Impacted: {', '.join(impacted)}."),
        "impacted": impacted,
        "source": "deterministic",
    }


# ── LLM correlation (wrapped; None on any failure) ───────────────────────────
_SYSTEM_PROMPT = (
    "You are a platform-health incident correlator for a trading platform's "
    "observability layer. You are given a JSON list of subsystem health signals "
    "(subsystem, status in OK/WARN/ALERT/CRITICAL/UNKNOWN/NA, a summary, and "
    "sanitized metrics). Correlate them into exactly ONE root incident: identify "
    "the most likely single root cause and which downstream subsystems its effects "
    "explain, so operators get one page, not many. Do NOT invent metrics or "
    "numbers not present. Do NOT take or suggest any corrective action. Respond "
    "with ONLY a JSON object: {\"severity\": one of OK|WARN|ALERT|CRITICAL, "
    "\"root_cause\": short string, \"impacted\": list of subsystem names, "
    "\"summary\": a concise human-readable page body}."
)


def correlate_llm(signals: List[Dict[str, Any]],
                  timeout_sec: float = 12.0) -> Optional[Dict[str, Any]]:
    """WRAPPED Anthropic correlation. `signals` MUST already be sanitized. Returns
    the incident dict (source='llm') or None on ANY failure (caller falls back).
    Never raises. Never sends anything but the sanitized signals."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        log.debug("sysagents.llm: no ANTHROPIC_API_KEY — deterministic fallback")
        return None
    try:
        import anthropic  # noqa: WPS433
    except Exception as e:  # noqa: BLE001
        log.debug("sysagents.llm: anthropic import failed (%s) — fallback", e)
        return None
    try:
        client = anthropic.Anthropic(api_key=api_key, timeout=timeout_sec)
        msg = client.messages.create(
            model=_model(),
            max_tokens=700,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user",
                       "content": json.dumps({"signals": signals})}],
        )
        text = "".join(
            getattr(block, "text", "") for block in (msg.content or []))
        incident = _parse_incident(text)
        if incident is None:
            log.debug("sysagents.llm: unparseable response — fallback")
            return None
        incident["source"] = "llm"
        return incident
    except Exception as e:  # noqa: BLE001 — never let the LLM blind the layer
        log.warning("sysagents.llm: correlate_llm failed (%s) — fallback",
                    type(e).__name__)
        return None


def _parse_incident(text: str) -> Optional[Dict[str, Any]]:
    """Extract the JSON incident object from the model text; validate fields."""
    if not text:
        return None
    s = text.strip()
    # Tolerate a fenced ```json block.
    if "```" in s:
        parts = s.split("```")
        for p in parts:
            p = p.strip()
            if p.startswith("json"):
                p = p[4:].strip()
            if p.startswith("{"):
                s = p
                break
    start, end = s.find("{"), s.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        obj = json.loads(s[start:end + 1])
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(obj, dict):
        return None
    sev = str(obj.get("severity") or "").upper()
    if sev not in (Status.OK, Status.WARN, Status.ALERT, Status.CRITICAL):
        sev = Status.WARN
    impacted = obj.get("impacted")
    if not isinstance(impacted, list):
        impacted = []
    return {
        "severity": sev,
        "root_cause": str(obj.get("root_cause") or "")[:400],
        "summary": str(obj.get("summary") or "")[:2000],
        "impacted": [str(x)[:40] for x in impacted][:12],
    }


def correlate(signals: List[Any]) -> Dict[str, Any]:
    """Public entry: sanitize → try LLM → deterministic fallback. Always returns
    an incident dict with a `source` of 'llm' or 'deterministic'. Never raises."""
    safe = sanitize_signals(signals)
    incident = correlate_llm(safe)
    if incident is None:
        incident = correlate_deterministic(safe)
    incident.setdefault("impacted", [])
    return incident
