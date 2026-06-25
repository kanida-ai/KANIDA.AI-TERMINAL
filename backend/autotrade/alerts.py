"""Unified alert dispatcher — logging stub.

SMS/push/email integrations plug in here later. For now every alert is logged
at the matching level so smoke testing and prod both have a trace. send_urgent
is what the kill switch uses for MANUAL-EXIT-REQUIRED escalations.
"""
from __future__ import annotations

import logging

log = logging.getLogger("kanida.autotrade.alerts")


def send(message: str, severity: str = "info") -> None:
    level = {"info": logging.INFO, "warn": logging.WARNING,
             "critical": logging.CRITICAL}.get(severity, logging.INFO)
    log.log(level, "[ALERT:%s] %s", severity.upper(), message)


def send_urgent(message: str) -> None:
    log.critical("[ALERT:URGENT] %s", message)
