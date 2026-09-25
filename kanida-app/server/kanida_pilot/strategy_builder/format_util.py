"""Tiny text helpers shared by routes (no I/O)."""
from datetime import date


def dm(iso):
 """'2026-09-29' -> '29 Sep'."""
 try:return date.fromisoformat(str(iso)[:10]).strftime('%d %b').lstrip('0')
 except ValueError:return str(iso)
