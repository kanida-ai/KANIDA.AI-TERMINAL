"""Standalone Playwright launch probe — invoked by repair_playwright.bat.

Same probe that backend/services/playwright_preflight.py uses on boot.
Keeping the script self-contained (no kanida imports) so it works from any
cwd, even if the backend tree is half-broken during a recovery scenario.

Exit codes:
    0  — browser launched successfully
    2  — playwright module couldn't be imported
    3  — chromium.launch() raised (binary missing, DLL fail, etc.)
"""
from __future__ import annotations

import asyncio
import sys


async def _probe() -> int:
    try:
        from playwright.async_api import async_playwright
    except Exception as e:
        print(f"IMPORT_FAILED: {type(e).__name__}: {e}", file=sys.stderr)
        return 2

    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
                timeout=15_000,
            )
        except Exception as e:
            print(f"LAUNCH_FAILED: {type(e).__name__}: {str(e)[:400]}",
                  file=sys.stderr)
            return 3
        try:
            print("  ok  Chromium launched cleanly. Repair complete.")
            return 0
        finally:
            try:
                await browser.close()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(asyncio.run(_probe()))
