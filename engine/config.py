from __future__ import annotations

import json
from pathlib import Path
from typing import Any


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = PACKAGE_ROOT / "config" / "prototype_config.json"


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    cfg_path = Path(path) if path else DEFAULT_CONFIG
    with cfg_path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    project_root = Path(cfg["project_root"])
    signals_db = Path(cfg["signals_db"])
    if not signals_db.is_absolute():
        signals_db = project_root / signals_db
    cfg["signals_db_path"] = signals_db

    out_dir = Path(cfg.get("outputs_dir", "outputs"))
    if not out_dir.is_absolute():
        out_dir = PACKAGE_ROOT / out_dir
    cfg["outputs_path"] = out_dir
    for child in ["discovery", "reports", "logs", "cache"]:
        (out_dir / child).mkdir(parents=True, exist_ok=True)
    return cfg


def market_clause(markets: list[str]) -> tuple[str, list[str]]:
    placeholders = ",".join("?" for _ in markets)
    return f"market in ({placeholders})", markets


def scope_name(scope: str) -> str:
    return scope.upper().replace(" ", "_")
