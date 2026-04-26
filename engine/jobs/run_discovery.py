from __future__ import annotations

from engine.config import load_config
from engine.discovery.source_discovery import discover_sources
from engine.readers.sqlite_reader import ReadOnlySQLite


def main() -> None:
    config = load_config()
    with ReadOnlySQLite(config["signals_db_path"]) as db:
        discover_sources(db, config["outputs_path"])
    print(f"Discovery complete: {config['outputs_path'] / 'discovery'}")


if __name__ == "__main__":
    main()
