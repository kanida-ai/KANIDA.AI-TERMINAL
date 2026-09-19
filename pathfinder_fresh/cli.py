from __future__ import annotations

import argparse
from pathlib import Path
import sys

from .engine import PathfinderEngine
from .models import Policy


def parser() -> argparse.ArgumentParser:
    root = Path(__file__).resolve().parents[1]
    command = argparse.ArgumentParser(description="Fresh Pathfinder deterministic research engine")
    command.add_argument("--db", default=str(root / "db" / "kanida.db"), help="Read-only OHLCV SQLite database")
    command.add_argument("--state", default=str(Path(__file__).resolve().parent / "output" / "pathfinder_registry.db"))
    command.add_argument("--output", default=str(Path(__file__).resolve().parent / "output"))
    command.add_argument("--as-of", default="latest")
    command.add_argument("--replay-from", help="Build public grading and experiment memory from this date")
    command.add_argument("--edition-every", type=int, default=5, help="Replay edition cadence in market sessions")
    command.add_argument("--threshold", type=float, default=62.0, help="Usefulness publication threshold")
    return command


def main(argv: list[str] | None = None) -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass
    args = parser().parse_args(argv)
    policy = Policy(publication_threshold=args.threshold)
    engine = PathfinderEngine(args.db, args.state, policy=policy, through=None if args.as_of == "latest" else args.as_of)
    try:
        if args.replay_from:
            engine.replay(args.replay_from, args.as_of, args.edition_every)
        else:
            engine.run_edition(args.as_of)
        paths = engine.write_outputs(args.output)
        print(paths["report"].read_text(encoding="utf-8"))
        print("OUTPUT FILES")
        for name, path in paths.items():
            print(f"{name}: {path}")
        return 0
    finally:
        engine.close()


if __name__ == "__main__":
    sys.exit(main())
