"""
Generate the Pathfinder half of `docs/openapi.yaml` from the Pydantic contract,
and MERGE it into whatever is already in that file.

Why generate instead of hand-writing YAML: `backend/pathfinder/schemas.py` already
encodes the product laws as validators. Generating the spec from those models makes
contract drift impossible — the spec, the mock, and the server stubs cannot disagree.

Why MERGE instead of overwrite: `docs/openapi.yaml` is the whole product's contract.
Other sessions (01-api-contract's `/api/trader/slice`, and later the Investor,
compliance and creator surfaces) own their own paths in the same file. This script
only ever touches paths under `/api/pathfinder/` and the component schemas the
Pathfinder models generate. Everything else is preserved byte-for-byte in content.

Run:
    python scripts/gen_openapi.py            # write docs/openapi.yaml
    python scripts/gen_openapi.py --check    # fail if the file is out of date

Also refreshes `backend/pathfinder/fixtures/*.json` — raw JSON snapshots of every
mock response, for anyone who wants to serve them from Prism/MSW instead of running
the FastAPI mock.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "backend"))

SPEC_PATH = os.path.join(ROOT, "docs", "openapi.yaml")
FIXTURE_DIR = os.path.join(ROOT, "backend", "pathfinder", "fixtures")

PATH_PREFIX = "/api/pathfinder"

INFO = {
    "title": "KANIDA.AI API",
    "version": "0.1.0",
    "description": (
        "The KANIDA.AI product contract — the seam between the engine track and the app "
        "track.\n\n"
        "Product laws that are enforced *in the schemas*, not merely documented:\n"
        "- **Expectancy is the hero metric**; `win_rate_pct` is optional and never the headline.\n"
        "- **Every return is paired with its drawdown** — a performance block cannot exist "
        "without `max_drawdown_pct` and `current_drawdown_pct`.\n"
        "- **No return promises and no target prices** — there is deliberately no target, "
        "projection or expected-return field anywhere in this contract.\n"
        "- **Every number carries n + date range + data source + cost convention** via "
        "`Provenance`; `sample_flag` is derived (`greyed` below n=20, `flagged` below n=50).\n"
        "- **Point-in-time is law** — every `Provenance` carries an `as_of`.\n"
        "- **Losers first** — ledgers are named `ledger_losers_first` and sorted ascending "
        "by net P&L.\n"
        "- **Backtests are labelled** 'Simulated · Not traded · Not PaRRVA-verified'.\n"
        "- **The LLM never calculates** — narrative text authored by a model contains no "
        "literal numerals, only `{{fact:<id>}}` references resolved against `facts[]`."
    ),
}


def _build_pathfinder_spec() -> dict[str, Any]:
    from fastapi import FastAPI
    from pathfinder.router import router as pathfinder_router

    app = FastAPI(title=INFO["title"], version=INFO["version"])
    app.include_router(pathfinder_router, prefix="/api")
    spec = app.openapi()
    if spec.get("openapi", "").startswith("3.0"):
        raise SystemExit("expected OpenAPI 3.1 from FastAPI; got " + spec.get("openapi", "?"))
    return spec


def _merge(existing: dict[str, Any], generated: dict[str, Any]) -> dict[str, Any]:
    """Replace only the Pathfinder surface; leave every other session's work alone."""
    out: dict[str, Any] = dict(existing) if existing else {}
    out["openapi"] = generated["openapi"]
    out["info"] = INFO

    paths = {k: v for k, v in (out.get("paths") or {}).items() if not k.startswith(PATH_PREFIX)}
    for k, v in (generated.get("paths") or {}).items():
        paths[k] = v
    out["paths"] = dict(sorted(paths.items()))

    components = dict(out.get("components") or {})
    schemas = dict(components.get("schemas") or {})
    schemas.update((generated.get("components") or {}).get("schemas") or {})
    components["schemas"] = dict(sorted(schemas.items()))
    out["components"] = components

    tags = {t["name"]: t for t in (out.get("tags") or [])}
    tags["Pathfinder"] = {
        "name": "Pathfinder",
        "description": (
            "The autonomous research loop: an LLM research brain around a deterministic "
            "quant engine. Read-only. Pathfinder emits research, never an order."
        ),
    }
    out["tags"] = [tags[k] for k in sorted(tags)]
    return out


def _dump_fixtures() -> list[str]:
    from pathfinder import fixture_data as fx
    from pathfinder.schemas import ExperimentStatus

    os.makedirs(FIXTURE_DIR, exist_ok=True)
    written: list[str] = []

    def _w(name: str, model) -> None:
        path = os.path.join(FIXTURE_DIR, name)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(model.model_dump(mode="json"), fh, indent=2, ensure_ascii=False)
            fh.write("\n")
        written.append(os.path.relpath(path, ROOT))

    _w("loop.json", fx.LOOP)
    _w("experiments.json", fx.experiment_list())
    for status in ExperimentStatus:
        _w(f"experiments.{status.value}.json", fx.experiment_list(status))
    for exp_id, detail in fx.EXPERIMENTS.items():
        _w(f"experiment.{exp_id}.json", detail)
    _w("learnings.json", fx.LEARNINGS)
    return written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="fail if docs/openapi.yaml is stale")
    args = ap.parse_args()

    generated = _build_pathfinder_spec()
    existing: dict[str, Any] = {}
    if os.path.exists(SPEC_PATH):
        with open(SPEC_PATH, encoding="utf-8") as fh:
            existing = yaml.safe_load(fh) or {}

    merged = _merge(existing, generated)
    text = yaml.safe_dump(merged, sort_keys=False, allow_unicode=True, width=100)

    if args.check:
        current = open(SPEC_PATH, encoding="utf-8").read() if os.path.exists(SPEC_PATH) else ""
        if current != text:
            print("docs/openapi.yaml is STALE — run: python scripts/gen_openapi.py")
            return 1
        print("docs/openapi.yaml is up to date.")
        return 0

    with open(SPEC_PATH, "w", encoding="utf-8") as fh:
        fh.write(text)
    n_paths = len([p for p in merged["paths"] if p.startswith(PATH_PREFIX)])
    print(f"wrote docs/openapi.yaml — {n_paths} pathfinder paths, "
          f"{len(merged['components']['schemas'])} schemas")
    for rel in _dump_fixtures():
        print(f"  fixture: {rel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
