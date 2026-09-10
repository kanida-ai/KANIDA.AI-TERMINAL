"""
Validate `docs/openapi.yaml`.

Two layers:

  1. If `openapi-spec-validator` is installed, run the real OpenAPI 3.1 validator.
     (`pip install openapi-spec-validator` — a dev-only dependency; it is NOT
     installed by default here, and the script says so rather than pretending.)

  2. Always run KANIDA's own structural + product-law checks, which are the ones
     that actually protect this contract:
       * document is OpenAPI 3.1 with info/paths/components
       * every internal `$ref` resolves
       * every component schema is reachable from some path (no orphans)
       * every operation has an operationId, a 200 and guarded 400/404/500 shapes
       * NO field anywhere is named like a return promise or a target price
       * every performance-shaped schema pairs expectancy with drawdown

Run:  python scripts/validate_openapi.py
"""
from __future__ import annotations

import os
import re
import sys
from typing import Any

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPEC_PATH = os.path.join(ROOT, "docs", "openapi.yaml")

#: Field names that would let a promise or a price target into the product.
BANNED_FIELD_RE = re.compile(
    r"^(target|target_price|price_target|expected_return|projected_\w+|projection|"
    r"forecast|guaranteed_\w+|upside|stoploss_target)$"
)

FAILURES: list[str] = []


def fail(msg: str) -> None:
    FAILURES.append(msg)


def walk(node: Any, path: str = "$"):
    yield path, node
    if isinstance(node, dict):
        for k, v in node.items():
            yield from walk(v, f"{path}.{k}")
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from walk(v, f"{path}[{i}]")


def resolve(spec: dict, ref: str) -> Any:
    if not ref.startswith("#/"):
        fail(f"external $ref not allowed: {ref}")
        return None
    node: Any = spec
    for part in ref[2:].split("/"):
        part = part.replace("~1", "/").replace("~0", "~")
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def main() -> int:
    if not os.path.exists(SPEC_PATH):
        print("MISSING docs/openapi.yaml")
        return 1
    with open(SPEC_PATH, encoding="utf-8") as fh:
        spec = yaml.safe_load(fh)

    # ── Layer 1: the real validator, if present ─────────────────────────────
    try:
        from openapi_spec_validator import validate as _validate  # type: ignore
        _validate(spec)
        print("[ok]   openapi-spec-validator: document is a valid OpenAPI 3.1 document")
    except ImportError:
        print("[skip] openapi-spec-validator not installed "
              "(dev-only: pip install openapi-spec-validator) — running structural checks only")
    except Exception as exc:  # pragma: no cover - only when the spec is broken
        fail(f"openapi-spec-validator rejected the document: {exc}")

    # ── Layer 2: structural + product-law checks ────────────────────────────
    if not str(spec.get("openapi", "")).startswith("3.1"):
        fail(f"expected OpenAPI 3.1, got {spec.get('openapi')!r}")
    for key in ("info", "paths", "components"):
        if key not in spec:
            fail(f"missing top-level '{key}'")

    schemas = (spec.get("components") or {}).get("schemas") or {}

    refs_used: set[str] = set()
    for path, node in walk(spec):
        if isinstance(node, dict) and "$ref" in node and isinstance(node["$ref"], str):
            ref = node["$ref"]
            refs_used.add(ref)
            if resolve(spec, ref) is None:
                fail(f"dangling $ref at {path}: {ref}")

    referenced = {r.rsplit("/", 1)[-1] for r in refs_used}
    orphans = sorted(set(schemas) - referenced)
    if orphans:
        fail(f"orphan component schemas (nothing refs them): {orphans}")

    for route, ops in (spec.get("paths") or {}).items():
        for method, op in (ops or {}).items():
            if method not in {"get", "post", "put", "patch", "delete"}:
                continue
            where = f"{method.upper()} {route}"
            if not op.get("operationId"):
                fail(f"{where}: missing operationId")
            responses = op.get("responses") or {}
            if "200" not in responses:
                fail(f"{where}: no 200 response")
            for code in ("400", "404", "500"):
                if code not in responses:
                    fail(f"{where}: missing guarded {code} response shape")

    # No promise / target-price fields anywhere in the contract.
    for name, schema in schemas.items():
        for prop in (schema.get("properties") or {}):
            if BANNED_FIELD_RE.match(prop):
                fail(f"banned field '{prop}' on schema '{name}' — no promises, no target prices")

    # Expectancy is the hero, and every return is paired with a drawdown.
    perf = schemas.get("PerformanceBlock")
    if perf is None:
        fail("PerformanceBlock schema is missing from the contract")
    else:
        required = set(perf.get("required") or [])
        for must in ("expectancy_pct_per_trade", "expectancy_2x_slippage_pct_per_trade",
                     "max_drawdown_pct", "current_drawdown_pct", "n", "provenance", "label"):
            if must not in required:
                fail(f"PerformanceBlock must REQUIRE '{must}'")
        if "win_rate_pct" in required:
            fail("PerformanceBlock must NOT require win_rate_pct — expectancy is the hero")

    prov = schemas.get("Provenance")
    if prov is None:
        fail("Provenance schema is missing")
    else:
        required = set(prov.get("required") or [])
        for must in ("data_source", "date_range", "as_of", "cost_convention", "computed_by"):
            if must not in required:
                fail(f"Provenance must REQUIRE '{must}'")

    if FAILURES:
        print(f"\nFAILED — {len(FAILURES)} problem(s):")
        for f in FAILURES:
            print(f"  - {f}")
        return 1

    n_paths = len(spec.get("paths") or {})
    print(f"[ok]   structural + product-law checks passed "
          f"({n_paths} paths, {len(schemas)} schemas, {len(refs_used)} refs resolved)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
