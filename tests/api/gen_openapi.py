# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Generate / regenerate the committed OpenAPI snapshot.

``openapi_snapshot.json`` is a normalized dump of ``create_app().openapi()``.
``tests/api/test_openapi_snapshot.py`` diffs the live schema against it, so any
change to a route, a request/response model, or a per-endpoint error
declaration surfaces as a reviewable diff in that one file.

This is a schema-change REVIEW gate, not a correctness proof: it CANNOT catch a
handler that emits a key its ``response_model`` lacks (the rendered schema is
unchanged) — the golden-body tests (``tests/api/test_golden_*.py``) are that
drop guard.

Regenerate deliberately, after an intended schema change::

    uv run --native-tls -- python -m tests.api.gen_openapi --write
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

SNAPSHOT_PATH = Path(__file__).resolve().parent / "openapi_snapshot.json"


def normalize(schema: dict[str, Any]) -> str:
    """Canonical, diff-stable text form of an OpenAPI document.

    Keys are sorted so a dependency bump that merely reorders dict keys does not
    churn the snapshot; array order (``required``, ``enum``, ``anyOf``, …) is
    preserved because it is semantically meaningful and already deterministic.
    """
    return json.dumps(schema, indent=2, sort_keys=True) + "\n"


def generate() -> str:
    # Imported lazily so ``--help`` doesn't pay the app-import cost.
    from nso_adapter.main import create_app

    return normalize(create_app().openapi())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Regenerate the OpenAPI snapshot.")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Overwrite the committed snapshot with the current schema.",
    )
    args = parser.parse_args(argv)
    current = generate()
    if args.write:
        SNAPSHOT_PATH.write_text(current)
        print(f"wrote {SNAPSHOT_PATH} ({len(current)} bytes)")
    else:
        print(current, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
