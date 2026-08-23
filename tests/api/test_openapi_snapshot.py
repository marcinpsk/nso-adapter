# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""S4 schema gates for the auto-generated OpenAPI document.

Three invariants on ``create_app().openapi()``:

1. It matches the committed snapshot (``openapi_snapshot.json``). This is a
   deliberate schema-change REVIEW gate: a change to any route, request/response
   model, or per-endpoint error declaration re-renders the schema and this test
   fails until the snapshot is regenerated on purpose. Framing (plan F8): the
   snapshot CANNOT catch a dropped response field — a handler returning a key
   its model lacks leaves the schema identical; the ``test_golden_*`` bodies are
   that drop guard.
2. No component name is disambiguation-qualified (F8): two distinct models
   sharing a class name make FastAPI/pydantic mint module-qualified keys like
   ``pkg__mod__Clash``, silently splitting what should be one contract type.
   (Our own model names never contain ``__``, so its presence means a collision.)
3. Every internal ``$ref`` resolves to a node in the document (no dangling
   references), across all ``#/components/*`` sections, not just schemas.
"""

from __future__ import annotations

import difflib

import pytest

from nso_adapter.main import create_app
from tests.api.gen_openapi import SNAPSHOT_PATH, normalize


@pytest.fixture(scope="module")
def openapi_schema() -> dict:
    return create_app().openapi()


def _iter_refs(node):
    """Yield every ``$ref`` string reachable in an OpenAPI (sub)document."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "$ref" and isinstance(value, str):
                yield value
            else:
                yield from _iter_refs(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_refs(item)


def _unified_diff(expected: str, current: str, limit: int = 80) -> str:
    lines = list(
        difflib.unified_diff(
            expected.splitlines(keepends=True),
            current.splitlines(keepends=True),
            fromfile="committed snapshot",
            tofile="live schema",
        )
    )
    shown = "".join(lines[:limit])
    if len(lines) > limit:
        shown += f"\n... ({len(lines) - limit} more diff lines)\n"
    return shown


def test_openapi_matches_committed_snapshot(openapi_schema):
    current = normalize(openapi_schema)
    expected = SNAPSHOT_PATH.read_text()
    if current != expected:
        raise AssertionError(
            "OpenAPI schema drifted from the committed snapshot. If the change is "
            "intentional, regenerate it:\n"
            "    uv run --native-tls -- python -m tests.api.gen_openapi --write\n\n" + _unified_diff(expected, current)
        )


def test_generation_actions_document_both_conflict_details(openapi_schema):
    assert openapi_schema["components"]["schemas"]["BarrierActionOut"]["required"] == ["job_id"]

    for action in ("retry-generation", "abandon-generation"):
        operation = openapi_schema["paths"][f"/api/v1/devices/{{device_id}}/actions/{action}"]["post"]
        description = operation["description"]

        assert "error.detail.head_status" in description
        assert "empty detail" in description


def test_no_disambiguation_qualified_component_names(openapi_schema):
    schemas = openapi_schema["components"]["schemas"]
    # A same-name/different-shape collision is the only thing that makes
    # FastAPI/pydantic *introduce* ``__`` into a component key (as a
    # ``<module>__<ClassName>`` disambiguator). Since our own model names never
    # contain ``__``, any ``__`` here signals such a collision. (Pydantic v2's
    # ``-Input``/``-Output`` mode-suffixes are a different, legitimate mechanism
    # and correctly do not trip this gate.)
    qualified = sorted(name for name in schemas if "__" in name)
    assert not qualified, (
        "OpenAPI component names were disambiguation-qualified, meaning two "
        f"distinct models share a class name: {qualified}. Give each response/"
        "request model a globally-unique class name (plan F8)."
    )


def _resolve_pointer(doc, ref: str) -> bool:
    """True iff a local ``#/...`` JSON-pointer ``$ref`` resolves in ``doc`` (RFC 6901)."""
    node = doc
    for raw in ref[2:].split("/"):
        token = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(node, dict):
            if token not in node:
                return False
            node = node[token]
        elif isinstance(node, list):
            if not token.isdigit() or int(token) >= len(node):
                return False
            node = node[int(token)]
        else:
            return False
    return True


def test_all_internal_refs_resolve(openapi_schema):
    internal = {ref for ref in _iter_refs(openapi_schema) if ref.startswith("#/")}
    dangling = sorted(ref for ref in internal if not _resolve_pointer(openapi_schema, ref))
    assert not dangling, f"OpenAPI $ref targets not present in the document: {dangling}"


def test_normalize_masks_the_release_version(openapi_schema):
    # semantic-release bumps the package version each release; the review gate
    # must not fail on that bump (it did once: v0.2.0 broke main's CI).
    bumped = {**openapi_schema, "info": {**openapi_schema["info"], "version": "99.99.99"}}
    assert normalize(bumped) == normalize(openapi_schema)
