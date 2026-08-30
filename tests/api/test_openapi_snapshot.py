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


def test_generation_actions_document_the_generation_cas(openapi_schema):
    schemas = openapi_schema["components"]["schemas"]
    assert schemas["BarrierActionIn"]["required"] == ["generation_id"]
    assert schemas["BarrierActionOut"]["required"] == ["generation_id", "seq", "job_id"]

    for action in ("retry-generation", "abandon-generation"):
        operation = openapi_schema["paths"][f"/api/v1/devices/{{device_id}}/actions/{action}"]["post"]
        description = operation["description"]

        assert operation["requestBody"] == {
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/BarrierActionIn"},
                }
            },
            "required": True,
        }
        assert operation["responses"]["409"]["content"]["application/json"]["schema"] == {
            "$ref": "#/components/schemas/ErrorEnvelope"
        }
        assert "error.detail.head_status" in description
        assert "error.detail.head_generation_id" in description


def test_action_apply_requires_skipped_detail(openapi_schema):
    schema = openapi_schema["components"]["schemas"]["ActionApplyOut"]

    assert "skipped_detail" in schema["required"]
    assert {"type": "null"} in schema["properties"]["skipped_detail"]["anyOf"]


def test_action_apply_requires_an_attempt_id(openapi_schema):
    schema = openapi_schema["components"]["schemas"]["ActionApplyIn"]

    assert schema["additionalProperties"] is False
    assert schema["required"] == ["apply_attempt_id", "selected"]
    assert schema["properties"]["apply_attempt_id"] == {
        "format": "uuid",
        "title": "Apply Attempt Id",
        "type": "string",
    }


def test_api_contract_documents_the_generation_action_cas():
    contract = (SNAPSHOT_PATH.parents[2] / "docs" / "api-contract.md").read_text()
    actions_table = contract.split("### Execution and admission", maxsplit=1)[1].split(
        "### `POST /api/v1/devices/{id}/actions/sync`",
        maxsplit=1,
    )[0]
    moved_head_sentence = (
        "The request names the generation to act on; when it is not the current head, the 409 "
        "carries `error.detail.head_generation_id` naming the head."
    )
    for action in ("retry-generation", "abandon-generation"):
        row = next(line for line in actions_table.splitlines() if f"`actions/{action}`" in line)
        assert moved_head_sentence in row

    section = contract.split(
        "### `POST /api/v1/devices/{id}/actions/{retry,abandon}-generation`",
        maxsplit=1,
    )[1].split("\n### ", maxsplit=1)[0]
    section = " ".join(section.split())

    assert '`{ "generation_id": <int> }`' in section
    assert '`202 { "generation_id": <int>, "seq": <int>, "job_id": <int|null> }`' in section
    assert "error.detail.head_status" in section
    assert "error.detail.head_generation_id" in section


def test_api_contract_documents_the_skipped_detail_null_shape():
    contract = (SNAPSHOT_PATH.parents[2] / "docs" / "api-contract.md").read_text()
    section = contract.split("### `POST /api/v1/devices/{id}/actions/apply`", maxsplit=1)[1].split(
        "\n### ", maxsplit=1
    )[0]

    assert "only its CONTENT is conditional" in section
    assert "The value is\n`null` when no member qualifies" in section
    assert '"skipped_detail": null' in section


def test_api_contract_documents_apply_attempt_identity_and_recovery():
    contract = (SNAPSHOT_PATH.parents[2] / "docs" / "api-contract.md").read_text()
    section = contract.split("### `POST /api/v1/devices/{id}/actions/apply`", maxsplit=1)[1].split(
        "\n### ", maxsplit=1
    )[0]
    section = " ".join(section.split())

    assert '"apply_attempt_id": "8a2c9231-7ad8-4b17-a4b8-f5b4df745dd8"' in section
    assert "`apply_attempt_id` is required" in section
    assert "same device and complete canonical `selected`" in section
    assert "stored HTTP status and response body byte for byte" in section
    assert "`error.detail.mismatch` is `device_id` or `selected`" in section
    assert "re-POST the identical request with the same UUID and selection" in section
    assert "No revision field is accepted" in section


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


def test_action_apply_sequence_schema_preserves_the_exact_receipt_bound(openapi_schema):
    from nso_adapter.core.request_flags import MAX_PUSH_SEQ, MIN_PUSH_SEQ

    selected = openapi_schema["components"]["schemas"]["ActionApplyIn"]["properties"]["selected"]
    sequence = selected["additionalProperties"]

    assert sequence["minimum"] == MIN_PUSH_SEQ
    assert sequence["maximum"] == MAX_PUSH_SEQ
    assert isinstance(sequence["minimum"], int)
    assert isinstance(sequence["maximum"], int)


def test_action_apply_documents_its_internal_error_envelope(openapi_schema):
    responses = openapi_schema["paths"]["/api/v1/devices/{device_id}/actions/apply"]["post"]["responses"]

    assert responses["500"] == {
        "description": "Internal adapter invariant failed",
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/ErrorEnvelope"},
            }
        },
    }


def test_action_abandon_documents_the_successor_carrier_identity(openapi_schema):
    operation = openapi_schema["paths"]["/api/v1/devices/{device_id}/actions/abandon-generation"]["post"]

    assert "successor carrier" in operation["description"]
    assert "null" in operation["description"]


def test_action_conflict_descriptions_match_their_admission_rules(openapi_schema):
    queued_action = "A job of the requested type is already queued for this device"
    trigger_paths = (
        "/api/v1/devices/{device_id}/actions/sync",
        "/api/v1/devices/{device_id}/actions/sync-from-nso",
        "/api/v1/devices/{device_id}/actions/detect-drift",
        "/api/v1/devices/{device_id}/actions/connect",
        "/api/v1/devices/{device_id}/sync-notify",
    )
    for path in trigger_paths:
        assert openapi_schema["paths"][path]["post"]["responses"]["409"]["description"] == queued_action

    apply = openapi_schema["paths"]["/api/v1/devices/{device_id}/actions/apply"]["post"]
    assert apply["responses"]["409"]["description"] == (
        "The Apply UUID identifies a different request, a job is already queued or running, "
        "or a selected stream cannot be applied faithfully"
    )


def test_device_generation_limit_schema_matches_the_runtime_bounds(openapi_schema):
    from nso_adapter.api.pagination import LIMIT_MAX, LIMIT_MIN

    parameters = openapi_schema["paths"]["/api/v1/devices/{device_id}/generations"]["get"]["parameters"]
    limit = next(parameter for parameter in parameters if parameter["name"] == "limit")

    assert limit["schema"]["minimum"] == LIMIT_MIN
    assert limit["schema"]["maximum"] == LIMIT_MAX


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
