# SPDX-License-Identifier: Apache-2.0
"""Closed error-code set + envelope contract for every non-2xx response.

api-contract.md promises ONE error shape for all errors:
    {"error": {"code": "<closed set>", "message": "...", "detail": {}}}
Historically FastAPI's own request-validation failures leaked its default
``{"detail": [...]}`` shape instead — a latent contract violation. These tests
pin the envelope on that path too (the one deliberate wire change of the
OpenAPI-truthfulness program) and enforce the closed code set mechanically:
call sites ⊆ ERROR_CODES ⊆ api-contract.md.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from pathlib import Path
from types import NoneType, UnionType
from typing import Annotated, TypeAliasType, Union, get_args, get_origin

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from pydantic import BaseModel, Field, SecretStr, field_validator

from nso_adapter.api.errors import ERROR_CODES, ErrorCode, api_error
from tests._secret_discipline import assert_records_free_of, assert_text_free_of
from tests.conftest import VALID_TOKEN, push_seq

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PKG_DIR = _REPO_ROOT / "nso_adapter"
_CONTRACT_DOC = _REPO_ROOT / "docs" / "api-contract.md"
#: Stands for the list index or map key a location gains when it passes through a container.
_PER_REQUEST_SEGMENT = "<index>"

type _CallerKeyedMapAlias = dict[str, SecretStr]
type _CyclicCallerKeyedMapAlias = _CyclicCallerKeyedMapAlias


def _unwrap(annotation: object) -> object | None:
    """Strip the wrappers pydantic reports THROUGH, or return None when it reports a segment.

    ``Annotated`` and ``X | None`` keep the location unchanged. A wider union makes pydantic
    tag the failing member, so the location gains a segment this predicate cannot name.
    """

    def strip(candidate: object, seen_aliases: frozenset[int]) -> object | None:
        if isinstance(candidate, TypeAliasType):
            identity = id(candidate)
            if identity in seen_aliases:
                return None
            return strip(candidate.__value__, seen_aliases | {identity})

        origin = get_origin(candidate)
        arguments = get_args(candidate)
        if origin is Annotated:
            return strip(arguments[0], seen_aliases) if arguments else None
        if origin in (Union, UnionType) and len(arguments) == 2 and NoneType in arguments:
            wrapped = arguments[0] if arguments[1] is NoneType else arguments[1]
            return strip(wrapped, seen_aliases)
        return candidate

    return strip(annotation, frozenset())


def _is_caller_keyed_map_annotation(annotation: object) -> bool:
    """Recognize a string-keyed map after removing validation-path-transparent wrappers.

    The VALUE type is irrelevant: pydantic puts the caller's key in the location whether the
    entry is a secret, an int or a nested model. A bare ``dict`` counts too: JSON object keys
    are strings, so it is the same shape with the parameters left off.
    """
    candidate = _unwrap(annotation)
    if candidate is dict:
        return True
    return get_origin(candidate) is dict and get_args(candidate)[:1] == (str,)


def _api_routes(routes: Iterable[object]) -> Iterator[APIRoute]:
    """Every APIRoute, including the ones an include_router() wrapper holds."""
    for route in routes:
        if isinstance(route, APIRoute):
            yield route
        included = getattr(route, "original_router", None)
        if included is not None:
            yield from _api_routes(included.routes)


def _models_in(annotation: object) -> Iterator[type[BaseModel]]:
    """Every BaseModel an annotation carries, at any depth of its type arguments."""
    candidate = _unwrap(annotation)
    if isinstance(candidate, type) and issubclass(candidate, BaseModel):
        yield candidate
        return
    for argument in get_args(candidate):
        yield from _models_in(argument)


def _request_body_models(app: FastAPI) -> set[type[BaseModel]]:
    """The models a caller's request body is validated against."""
    return {
        model
        for route in _api_routes(app.routes)
        for parameter in route.dependant.body_params
        for model in _models_in(parameter.field_info.annotation)
    }


def _caller_keyed_locations(models: Iterable[type[BaseModel]]) -> tuple[set[tuple[str, ...]], set[str]]:
    """The body locations whose next segment is a caller-chosen key.

    Returns the ones that can be written as a literal prefix, and separately the ones that
    cannot: a map under a list or a map entry sits behind an index or key that varies per
    request, so ``DYNAMIC_KEY_LOCATIONS`` has no way to name it. Reporting those instead of
    dropping them keeps the derivation from becoming a prefix that silently never matches.
    """
    named: set[tuple[str, ...]] = set()
    unnameable: set[str] = set()

    def walk(model: type[BaseModel], prefix: tuple[str, ...], nameable: bool, seen: frozenset[type[BaseModel]]) -> None:
        if model in seen:
            return
        for name, field in model.model_fields.items():
            path = (*prefix, name)
            if _is_caller_keyed_map_annotation(field.annotation):
                if nameable:
                    named.add(path)
                else:
                    unnameable.add(".".join(path))
            unwrapped = _unwrap(field.annotation)
            for nested in _models_in(field.annotation):
                # A model the annotation IS gets its own field name; one it merely contains
                # sits behind a segment (an index, a map key) this set cannot spell.
                direct = nested is unwrapped
                walk(nested, path if direct else (*path, _PER_REQUEST_SEGMENT), nameable and direct, seen | {model})

    for model in models:
        walk(model, ("body",), True, frozenset())
    return named, unnameable


# ---------------------------------------------------------------- envelope on 422


async def test_request_validation_error_uses_envelope(adapter_client):
    """A Pydantic body-validation failure must return the documented envelope,
    not FastAPI's default ``{"detail": [...]}`` shape."""
    resp = await adapter_client.post(
        "/api/v1/devices",
        json={"netbox_device_id": "not-an-int"},
        headers=AUTH,
    )
    assert resp.status_code == 422
    body = resp.json()
    assert set(body) == {"error"}, f"not the envelope: {body}"
    err = body["error"]
    assert err["code"] == "validation_error"
    assert isinstance(err["message"], str) and err["message"]
    # the pydantic error list rides inside detail (encoder-safe)
    assert isinstance(err["detail"], dict)
    assert isinstance(err["detail"]["errors"], list) and err["detail"]["errors"]


async def test_validation_error_with_non_primitive_ctx(adapter_client):
    """A validator raising ValueError puts the exception object into the pydantic
    error ``ctx`` — the handler must encode it (jsonable_encoder), not 500."""
    resp = await adapter_client.put(
        "/api/v1/devices/1/snmp-intent",
        json={
            "communities": [
                {"name": "public", "vault_ref": "not-a-valid-triple"},
            ]
        },
        headers=AUTH | push_seq(),
    )
    assert resp.status_code == 422, resp.text
    err = resp.json()["error"]
    assert err["code"] == "validation_error"
    # every leaf of the encoded error list must be JSON-native (it round-tripped)
    assert isinstance(err["detail"]["errors"], list)


async def test_validation_error_does_not_echo_validator_text():
    """A validator can include submitted data in its exception text, so the handler must not."""
    from fastapi import FastAPI
    from fastapi.exceptions import RequestValidationError
    from httpx import ASGITransport, AsyncClient

    from nso_adapter.api.errors import validation_error_handler

    secret = "operator-supplied-secret"

    class SecretBody(BaseModel):
        value: str

        @field_validator("value")
        @classmethod
        def reject(cls, value: str) -> str:
            raise ValueError(f"rejected {value}")

    app = FastAPI()
    app.add_exception_handler(RequestValidationError, validation_error_handler)

    async def reject_secret(body):
        return body

    reject_secret.__annotations__["body"] = SecretBody
    app.post("/_test/validation-secret")(reject_secret)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/_test/validation-secret", json={"value": secret})

    assert_text_free_of(response.text, [secret])
    assert response.status_code == 422
    assert response.json()["error"]["detail"]["errors"] == [
        {"type": "value_error", "loc": ["body", "value"], "msg": "Invalid value"}
    ]


# ------------------------------------------------------- envelope on an unexpected 500


async def test_an_unhandled_exception_uses_the_envelope_and_never_echoes_the_exception():
    """The catch-all: an unexpected failure anywhere still answers the documented shape.

    Its text is deliberately generic. An exception raised deep in a dependency routinely
    carries the credential (or the URL, or the row) it failed on, and a 500 body is the one
    place nobody inspects before it reaches a log aggregator — so nothing from the exception
    crosses the wire, and the adapter's own log line carries safe metadata only.

    The DEFAULT transport is the assertion: the outermost middleware answers and re-raises
    nothing, so no exception escapes the ASGI app for a server to log a raw traceback from.
    The redacted ``where`` frames are the whole diagnostic remainder.
    """
    from httpx import ASGITransport, AsyncClient
    from structlog.testing import capture_logs

    from nso_adapter.main import create_app

    secret = "s3cr3t-vault-token"
    app = create_app()

    @app.get("/_test/boom")
    async def _boom():
        raise RuntimeError(f"vault login failed with token {secret}")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        with capture_logs() as logs:
            resp = await client.get("/_test/boom")

    assert_text_free_of(resp.text, [secret])
    assert_records_free_of(logs, [secret])
    assert resp.status_code == 500
    assert resp.json() == {"error": {"code": "internal", "message": "Internal server error", "detail": {}}}

    (record,) = [log for log in logs if log["event"] == "api.unhandled_exception"]
    assert record["exception_type"] == "RuntimeError"
    assert not record.get("exc_info"), "the raw exception reaches the log renderer"

    # Locations only — the frames must name where it broke without quoting anything from it.
    where = record["where"]
    assert 0 < len(where) <= 5, where
    assert where[-1].endswith(" in _boom"), where
    assert not any(secret in frame for frame in where)


async def test_a_specific_handler_still_wins_over_the_catch_all():
    """The catch-all is the LAST resort: a raised ApiError keeps its own status and code."""
    from httpx import ASGITransport, AsyncClient

    from nso_adapter.main import create_app

    app = create_app()

    @app.get("/_test/conflict")
    async def _conflict():
        raise api_error(409, "conflict", "a queued action already exists", {"device_id": 7})

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/_test/conflict")

    assert resp.status_code == 409
    assert resp.json() == {
        "error": {"code": "conflict", "message": "a queued action already exists", "detail": {"device_id": 7}}
    }


@pytest.mark.parametrize(
    ("method", "path", "status_code", "code", "message"),
    [
        ("GET", "/api/v1/route-that-does-not-exist", 404, "not_found", "Not Found"),
        ("TRACE", "/api/v1/devices", 405, "method_not_allowed", "Method Not Allowed"),
    ],
)
async def test_framework_http_errors_use_the_canonical_envelope(
    adapter_client, method, path, status_code, code, message
):
    response = await adapter_client.request(method, path, headers=AUTH)

    assert response.status_code == status_code
    assert response.json() == {"error": {"code": code, "message": message, "detail": {}}}
    if status_code == 405:
        assert response.headers["allow"]


async def test_promotion_provenance_handler_uses_closed_error_factory(monkeypatch):
    from fastapi import Request

    from nso_adapter.api import errors
    from nso_adapter.core.receipt import PromotionProvenanceUnexecutable

    monkeypatch.setattr(errors, "ERROR_CODES", errors.ERROR_CODES - {"apply_unexecutable"})

    with pytest.raises(ValueError, match="unknown error code 'apply_unexecutable'"):
        await errors.promotion_provenance_handler(
            Request({"type": "http"}),
            PromotionProvenanceUnexecutable("vlan"),
        )


async def test_promotion_provenance_error_is_dispatched_through_the_application():
    from fastapi import Depends
    from httpx import ASGITransport, AsyncClient

    from nso_adapter.api.deps import verify_token
    from nso_adapter.core.receipt import PromotionProvenanceUnexecutable
    from nso_adapter.main import create_app

    app = create_app()
    app.state.adapter_token = VALID_TOKEN

    @app.get("/_test/promotion-provenance", dependencies=[Depends(verify_token)])
    async def _promotion_provenance():
        raise PromotionProvenanceUnexecutable("vlan")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        unauthenticated = await client.get("/_test/promotion-provenance")
        response = await client.get("/_test/promotion-provenance", headers=AUTH)

    assert unauthenticated.status_code == 401
    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "apply_unexecutable",
            "message": (
                "Push cannot promote outstanding deletion provenance for vlan. "
                "Apply the stored receipt when vlan is document-executed, then retry this push"
            ),
            "detail": {"streams": {"vlan": "outstanding_deletion_provenance"}},
        }
    }


# ---------------------------------------------------------------- closed set


def test_api_error_rejects_unknown_code():
    with pytest.raises(ValueError) as exc_info:
        api_error(400, "definitely_not_a_code", "boom")
    assert str(exc_info.value) == (
        "unknown error code 'definitely_not_a_code': add it to ErrorCode and api-contract.md"
    )


def test_runtime_error_codes_match_openapi_enum():
    assert ERROR_CODES == frozenset(get_args(ErrorCode))


def test_every_code_roundtrips_envelope():
    for code in sorted(ERROR_CODES):
        exc = api_error(400, code, f"msg for {code}", {"k": "v"})
        assert exc.detail == {"error": {"code": code, "message": f"msg for {code}", "detail": {"k": "v"}}}


def test_call_site_codes_are_subset_of_error_codes():
    """Every literal code passed to api_error() anywhere in the package is in the
    closed set. A new code must be added to ErrorCode and the contract first."""
    seen: dict[str, str] = {}
    for path in _PKG_DIR.rglob("*.py"):
        src = path.read_text()
        for m in re.finditer(r'api_error\(\s*\d+\s*,\s*"([a-z_]+)"', src):
            seen[m.group(1)] = str(path)
        # no dynamic/non-literal code arguments allowed at all
        for m in re.finditer(r"api_error\(\s*[\w.]+\s*,\s*([a-z_][\w.]*)\s*,", src):
            raise AssertionError(f"non-literal error code at {path}: {m.group(1)}")
    unknown = {c: p for c, p in seen.items() if c not in ERROR_CODES}
    assert not unknown, f"codes at call sites missing from ERROR_CODES: {unknown}"


def test_error_codes_all_documented():
    """Every member of the closed set appears in docs/api-contract.md."""
    doc = _CONTRACT_DOC.read_text()
    missing = [c for c in sorted(ERROR_CODES) if c not in doc]
    assert not missing, f"ERROR_CODES not documented in api-contract.md: {missing}"


async def test_unauthorized_envelope(adapter_client):
    """The shared verify_token dependency emits the same envelope (code=unauthorized)."""
    resp = await adapter_client.get("/api/v1/devices")  # deliberately no auth header
    assert resp.status_code == 401
    body = resp.json()
    assert body["error"]["code"] == "unauthorized"


def test_version_single_source_matches_pyproject():
    """nso_adapter.__version__ is THE version; pyproject must agree (no triplication)."""
    import tomllib

    from nso_adapter import __version__

    pyproject = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text())
    assert pyproject["project"]["version"] == __version__


def test_caller_keyed_maps_are_registered_for_loc_redaction():
    """Every caller-keyed map a REQUEST body carries must be in ``DYNAMIC_KEY_LOCATIONS``.

    Pydantic reports a failing map entry at ``("body", <field>, <key>)``, and the key is a
    name the caller chose whatever the entry's value type is. The set is derived from the
    app's own routes, so a new map added without registering it fails here instead of
    putting the caller's key into a 422. Response models are out: their validation failures
    never reach this handler.
    """
    from nso_adapter.api.errors import DYNAMIC_KEY_LOCATIONS
    from nso_adapter.main import app

    found, unnameable = _caller_keyed_locations(_request_body_models(app))

    assert found, "no caller-keyed map was found at all; the introspection stopped matching"
    assert found <= DYNAMIC_KEY_LOCATIONS, (
        "a request field keyed by a caller-chosen name is not registered for loc redaction: "
        f"{sorted(found - DYNAMIC_KEY_LOCATIONS)}"
    )
    assert not unnameable, (
        "a caller-keyed map sits behind a per-request segment, which DYNAMIC_KEY_LOCATIONS "
        f"cannot express; _safe_loc has to grow that shape first: {sorted(unnameable)}"
    )


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        (dict[str, SecretStr], True),
        (dict[str, SecretStr] | None, True),
        (Union[dict[str, SecretStr], None], True),  # noqa: UP007 - exercise typing.Union
        (Annotated[dict[str, SecretStr], "marker"], True),
        (Annotated[dict[str, SecretStr] | None, "marker"], True),
        (_CallerKeyedMapAlias, True),
        (_CallerKeyedMapAlias | None, True),
        # the key is the caller's string whatever the entry holds
        (dict[str, str], True),
        (dict[str, int], True),
        (dict[str, Annotated[int, Field(ge=1)]], True),
        (dict, True),
        (dict | None, True),
        (_CyclicCallerKeyedMapAlias, False),
        (dict[str, SecretStr] | int, False),
        (
            Union[  # noqa: UP007 - exercise a non-transparent typing.Union
                dict[str, SecretStr],
                Annotated[dict[str, SecretStr], Field(min_length=2)],
                None,
            ],
            False,
        ),
        (list[dict[str, SecretStr]], False),
        (dict[int, SecretStr], False),
    ],
)
def test_caller_keyed_map_annotation_recognizes_only_transparent_wrappers(annotation, expected):
    assert _is_caller_keyed_map_annotation(annotation) is expected


def test_caller_keyed_location_discovery_uses_the_wrapped_annotation_predicate():
    class WrappedMapRequest(BaseModel):
        values: _CallerKeyedMapAlias | None = None
        selected: dict[str, int] = {}

    assert _caller_keyed_locations([WrappedMapRequest]) == ({("body", "values"), ("body", "selected")}, set())


def test_caller_keyed_location_discovery_walks_nested_request_models():
    class Inner(BaseModel):
        entries: dict[str, int] = {}

    class OuterRequest(BaseModel):
        inner: Inner | None = None

    assert _caller_keyed_locations([OuterRequest]) == ({("body", "inner", "entries")}, set())


def test_a_map_behind_a_per_request_segment_is_reported_as_unnameable():
    """A list index is not a literal, so the prefix cannot be registered: say so, never drop it."""

    class Item(BaseModel):
        entries: dict[str, int] = {}

    class ListRequest(BaseModel):
        items: list[Item] = []

    assert _caller_keyed_locations([ListRequest]) == (set(), {"body.items.<index>.entries"})
