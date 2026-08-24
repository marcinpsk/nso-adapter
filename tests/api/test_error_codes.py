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
from pathlib import Path
from typing import get_args

import pytest
from pydantic import BaseModel, field_validator

from nso_adapter.api.errors import ERROR_CODES, ErrorCode, api_error
from tests.conftest import VALID_TOKEN, push_seq

AUTH = {"Authorization": f"Bearer {VALID_TOKEN}"}

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PKG_DIR = _REPO_ROOT / "nso_adapter"
_CONTRACT_DOC = _REPO_ROOT / "docs" / "api-contract.md"


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

    assert response.status_code == 422
    assert secret not in response.text
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

    assert resp.status_code == 500
    assert resp.json() == {"error": {"code": "internal", "message": "Internal server error", "detail": {}}}
    assert secret not in resp.text

    (record,) = [log for log in logs if log["event"] == "api.unhandled_exception"]
    assert record["exception_type"] == "RuntimeError"
    assert not record.get("exc_info"), "the raw exception reaches the log renderer"
    assert secret not in repr(logs)

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
        raise api_error(409, "conflict", "a job is already running", {"device_id": 7})

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/_test/conflict")

    assert resp.status_code == 409
    assert resp.json() == {
        "error": {"code": "conflict", "message": "a job is already running", "detail": {"device_id": 7}}
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
    from httpx import ASGITransport, AsyncClient

    from nso_adapter.core.receipt import PromotionProvenanceUnexecutable
    from nso_adapter.main import create_app

    app = create_app()

    @app.get("/_test/promotion-provenance")
    async def _promotion_provenance():
        raise PromotionProvenanceUnexecutable("vlan")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/_test/promotion-provenance")

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
