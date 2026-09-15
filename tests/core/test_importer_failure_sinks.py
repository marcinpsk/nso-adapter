# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The surface fan-out must classify a failure, never repeat the server's text.

``sync.surface_refresh_failed`` logged ``repr(exc)`` for ANY surface. The surfaces that
raise today are adapter-authored, so the sink was safe for them, but a surface that lets an
``httpx`` failure out puts the request URL and the server's reason phrase into the record.
Both fan-outs are covered: ``_run_surfaces`` (the plain one) and ``_apply_projected`` (the
projected one).
"""

from __future__ import annotations

import ast
import copy
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
import yaml

from nso_adapter.store.models import Device
from tests.conftest import seed_device, session

#: A URL and reason phrase a real NSO would put in the httpx message.
_URL = "https://nso.invalid/restconf/data/placeholder-mount/placeholder-path"
_REASON = "Placeholder Reason Phrase"
_LEAKS = [_URL, "placeholder-mount", "placeholder-path", _REASON]
_COVERAGE_DOC = Path(__file__).resolve().parents[2] / ".opengrep" / "README.md"
_RULES = Path(__file__).resolve().parents[2] / ".opengrep" / "nso-rules.yaml"
_IMPORTER = Path(__file__).resolve().parents[2] / "nso_adapter" / "core" / "importer.py"
_NSO_CLIENT = Path(__file__).resolve().parents[2] / "nso_adapter" / "nso" / "client.py"
_GUARDED_LOG_SINKS = (
    Path(__file__).resolve().parents[2] / "nso_adapter" / "main.py",
    *(
        Path(__file__).resolve().parents[2] / "nso_adapter" / "core" / name
        for name in ("generation.py", "refresh_engine.py", "redistribution.py")
    ),
)


def _is_closed_exception_classification(value: ast.expr) -> bool:
    """Return whether the expression keeps only an approved closed classification."""
    if (
        isinstance(value, ast.Call)
        and isinstance(value.func, ast.Name)
        and value.func.id == "failure_detail"
        and len(value.args) == 1
        and not value.keywords
    ):
        return True
    if (
        isinstance(value, ast.Attribute)
        and value.attr == "__name__"
        and isinstance(value.value, ast.Call)
        and isinstance(value.value.func, ast.Name)
        and value.value.func.id == "type"
        and len(value.value.args) == 1
        and not value.value.keywords
    ):
        return True
    return (
        isinstance(value, ast.Call)
        and isinstance(value.func, ast.Name)
        and value.func.id == "http_status_of"
        and len(value.args) == 1
        and not value.keywords
    )


class _RawLogExceptionVisitor(ast.NodeVisitor):
    """Track exception values through simple assignments while visiting log calls."""

    def __init__(self) -> None:
        self.aliases = {"exc"}
        self.violations: list[int] = []

    def _record_violation(self, lineno: int) -> None:
        if lineno not in self.violations:
            self.violations.append(lineno)

    def _assignment(self, targets: list[ast.expr], value: ast.expr) -> None:
        aliases_exception = isinstance(value, ast.Name) and value.id in self.aliases
        for target in targets:
            if not isinstance(target, ast.Name):
                continue
            if aliases_exception:
                self.aliases.add(target.id)
            else:
                self.aliases.discard(target.id)

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        outer_aliases = self.aliases
        self.aliases = {"exc"}
        for statement in node.body:
            self.visit(statement)
        self.aliases = outer_aliases

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802 - ast visitor API
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802 - ast visitor API
        self._visit_function(node)

    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802 - ast visitor API
        self.visit(node.value)
        self._assignment(node.targets, node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:  # noqa: N802 - ast visitor API
        if node.value is not None:
            self.visit(node.value)
            self._assignment([node.target], node.value)

    def visit_If(self, node: ast.If) -> None:  # noqa: N802 - ast visitor API
        self.visit(node.test)
        incoming = self.aliases.copy()

        self.aliases = incoming.copy()
        for statement in node.body:
            self.visit(statement)
        body_aliases = self.aliases

        self.aliases = incoming.copy()
        for statement in node.orelse:
            self.visit(statement)
        self.aliases |= body_aliases

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:  # noqa: N802 - ast visitor API
        handler_name = node.name
        if handler_name is not None:
            self.aliases.add(handler_name)
        for statement in node.body:
            self.visit(statement)
        if handler_name is not None:
            self.aliases.discard(handler_name)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802 - ast visitor API
        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "logger"
        ):
            if node.func.attr == "exception":
                self._record_violation(node.lineno)
            for argument in node.args:
                if not _is_closed_exception_classification(argument) and any(
                    isinstance(part, ast.Name) and part.id in self.aliases for part in ast.walk(argument)
                ):
                    self._record_violation(node.lineno)
            for keyword in node.keywords:
                if (
                    keyword.arg == "exc_info"
                    and isinstance(keyword.value, ast.Constant)
                    and keyword.value.value is True
                ):
                    self._record_violation(node.lineno)
                elif keyword.arg == "error" and not _is_closed_exception_classification(keyword.value):
                    self._record_violation(node.lineno)
                elif not _is_closed_exception_classification(keyword.value) and any(
                    isinstance(part, ast.Name) and part.id in self.aliases for part in ast.walk(keyword.value)
                ):
                    self._record_violation(node.lineno)
        self.generic_visit(node)


def _raw_log_exception_renderers(source: str) -> list[int]:
    """Return log calls that do not use the one classified exception shape."""
    visitor = _RawLogExceptionVisitor()
    visitor.visit(ast.parse(source))
    return visitor.violations


def test_raw_exception_log_guard_rejects_every_unsanitized_form() -> None:
    source = """\
logger.warning("event", error=exc)
logger.warning("event", error=f"{exc}")
logger.warning("event", error=str(exc))
logger.warning("event", error=repr(exc))
logger.exception("event")
logger.exception("event", error=failure_detail(exc))
logger.warning("event", detail=exc)
logger.warning("event", detail=f"{exc}")
logger.warning("event", detail="{}".format(exc))
logger.warning("event", detail="%s" % exc)
logger.warning("event", detail="failure: " + str(exc))
logger.warning("event", detail=format(exc))
logger.warning("event", detail=str(exc))
logger.warning("event", detail=repr(exc))
logger.warning("event", exc_info=True)
logger.warning("event", detail=failure_detail(exc))
logger.warning("event", error=failure_detail(exc))
"""
    assert _raw_log_exception_renderers(source) == list(range(1, 16))


@pytest.mark.parametrize(
    "source",
    [
        'logger.warning(f"failed: {exc}")',
        'logger.warning("failed: {}".format(exc))',
        'logger.warning("failed: %s" % exc)',
        'logger.warning("failed: %s", exc)',
    ],
)
def test_raw_exception_log_guard_rejects_positional_renderers(source: str) -> None:
    assert _raw_log_exception_renderers(source) == [1]


def test_raw_exception_log_guard_accepts_classified_positional_detail() -> None:
    assert _raw_log_exception_renderers('logger.warning("event", failure_detail(exc))') == []


@pytest.mark.parametrize(
    "source",
    [
        'logger.warning("event", reason=exc)',
        'logger.warning("event", message=f"failed: {exc}")',
        'logger.warning("event", exc_info=exc)',
    ],
)
def test_raw_exception_log_guard_rejects_aliases_in_every_structured_field(source: str) -> None:
    assert _raw_log_exception_renderers(source) == [1]


@pytest.mark.parametrize("field", ["reason", "message", "exc_info"])
def test_raw_exception_log_guard_accepts_classified_structured_fields(field: str) -> None:
    assert _raw_log_exception_renderers(f'logger.warning("event", {field}=failure_detail(exc))') == []


@pytest.mark.parametrize(
    "source",
    [
        'logger.warning("event", error_type=type(exc).__name__)',
        'logger.warning("event", http_status=http_status_of(exc))',
    ],
)
def test_raw_exception_log_guard_accepts_closed_exception_classifications(source: str) -> None:
    assert _raw_log_exception_renderers(source) == []


def test_raw_exception_log_guard_tracks_simple_aliases() -> None:
    source = """\
try:
    work()
except Exception as caught:
    alias = caught
    logger.warning("event", detail=alias)
    alias = "authored detail"
    logger.warning("event", detail=alias)
"""
    assert _raw_log_exception_renderers(source) == [5]


def test_raw_exception_log_guard_preserves_aliases_from_conditional_branches() -> None:
    source = """\
try:
    work()
except Exception as caught:
    if condition:
        alias = caught
    else:
        alias = "authored detail"
    logger.warning("event", detail=alias)
"""
    assert _raw_log_exception_renderers(source) == [8]


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            """\
alias = exc
if condition:
    alias = "authored detail"
logger.warning("event", detail=alias)
""",
            [4],
        ),
        (
            """\
alias = exc
if condition:
    alias = "authored detail"
else:
    alias = "also authored"
logger.warning("event", detail=alias)
""",
            [],
        ),
        (
            """\
if logger.warning("event", detail=exc):
    pass
""",
            [1],
        ),
    ],
)
def test_raw_exception_log_guard_preserves_conditional_flow(source: str, expected: list[int]) -> None:
    assert _raw_log_exception_renderers(source) == expected


def test_raw_exception_log_guard_does_not_leak_aliases_between_functions() -> None:
    source = """\
def first():
    try:
        work()
    except Exception as caught:
        alias = caught

def second(alias):
    logger.warning("event", detail=alias)
"""
    assert _raw_log_exception_renderers(source) == []


def test_importer_never_logs_raw_exception_text() -> None:
    assert _raw_log_exception_renderers(_IMPORTER.read_text(encoding="utf-8")) == []


def test_guarded_modules_never_log_raw_exception_text() -> None:
    violations = {
        path.name: _raw_log_exception_renderers(path.read_text(encoding="utf-8"))
        for path in _GUARDED_LOG_SINKS
        if _raw_log_exception_renderers(path.read_text(encoding="utf-8"))
    }
    assert violations == {}


def test_guarded_modules_are_documented() -> None:
    coverage = _COVERAGE_DOC.read_text(encoding="utf-8").split("## Coverage", maxsplit=1)[1]
    for path in (_IMPORTER, *_GUARDED_LOG_SINKS):
        assert path.name in coverage, f"{path.name} is missing from the OpenGrep coverage documentation"
    assert "any `api_error` in `action_force_removal`" in coverage
    assert "no endpoint error response returns the submitted scope" in coverage


def test_review_guards_cover_each_authored_error_boundary() -> None:
    rules = {rule["id"]: rule for rule in yaml.safe_load(_RULES.read_text(encoding="utf-8"))["rules"]}
    validation_paths = set(rules["nso-api-validation-error-raw-exception-renderer"]["paths"]["include"])
    assert validation_paths == {
        "nso_adapter/api/devices.py",
        "nso_adapter/api/lag_config.py",
        "nso_adapter/api/vlan.py",
        "review-patterns.py",
    }
    outcome_paths = set(rules["nso-outcome-raw-exception-renderer"]["paths"]["include"])
    alias_paths = set(rules["nso-outcome-raw-exception-alias-renderer"]["paths"]["include"])
    assert alias_paths == outcome_paths
    assert "nso_adapter/core/generation.py" in outcome_paths


def _binds_formatter_name(node: ast.AST) -> bool:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return node.name == "failure_detail"
    if isinstance(node, ast.Name):
        return node.id == "failure_detail" and isinstance(node.ctx, (ast.Store, ast.Del))
    if isinstance(node, ast.alias):
        imported_name = node.asname or node.name.split(".", maxsplit=1)[0]
        return imported_name == "failure_detail" or node.name == "*"
    return False


def _failure_detail_definition_ast(source: str) -> str:
    """Return the one effective formatter definition with its docstring normalized."""
    tree = ast.parse(source)
    bindings = [node for node in ast.walk(tree) if _binds_formatter_name(node)]
    if len(bindings) != 1 or bindings[0] not in tree.body or not isinstance(bindings[0], ast.FunctionDef):
        raise ValueError("failure_detail must have one direct module function binding")
    formatter = copy.deepcopy(bindings[0])
    if formatter.body and isinstance(formatter.body[0], ast.Expr) and isinstance(formatter.body[0].value, ast.Constant):
        formatter.body[0].value.value = "<docstring>"
    return ast.dump(formatter, include_attributes=False)


_APPROVED_FAILURE_DETAIL = '''\
def failure_detail(exc: BaseException) -> str:
    """Approved formatter contract."""
    if isinstance(exc, httpx.HTTPStatusError):
        return f"{type(exc).__name__} (HTTP {exc.response.status_code})"
    if type(exc) is NsoActionFailedError:
        kind = getattr(exc, "kind", None)
        if type(kind) is NsoActionFailureKind:
            return f"NsoActionFailedError({kind.value!r})"
    return type(exc).__name__
'''
_APPROVED_FAILURE_DETAIL_AST = _failure_detail_definition_ast(_APPROVED_FAILURE_DETAIL)


def test_failure_detail_reads_only_closed_exception_properties() -> None:
    """Any executable change to the ratified formatter shape requires an explicit review."""
    actual = _failure_detail_definition_ast(_NSO_CLIENT.read_text(encoding="utf-8"))

    assert actual == _APPROVED_FAILURE_DETAIL_AST


@pytest.mark.parametrize(
    "unsafe_body",
    [
        '    alias = exc\n    return "{}".format(alias)\n',
        """\
    kind = getattr(exc, "kind", None)
    if type(kind) is NsoActionFailureKind:
        pass
    else:
        return kind.value
""",
        """\
    kind = getattr(exc, "kind", None)
    if type(kind) is NsoActionFailureKind:
        kind = getattr(exc, "request", None)
        return kind.value
""",
    ],
)
def test_failure_detail_guard_rejects_unratified_shapes(unsafe_body: str) -> None:
    candidate = f"def failure_detail(exc):\n{unsafe_body}"

    assert _failure_detail_definition_ast(candidate) != _APPROVED_FAILURE_DETAIL_AST


@pytest.mark.parametrize(
    "rebind",
    [
        "\ndef failure_detail(exc):\n    return exc.args[0]\n",
        "\nfailure_detail = lambda exc: exc.args[0]\n",
    ],
)
def test_failure_detail_guard_rejects_an_alternate_binding(rebind: str) -> None:
    with pytest.raises(ValueError, match="one direct module function binding"):
        _failure_detail_definition_ast(_APPROVED_FAILURE_DETAIL + rebind)


@pytest.mark.parametrize(
    "rebind",
    [
        "\nif True:\n    def failure_detail(exc):\n        return str(exc)\n",
        "\ntry:\n    from unsafe_it import *\nexcept ImportError:\n    pass\n",
    ],
)
def test_failure_detail_guard_rejects_a_conditional_binding(rebind: str) -> None:
    with pytest.raises(ValueError, match="one direct module function binding"):
        _failure_detail_definition_ast(_APPROVED_FAILURE_DETAIL + rebind)


def test_failure_detail_guard_rejects_a_decorator() -> None:
    decorated = _APPROVED_FAILURE_DETAIL.replace("def failure_detail", "@unsafe\ndef failure_detail", 1)

    assert _failure_detail_definition_ast(decorated) != _APPROVED_FAILURE_DETAIL_AST


@asynccontextmanager
async def _device_session(device_id: int):
    async with session() as db:
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return


def _httpx_failure() -> httpx.HTTPStatusError:
    """The REAL httpx-authored error, message built by httpx itself, not by hand."""
    request = httpx.Request("GET", _URL)
    response = httpx.Response(
        403,
        request=request,
        extensions={"reason_phrase": _REASON.encode("ascii")},
        text="placeholder body",
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        return exc
    raise AssertionError("raise_for_status did not raise on 403")


def _assert_classified(record: dict) -> None:
    detail = record["error"]
    for leaked in _LEAKS:
        assert leaked not in detail, f"the record repeats {leaked!r} from the server"
    assert "HTTPStatusError" in detail, "the record must still name the failure type"
    assert "403" in detail, "the operator must still be able to tell an auth refusal from an outage"


@pytest.mark.anyio
async def test_the_plain_fanout_classifies_an_httpx_failure_and_repeats_no_server_text(adapter_client):
    """``_run_surfaces`` is the plain fan-out: one surface raising must not take down the rest."""
    from structlog.testing import capture_logs

    from nso_adapter.core.importer import _run_surfaces

    device_id = await seed_device(nso_device_name="placeholder-sink-dev")

    async def _raises(db, device, nso_client, *, refresh_source):
        raise _httpx_failure()

    async with _device_session(device_id) as (db, device):
        with capture_logs() as logs:
            failed = await _run_surfaces(db, device, AsyncMock(), [("vlan", _raises)], "poll")

    assert failed == ["vlan"], "the surface must still be reported as failed"
    records = [r for r in logs if r["event"] == "sync.surface_refresh_failed"]
    assert records, "the failure was not reported at all"
    _assert_classified(records[0])


@pytest.mark.anyio
async def test_the_projected_fanout_classifies_an_httpx_failure_and_repeats_no_server_text(adapter_client):
    """``_apply_projected`` is the second fan-out, and it carried the same sink."""
    from structlog.testing import capture_logs

    from nso_adapter.core.importer import _apply_projected, _ProjectedRead, _projection_layout

    device_id = await seed_device(nso_device_name="placeholder-sink-dev-2")

    async def _raises(db, device, nso_client, *, refresh_source):
        raise _httpx_failure()

    surfaces = [("placeholder-spec-less-surface", _raises)]
    layout = _projection_layout(surfaces)
    assert layout.spec_by_name["placeholder-spec-less-surface"] is None, "the spec-less branch is under test"

    async with _device_session(device_id) as (db, device):
        projection = _ProjectedRead(
            device=device.nso_device_name,
            sections={},
            supplier_outcome=None,
            section_failures={},
        )
        with capture_logs() as logs:
            failed = await _apply_projected(db, device, AsyncMock(), surfaces, "poll", layout, projection)

    assert failed == ["placeholder-spec-less-surface"], "the surface must still be reported as failed"
    records = [r for r in logs if r["event"] == "sync.surface_refresh_failed"]
    assert records, "the failure was not reported at all"
    _assert_classified(records[0])


# ── the action's own contract failures keep their own codes ──────────────────


def _atomic_output(device_name: str, sections: dict) -> dict:
    """One certified device-state-read output: atomic, right device, terminal sections."""
    return {"network-state-export:output": {"atomic": True, "device-name": device_name, **sections}}


async def test_a_missing_action_section_is_named_missing_not_malformed(adapter_client):
    """A requested family the action did not answer is an action contract failure.

    The certification deliberately lets a missing section through (`client.py:128`) because
    what it means is the caller's to decide, and the single-family escalation already decides
    `action_section_missing`. Splitting the multi-family output called it `section_malformed`,
    which says the server sent something unusable rather than nothing at all.
    """
    from nso_adapter.core.importer import _fetch_projection
    from nso_adapter.nso.read_outcome import ReadFailureCode, ReadOperation
    from tests.nso.test_nso_client_methods import MockTransport, _make_client

    device_id = await seed_device(nso_device_name="split-missing-section")
    client = _make_client()
    served = _atomic_output("split-missing-section", {"static-route": {"status": "ok", "route": []}})
    transport = MockTransport(200, served)
    client._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url="http://nso:8080")

    async with _device_session(device_id) as (_db, device):
        sections, outcome, failures = await _fetch_projection(
            client, device, ["static-route", "interface-ip"], atomic=True
        )

    assert outcome is None, "the supplier answered; only one family is unserved"
    assert sections["static-route"] == {"status": "ok", "route": []}
    assert sections["interface-ip"] is None
    failure = failures["interface-ip"]
    assert failure.code is ReadFailureCode.action_section_missing
    assert failure.operation is ReadOperation.device_state_read
    assert failure.family == "interface-ip"


async def test_a_non_terminal_action_section_never_reaches_the_split(adapter_client):
    """The client refuses a non-terminal status, so the split cannot see a not-ready one.

    `_certify_device_state_output` (client.py:132-137) raises NsoReadContractError unless every
    requested-and-present section carries ok/unsupported/error, and that raise happens inside
    the supplier's own try, so the whole read degrades to read_error with the family rows kept.
    """
    from nso_adapter.core.importer import _fetch_projection
    from nso_adapter.nso.read_outcome import ReadOperation, Unavailable, UnavailableReason
    from tests.nso.test_nso_client_methods import MockTransport, _make_client

    device_id = await seed_device(nso_device_name="split-not-ready")
    client = _make_client()
    served = _atomic_output("split-not-ready", {"static-route": {"status": "not-ready"}})
    transport = MockTransport(200, served)
    client._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url="http://nso:8080")

    async with _device_session(device_id) as (_db, device):
        sections, outcome, _failures = await _fetch_projection(client, device, ["static-route"], atomic=True)

    assert sections == {}, "nothing may be materialized from an uncertified answer"
    assert isinstance(outcome, Unavailable)
    assert outcome.reason is UnavailableReason.read_error
    assert outcome.failure.error_type == "NsoReadContractError"
    assert outcome.failure.operation is ReadOperation.device_state_read


async def test_a_malformed_record_document_is_a_read_error_not_an_export_outage(adapter_client):
    """A malformed HTTP 200 is a contract failure, not a missing export container."""
    from nso_adapter.core.importer import _fetch_projection
    from nso_adapter.nso.read_outcome import ReadOperation, Unavailable, UnavailableReason
    from tests.nso.test_device_state_client import EnvelopeTransport, _make_client

    device_id = await seed_device(nso_device_name="malformed-record-document")
    client = _make_client()
    transport = EnvelopeTransport(device_body={})
    client._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url="http://nso:8080")

    async with _device_session(device_id) as (_db, device):
        sections, outcome, failures = await _fetch_projection(client, device, ["static-route"])

    assert sections == {}
    assert failures == {}
    assert isinstance(outcome, Unavailable)
    assert outcome.reason is UnavailableReason.read_error
    assert outcome.failure.error_type == "NsoReadContractError"
    assert outcome.failure.operation is ReadOperation.doc_get
