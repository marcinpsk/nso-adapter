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
    *(
        Path(__file__).resolve().parents[2] / "nso_adapter" / "notifications" / name
        for name in ("persistent_subscriber.py", "sse_subscriber.py")
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
        self._try_handler_inputs: list[set[str]] = []

    def _record_violation(self, lineno: int) -> None:
        if lineno not in self.violations:
            self.violations.append(lineno)

    def _visit_statements(self, statements: list[ast.stmt]) -> None:
        for statement in statements:
            for handler_input in self._try_handler_inputs:
                handler_input.update(self.aliases)
            self.visit(statement)

    def _aliases_exception(self, values: ast.expr | list[ast.expr]) -> bool:
        if not isinstance(values, list):
            values = [values]
        return any(
            not _is_closed_exception_classification(value)
            and any(isinstance(part, ast.Name) and part.id in self.aliases for part in ast.walk(value))
            for value in values
        )

    def _bind_target(self, target: ast.expr, values: ast.expr | list[ast.expr]) -> None:
        if isinstance(target, ast.Name):
            if self._aliases_exception(values):
                self.aliases.add(target.id)
            else:
                self.aliases.discard(target.id)
        elif isinstance(target, ast.Starred):
            self._bind_target(target.value, values)
        elif isinstance(target, (ast.List, ast.Tuple)):
            if isinstance(values, (ast.List, ast.Tuple)):
                self._bind_sequence(target.elts, values.elts)
            else:
                for element in target.elts:
                    self._bind_target(element, values)

    def _bind_sequence(self, targets: list[ast.expr], values: list[ast.expr]) -> None:
        starred = next((index for index, target in enumerate(targets) if isinstance(target, ast.Starred)), None)
        if starred is None:
            if len(targets) == len(values):
                for target, value in zip(targets, values, strict=True):
                    self._bind_target(target, value)
                return
        elif len(values) >= len(targets) - 1:
            trailing = len(targets) - starred - 1
            for target, value in zip(targets[:starred], values[:starred], strict=True):
                self._bind_target(target, value)
            starred_end = len(values) - trailing if trailing else len(values)
            self._bind_target(targets[starred], values[starred:starred_end])
            if trailing:
                for target, value in zip(targets[-trailing:], values[-trailing:], strict=True):
                    self._bind_target(target, value)
            return
        for target in targets:
            self._bind_target(target, values)

    def _assignment(self, targets: list[ast.expr], value: ast.expr) -> None:
        for target in targets:
            self._bind_target(target, value)

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        outer_aliases = self.aliases
        outer_handler_inputs = self._try_handler_inputs
        self.aliases = {"exc"}
        self._try_handler_inputs = []
        self._visit_statements(node.body)
        self._try_handler_inputs = outer_handler_inputs
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

    def visit_AugAssign(self, node: ast.AugAssign) -> None:  # noqa: N802 - ast visitor API
        self.visit(node.value)
        target_was_alias = isinstance(node.target, ast.Name) and node.target.id in self.aliases
        self._assignment([node.target], node.value)
        if target_was_alias and isinstance(node.target, ast.Name):
            self.aliases.add(node.target.id)

    def visit_If(self, node: ast.If) -> None:  # noqa: N802 - ast visitor API
        self.visit(node.test)
        incoming = self.aliases.copy()

        self.aliases = incoming.copy()
        self._visit_statements(node.body)
        body_aliases = self.aliases

        self.aliases = incoming.copy()
        self._visit_statements(node.orelse)
        self.aliases |= body_aliases

    def _visit_loop(self, node: ast.For | ast.AsyncFor | ast.While) -> None:
        if isinstance(node, (ast.For, ast.AsyncFor)):
            self.visit(node.iter)
        else:
            self.visit(node.test)
        incoming = self.aliases.copy()

        self.aliases = incoming.copy()
        self._visit_statements(node.body)
        body_aliases = self.aliases.copy()

        self.aliases = incoming | body_aliases
        self._visit_statements(node.orelse)
        self.aliases |= incoming | body_aliases

    def visit_For(self, node: ast.For) -> None:  # noqa: N802 - ast visitor API
        self._visit_loop(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:  # noqa: N802 - ast visitor API
        self._visit_loop(node)

    def visit_While(self, node: ast.While) -> None:  # noqa: N802 - ast visitor API
        self._visit_loop(node)

    def _visit_with(self, node: ast.With | ast.AsyncWith) -> None:
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars is not None:
                self.visit(item.optional_vars)
        self._visit_statements(node.body)

    def visit_With(self, node: ast.With) -> None:  # noqa: N802 - ast visitor API
        self._visit_with(node)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:  # noqa: N802 - ast visitor API
        self._visit_with(node)

    def visit_Match(self, node: ast.Match) -> None:  # noqa: N802 - ast visitor API
        self.visit(node.subject)
        incoming = self.aliases.copy()
        surviving = incoming.copy()
        for case in node.cases:
            self.aliases = incoming.copy()
            self.visit(case.pattern)
            if case.guard is not None:
                self.visit(case.guard)
            self._visit_statements(case.body)
            surviving |= self.aliases
        self.aliases = surviving

    def visit_TryStar(self, node: ast.TryStar) -> None:  # noqa: N802 - ast visitor API
        # Same fields, same flow: without this, generic_visit walks `except*` in source order.
        self.visit_Try(node)

    def visit_Try(self, node: ast.Try | ast.TryStar) -> None:  # noqa: N802 - ast visitor API
        incoming = self.aliases.copy()
        handler_input = incoming.copy()

        self.aliases = incoming.copy()
        self._try_handler_inputs.append(handler_input)
        self._visit_statements(node.body)
        self._try_handler_inputs.pop()
        body_aliases = self.aliases.copy()

        self.aliases = body_aliases.copy()
        self._visit_statements(node.orelse)
        surviving = self.aliases.copy()

        for handler in node.handlers:
            self.aliases = handler_input.copy()
            self.visit(handler)
            surviving |= self.aliases

        # `finally` also runs when an earlier try-body statement raises, before a later
        # assignment can clear an alias. Only paths that fall out of the try continue after it.
        entry = surviving | handler_input
        self.aliases = entry.copy()
        self._visit_statements(node.finalbody)
        self.aliases = (surviving | (self.aliases - entry)) - (entry - self.aliases)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:  # noqa: N802 - ast visitor API
        handler_name = node.name
        if handler_name is not None:
            self.aliases.add(handler_name)
        self._visit_statements(node.body)
        if handler_name is not None:
            self.aliases.discard(handler_name)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802 - ast visitor API
        if isinstance(node.func, ast.Attribute) and _is_logger_receiver(node.func.value):
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


def _is_logger_receiver(node: ast.expr) -> bool:
    """Return whether a call receiver is the module logger or one of its bound loggers."""
    if isinstance(node, ast.Name):
        return node.id == "logger"
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "bind"
        and _is_logger_receiver(node.func.value)
    )


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


def test_subscriber_modules_are_in_the_positional_exception_guard() -> None:
    guarded_names = {path.name for path in _GUARDED_LOG_SINKS}
    assert {"persistent_subscriber.py", "sse_subscriber.py"} <= guarded_names


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


@pytest.mark.parametrize(
    "source",
    [
        """\
detail, authored = exc, "authored detail"
logger.warning("event", detail=detail)
logger.warning("event", detail=authored)
""",
        """\
[detail, authored] = [exc, "authored detail"]
logger.warning("event", detail=detail)
logger.warning("event", detail=authored)
""",
        """\
*detail, authored = exc, "authored detail"
logger.warning("event", detail=detail)
logger.warning("event", detail=authored)
""",
    ],
    ids=["tuple", "list", "starred"],
)
def test_raw_exception_log_guard_tracks_structured_assignment_elements(source: str) -> None:
    assert _raw_log_exception_renderers(source) == [2]


def test_raw_exception_log_guard_rejects_bound_logger_calls() -> None:
    source = 'logger.bind(component="sync").warning("event", detail=exc)'
    assert _raw_log_exception_renderers(source) == [1]


def test_raw_exception_log_guard_tracks_rendered_and_augmented_aliases() -> None:
    source = """\
detail = str(exc)
logger.warning("event", detail=detail)
detail = "authored detail"
detail += str(exc)
logger.warning("event", detail=detail)
"""
    assert _raw_log_exception_renderers(source) == [2, 5]


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


def test_raw_exception_log_guard_unions_handler_and_else_paths() -> None:
    source = """\
try:
    work()
except Exception as caught:
    alias = caught
else:
    alias = "authored detail"
logger.warning("event", detail=alias)
"""
    assert _raw_log_exception_renderers(source) == [7]


def test_raw_exception_log_guard_preserves_taint_at_each_try_body_exit() -> None:
    source = """\
try:
    alias = exc
    work()
    alias = "authored detail"
except Exception:
    pass
else:
    alias = "authored detail"
logger.warning("event", detail=alias)
"""
    assert _raw_log_exception_renderers(source) == [9]


def test_raw_exception_log_guard_preserves_nested_try_body_taint() -> None:
    source = """\
try:
    if condition:
        alias = exc
        work()
        alias = "authored detail"
except Exception:
    pass
logger.warning("event", detail=alias)
"""
    assert _raw_log_exception_renderers(source) == [8]


@pytest.mark.parametrize(
    ("source", "expected_line"),
    [
        (
            """\
try:
    with context():
        alias = exc
        work()
        alias = "authored detail"
except Exception:
    pass
logger.warning("event", detail=alias)
""",
            8,
        ),
        (
            """\
try:
    match value:
        case _:
            alias = exc
            work()
            alias = "authored detail"
except Exception:
    pass
logger.warning("event", detail=alias)
""",
            9,
        ),
    ],
)
def test_raw_exception_log_guard_preserves_taint_in_nested_statement_containers(
    source: str, expected_line: int
) -> None:
    assert _raw_log_exception_renderers(source) == [expected_line]


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


def test_the_action_section_code_is_derived_in_exactly_one_place() -> None:
    """`.get()` cannot tell an omitted key from a present null, so only the shared helper decides.

    This class already came back once: `_split_sections` was fixed while the escalation path in
    `refresh_engine` kept deriving the code itself. Naming the member anywhere but the helper is
    how that happens, so the guard is the reference, not the comparison.
    """
    root = Path(__file__).resolve().parents[2] / "nso_adapter"
    offenders = sorted(
        str(path.relative_to(root))
        for path in root.rglob("*.py")
        if path.name != "read_outcome.py" and "action_section_missing" in path.read_text(encoding="utf-8")
    )

    assert offenders == [], "derive the code via read_outcome.section_absence_code, never in the caller"


@pytest.mark.anyio
async def test_discovery_error_uses_the_configured_instance_identity(db_session, monkeypatch) -> None:
    from types import SimpleNamespace

    from structlog.testing import capture_logs

    from nso_adapter.config import NsoInstanceConfig
    from nso_adapter.core import importer
    from nso_adapter.nso.client import NsoClient
    from tests._secret_discipline import assert_records_free_of

    provider_device = "placeholder-provider-device"
    configured = NsoInstanceConfig(
        name="configured-discovery-instance",
        base_url="http://nso.invalid:8080",
        username_ref="NSO_USERNAME",
        password_ref="NSO_PASSWORD",
    )
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            503,
            request=request,
            extensions={"reason_phrase": provider_device.encode()},
        )
    )
    client = NsoClient(configured, "placeholder-user", "placeholder-password")
    client._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url=configured.base_url)
    monkeypatch.setattr(importer, "get_config", lambda: SimpleNamespace(nso_instances=[configured]))
    monkeypatch.setitem(importer._nso_clients, configured.name, client)

    with capture_logs() as logs:
        await importer.discover_devices(db_session)

    record = next(item for item in logs if item["event"] == "discover.error")
    assert_records_free_of([record], [provider_device])
    assert record["instance"] == configured.name
    assert record["error"] == "HTTPStatusError (HTTP 503)"


def test_guarded_modules_are_documented() -> None:
    coverage = _COVERAGE_DOC.read_text(encoding="utf-8").split("## Coverage", maxsplit=1)[1]
    for path in (_IMPORTER, *_GUARDED_LOG_SINKS):
        assert path.name in coverage, f"{path.name} is missing from the OpenGrep coverage documentation"
    assert "`nso-diagnostic-raw-identifier`" in coverage
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
    assert set(rules["nso-api-validation-error-raw-data-alias"]["paths"]["include"]) == validation_paths
    outcome_paths = set(rules["nso-outcome-raw-exception-renderer"]["paths"]["include"])
    alias_paths = set(rules["nso-outcome-raw-exception-alias-renderer"]["paths"]["include"])
    assert alias_paths == outcome_paths
    assert "nso_adapter/core/generation.py" in outcome_paths
    identifier_paths = set(rules["nso-diagnostic-raw-identifier"]["paths"]["include"])
    assert {
        "nso_adapter/core/importer.py",
        "nso_adapter/core/redistribution.py",
        "nso_adapter/nso/client.py",
    } <= identifier_paths


def _rule_patterns(node: object) -> list[str]:
    """Every `pattern:` string anywhere under one rule clause, however it is nested."""
    if isinstance(node, dict):
        return [
            *(value for key, value in node.items() if key == "pattern" and isinstance(value, str)),
            *(item for key, value in node.items() if key != "pattern" for item in _rule_patterns(value)),
        ]
    if isinstance(node, list):
        return [item for element in node for item in _rule_patterns(element)]
    return []


def test_the_identifier_guard_leaves_the_operator_authored_instance_name_alone() -> None:
    """The NSO instance name is out of the identifier class, so neither rule may carry it.

    The keyword rule banned `nso_instance=` while the tree spells the field `instance=`, so
    the guard passed on the spelling rather than on the verdict and a reviewer re-raised the
    same site three times. Both policies are pinned whole, together, so they cannot drift.
    """
    rules = {rule["id"]: rule for rule in yaml.safe_load(_RULES.read_text(encoding="utf-8"))["rules"]}
    fields = {
        pattern.split("=", maxsplit=1)[0].rsplit(" ", maxsplit=1)[-1]
        for pattern in _rule_patterns(rules["nso-diagnostic-raw-identifier"]["pattern-either"])
    }
    sources = _rule_patterns(rules["nso-diagnostic-raw-identifier-alias"]["pattern-sources"])
    instance_sources = {pattern for pattern in sources if pattern.rsplit(".", maxsplit=1)[-1] == "nso_instance"}

    assert fields == {"device_name", "device", "stream", "stream_url", "url"}
    assert instance_sources == set(), "the alias rule must not carry an instance source either"


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


async def test_an_explicitly_null_action_section_is_named_malformed_not_missing(adapter_client):
    """An action that ANSWERED the family with null sent something unusable, not nothing.

    Certification lets a null section through (`client.py:137`), so the split decides. `.get()`
    cannot tell an absent key from a present null, and only the absent key is the action's
    omission contract failure.
    """
    from nso_adapter.core.importer import _fetch_projection
    from nso_adapter.nso.read_outcome import ReadFailureCode, ReadOperation
    from tests.nso.test_nso_client_methods import MockTransport, _make_client

    device_id = await seed_device(nso_device_name="split-null-section")
    client = _make_client()
    served = _atomic_output(
        "split-null-section",
        {"static-route": {"status": "ok", "route": []}, "interface-ip": None},
    )
    transport = MockTransport(200, served)
    client._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url="http://nso:8080")

    async with _device_session(device_id) as (_db, device):
        sections, outcome, failures = await _fetch_projection(
            client, device, ["static-route", "interface-ip"], atomic=True
        )

    assert outcome is None
    assert sections["interface-ip"] is None
    failure = failures["interface-ip"]
    assert failure.code is ReadFailureCode.section_malformed, "a present null is not an omission"
    assert failure.operation is ReadOperation.device_state_read


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


_TRY_ELSE_FINALLY = """\
def f():
    try:
        detail = "authored"
    {handler} ValueError as exc:
        detail = exc
    else:
        detail = "authored"
    finally:
        logger.warning("event", detail=detail)
"""


@pytest.mark.parametrize("handler", ["except", "except*"], ids=["try", "try-star"])
def test_an_alias_a_handler_taints_still_reaches_the_finally_block(handler: str) -> None:
    """The handler path reaches `finally` too, so an `else` assignment cannot clear the alias."""
    assert _raw_log_exception_renderers(_TRY_ELSE_FINALLY.format(handler=handler)) == [9]


@pytest.mark.parametrize("handler", ["except", "except*"], ids=["try", "try-star"])
def test_a_handler_sees_an_alias_from_an_earlier_try_body_prefix(handler: str) -> None:
    source = """\
def f():
    try:
        detail = exc
        detail = "authored"
    {handler} ValueError:
        logger.warning("event", detail=detail)
"""
    assert _raw_log_exception_renderers(source.format(handler=handler)) == [6]


def test_a_finally_block_sees_an_alias_from_an_earlier_try_body_prefix() -> None:
    source = """\
def f():
    try:
        detail = exc
        detail = "authored"
    finally:
        logger.warning("event", detail=detail)
"""
    assert _raw_log_exception_renderers(source) == [6]


def test_a_try_that_every_path_reassigns_leaves_no_alias_behind_it() -> None:
    """`finally` sees the incoming state; the statements AFTER the try only see the exits."""
    every_path_clean = """\
def f():
    detail = exc
    try:
        detail = "authored"
    except ValueError:
        detail = "authored"
    logger.warning("event", detail=detail)
"""
    empty_finally = """\
def f():
    detail = exc
    try:
        detail = "authored"
    finally:
        pass
    logger.warning("event", detail=detail)
"""
    tainted_by_finally = """\
def f():
    try:
        detail = "authored"
    except ValueError as exc:
        pass
    finally:
        detail = exc
    logger.warning("event", detail=detail)
"""

    assert _raw_log_exception_renderers(every_path_clean) == []
    assert _raw_log_exception_renderers(empty_finally) == []
    assert _raw_log_exception_renderers(tainted_by_finally) == [8]
