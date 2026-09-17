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
from tests._ast_scanner_support import (
    argument_names,
    match_capture_names,
    pattern_is_irrefutable,
    scope_bound_names,
    statement_may_raise,
    walrus_target_names,
)
from tests.conftest import seed_device, session

#: A URL and reason phrase a real NSO would put in the httpx message.
_URL = "https://nso.invalid/restconf/data/placeholder-mount/placeholder-path"
_REASON = "Placeholder Reason Phrase"
_LEAKS = [_URL, "placeholder-mount", "placeholder-path", _REASON]
_COVERAGE_DOC = Path(__file__).resolve().parents[2] / ".opengrep" / "README.md"
_RULES = Path(__file__).resolve().parents[2] / ".opengrep" / "nso-rules.yaml"
_IMPORTER = Path(__file__).resolve().parents[2] / "nso_adapter" / "core" / "importer.py"
_NSO_CLIENT = Path(__file__).resolve().parents[2] / "nso_adapter" / "nso" / "client.py"
_NETBOX_CLIENT = Path(__file__).resolve().parents[2] / "nso_adapter" / "bindings" / "netbox" / "client.py"
_GUARDED_LOG_SINKS = (
    Path(__file__).resolve().parents[2] / "nso_adapter" / "main.py",
    *(
        Path(__file__).resolve().parents[2] / "nso_adapter" / "core" / name
        for name in ("failover.py", "generation.py", "refresh_engine.py", "redistribution.py", "removal.py")
    ),
    *(
        Path(__file__).resolve().parents[2] / "nso_adapter" / "bindings" / "netbox" / name
        for name in ("client.py", "mapper.py", "writer.py")
    ),
    *(
        Path(__file__).resolve().parents[2] / "nso_adapter" / "notifications" / name
        for name in ("persistent_subscriber.py", "sse_subscriber.py")
    ),
)


#: One-argument callables whose result is an approved closed classification for a log field.
#: Every name here has its definition pinned below, so widening this set is a reviewed act.
_APPROVED_CLASSIFIERS = frozenset({"failure_detail", "http_status_of", "rejection_detail"})


def _is_closed_exception_classification(value: ast.expr) -> bool:
    """Return whether the expression keeps only an approved closed classification."""
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
        and value.func.id in _APPROVED_CLASSIFIERS
        and len(value.args) == 1
        and not value.keywords
    )


class _RawLogExceptionVisitor(ast.NodeVisitor):
    """Track exception aliases through control flow while visiting log calls."""

    def __init__(self) -> None:
        self.aliases = {"exc"}
        self.violations: list[int] = []
        self._try_handler_inputs: list[set[str]] = []
        self._loop_break_aliases: list[list[set[str]]] = []
        self._loop_continue_aliases: list[list[set[str]]] = []
        self._class_enclosing_aliases: list[set[str]] = []
        self._try_exception_inputs: list[set[str]] = []

    def _record_violation(self, lineno: int) -> None:
        if lineno not in self.violations:
            self.violations.append(lineno)

    def _record_handler_input(self) -> None:
        if self._try_handler_inputs:
            self._try_handler_inputs[-1].update(self.aliases)

    def _visit_statements(self, statements: list[ast.stmt]) -> bool:
        for statement in statements:
            if statement_may_raise(statement):
                self._record_handler_input()
                if self._try_exception_inputs:
                    self._try_exception_inputs[-1].update(self.aliases)
            if self.visit(statement) is False:
                return False
        return True

    def _aliases_exception(self, values: ast.expr | list[ast.expr]) -> bool:
        if not isinstance(values, list):
            values = [values]
        return any(
            not _is_closed_exception_classification(value)
            and any(isinstance(part, ast.Name) and part.id in self.aliases for part in ast.walk(value))
            for value in values
        )

    def _bind_target_from_verdict(self, target: ast.expr, aliases_exception: bool) -> None:
        if isinstance(target, ast.Name):
            self._bind_names({target.id}, aliases_exception)
        elif isinstance(target, ast.Starred):
            self._bind_target_from_verdict(target.value, aliases_exception)
        elif isinstance(target, (ast.List, ast.Tuple)):
            for element in target.elts:
                self._bind_target_from_verdict(element, aliases_exception)

    def _bind_assignment_target(self, target: ast.expr, values: ast.expr | list[ast.expr]) -> None:
        if isinstance(target, ast.Starred):
            self._bind_assignment_target(target.value, values)
        elif isinstance(target, (ast.List, ast.Tuple)) and isinstance(values, (ast.List, ast.Tuple)):
            self._bind_sequence(target.elts, values.elts)
        else:
            self._bind_target_from_verdict(target, self._aliases_exception(values))

    def _bind_sequence(self, targets: list[ast.expr], values: list[ast.expr]) -> None:
        starred = next((index for index, target in enumerate(targets) if isinstance(target, ast.Starred)), None)
        if starred is None:
            if len(targets) == len(values):
                for target, value in zip(targets, values, strict=True):
                    self._bind_assignment_target(target, value)
                return
        elif len(values) >= len(targets) - 1:
            trailing = len(targets) - starred - 1
            for target, value in zip(targets[:starred], values[:starred], strict=True):
                self._bind_assignment_target(target, value)
            starred_end = len(values) - trailing if trailing else len(values)
            self._bind_assignment_target(targets[starred], values[starred:starred_end])
            if trailing:
                for target, value in zip(targets[-trailing:], values[-trailing:], strict=True):
                    self._bind_assignment_target(target, value)
            return
        aliases_exception = self._aliases_exception(values)
        for target in targets:
            self._bind_target_from_verdict(target, aliases_exception)

    def _bind_names(self, names: set[str], aliases_exception: bool) -> None:
        if aliases_exception:
            self.aliases.update(names)
        else:
            self.aliases.difference_update(names)

    def _assignment(self, targets: list[ast.expr], value: ast.expr) -> None:
        for target in targets:
            self._bind_assignment_target(target, value)

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        for expression in (
            *node.decorator_list,
            *node.args.defaults,
            *(default for default in node.args.kw_defaults if default is not None),
        ):
            self.visit(expression)
        outer_aliases = self.aliases
        outer_handler_inputs = self._try_handler_inputs
        outer_exception_inputs = self._try_exception_inputs
        outer_class_enclosing_aliases = self._class_enclosing_aliases
        enclosing_aliases = self._class_enclosing_aliases[-1] if self._class_enclosing_aliases else outer_aliases
        local_names = scope_bound_names(list(node.body)) | argument_names(node.args)
        self.aliases = (enclosing_aliases - local_names) | {"exc"}
        self._try_handler_inputs = []
        self._try_exception_inputs = []
        self._class_enclosing_aliases = []
        self._visit_statements(node.body)
        self._class_enclosing_aliases = outer_class_enclosing_aliases
        self._try_exception_inputs = outer_exception_inputs
        self._try_handler_inputs = outer_handler_inputs
        self.aliases = outer_aliases
        self.aliases.discard(node.name)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802 - ast visitor API
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802 - ast visitor API
        self._visit_function(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802 - ast visitor API
        for expression in (*node.args.defaults, *(value for value in node.args.kw_defaults if value is not None)):
            self.visit(expression)
        outer_aliases = self.aliases
        outer_handler_inputs = self._try_handler_inputs
        outer_exception_inputs = self._try_exception_inputs
        outer_class_enclosing_aliases = self._class_enclosing_aliases
        enclosing_aliases = self._class_enclosing_aliases[-1] if self._class_enclosing_aliases else outer_aliases
        local_names = scope_bound_names([node.body]) | argument_names(node.args)
        self.aliases = (enclosing_aliases - local_names) | {"exc"}
        self._try_handler_inputs = []
        self._try_exception_inputs = []
        self._class_enclosing_aliases = []
        self.visit(node.body)
        self._class_enclosing_aliases = outer_class_enclosing_aliases
        self._try_exception_inputs = outer_exception_inputs
        self._try_handler_inputs = outer_handler_inputs
        self.aliases = outer_aliases

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802 - ast visitor API
        for expression in (*node.decorator_list, *node.bases, *(keyword.value for keyword in node.keywords)):
            self.visit(expression)
        outer_aliases = self.aliases
        outer_handler_inputs = self._try_handler_inputs
        outer_exception_inputs = self._try_exception_inputs
        class_enclosing_aliases = self._class_enclosing_aliases[-1] if self._class_enclosing_aliases else outer_aliases
        self.aliases = class_enclosing_aliases.copy()
        self._try_handler_inputs = []
        self._try_exception_inputs = []
        self._class_enclosing_aliases.append(class_enclosing_aliases.copy())
        self._visit_statements(node.body)
        self._class_enclosing_aliases.pop()
        self._try_exception_inputs = outer_exception_inputs
        self._try_handler_inputs = outer_handler_inputs
        self.aliases = outer_aliases
        self.aliases.discard(node.name)

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

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:  # noqa: N802 - ast visitor API
        self.visit(node.value)
        self._assignment([node.target], node.value)

    def visit_If(self, node: ast.If) -> bool:  # noqa: N802 - ast visitor API
        self.visit(node.test)
        incoming = self.aliases.copy()

        self.aliases = incoming.copy()
        body_falls_through = self._visit_statements(node.body)
        body_aliases = self.aliases

        self.aliases = incoming.copy()
        else_falls_through = self._visit_statements(node.orelse)
        else_aliases = self.aliases

        self.aliases = set()
        if body_falls_through:
            self.aliases |= body_aliases
        if else_falls_through:
            self.aliases |= else_aliases
        return body_falls_through or else_falls_through

    def _visit_loop(self, node: ast.For | ast.AsyncFor | ast.While) -> bool:
        if isinstance(node, (ast.For, ast.AsyncFor)):
            self.visit(node.iter)
            iter_aliases_exception = self._aliases_exception(node.iter)
        else:
            self.visit(node.test)
            iter_aliases_exception = False
        incoming = self.aliases.copy()
        loop_aliases = incoming.copy()
        break_aliases: set[str] = set()
        max_passes = len({part.id for part in ast.walk(node) if isinstance(part, ast.Name)}) + 2

        for _ in range(max_passes):
            self.aliases = loop_aliases.copy()
            if isinstance(node, (ast.For, ast.AsyncFor)):
                self._bind_target_from_verdict(node.target, iter_aliases_exception)

            self._loop_break_aliases.append([])
            self._loop_continue_aliases.append([])
            body_falls_through = self._visit_statements(node.body)
            continue_aliases = self._loop_continue_aliases.pop()
            current_break_aliases = self._loop_break_aliases.pop()

            for aliases in current_break_aliases:
                break_aliases |= aliases
            next_aliases = incoming.copy()
            if body_falls_through:
                next_aliases |= self.aliases
            for aliases in continue_aliases:
                next_aliases |= aliases
            if next_aliases <= loop_aliases:
                break
            loop_aliases |= next_aliases

        self.aliases = loop_aliases
        else_falls_through = self._visit_statements(node.orelse)
        else_aliases = self.aliases.copy()
        self.aliases = break_aliases
        if else_falls_through:
            self.aliases |= else_aliases
        return True

    def visit_For(self, node: ast.For) -> bool:  # noqa: N802 - ast visitor API
        return self._visit_loop(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> bool:  # noqa: N802 - ast visitor API
        return self._visit_loop(node)

    def visit_While(self, node: ast.While) -> bool:  # noqa: N802 - ast visitor API
        return self._visit_loop(node)

    def visit_Break(self, node: ast.Break) -> bool:  # noqa: N802 - ast visitor API
        if self._loop_break_aliases:
            self._loop_break_aliases[-1].append(self.aliases.copy())
        return False

    def visit_Continue(self, node: ast.Continue) -> bool:  # noqa: N802 - ast visitor API
        if self._loop_continue_aliases:
            self._loop_continue_aliases[-1].append(self.aliases.copy())
        return False

    def visit_Return(self, node: ast.Return) -> bool:  # noqa: N802 - ast visitor API
        self.generic_visit(node)
        return False

    def visit_Raise(self, node: ast.Raise) -> bool:  # noqa: N802 - ast visitor API
        self.generic_visit(node)
        return False

    def _visit_comprehension(self, node: ast.ListComp | ast.SetComp | ast.GeneratorExp | ast.DictComp) -> None:
        outer_aliases = self.aliases
        first_generator, *remaining_generators = node.generators
        self.visit(first_generator.iter)
        outer_after_iter = self.aliases.copy()

        self.aliases = outer_after_iter.copy()
        self._bind_target_from_verdict(first_generator.target, self._aliases_exception(first_generator.iter))
        for condition in first_generator.ifs:
            self.visit(condition)
        for generator in remaining_generators:
            self.visit(generator.iter)
            self._bind_target_from_verdict(generator.target, self._aliases_exception(generator.iter))
            for condition in generator.ifs:
                self.visit(condition)
        if isinstance(node, ast.DictComp):
            self.visit(node.key)
            self.visit(node.value)
        else:
            self.visit(node.elt)

        body_aliases = self.aliases
        outer_aliases.clear()
        outer_aliases.update(outer_after_iter)
        # A walrus in the body binds in THIS scope (PEP 572); the generator targets do not.
        for name in walrus_target_names(node):
            if name in body_aliases:
                outer_aliases.add(name)
            else:
                outer_aliases.discard(name)
        self.aliases = outer_aliases

    def visit_ListComp(self, node: ast.ListComp) -> None:  # noqa: N802 - ast visitor API
        self._visit_comprehension(node)

    def visit_SetComp(self, node: ast.SetComp) -> None:  # noqa: N802 - ast visitor API
        self._visit_comprehension(node)

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:  # noqa: N802 - ast visitor API
        self._visit_comprehension(node)

    def visit_DictComp(self, node: ast.DictComp) -> None:  # noqa: N802 - ast visitor API
        self._visit_comprehension(node)

    def _visit_with(self, node: ast.With | ast.AsyncWith) -> bool:
        for item in node.items:
            self._record_handler_input()
            self.visit(item.context_expr)
            self._record_handler_input()
            if item.optional_vars is not None:
                self._bind_target_from_verdict(item.optional_vars, self._aliases_exception(item.context_expr))
        falls_through = self._visit_statements(node.body)
        self._record_handler_input()
        return falls_through

    def visit_With(self, node: ast.With) -> bool:  # noqa: N802 - ast visitor API
        return self._visit_with(node)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> bool:  # noqa: N802 - ast visitor API
        return self._visit_with(node)

    def visit_Match(self, node: ast.Match) -> bool:  # noqa: N802 - ast visitor API
        self.visit(node.subject)
        incoming = self.aliases.copy()
        subject_aliases_exception = self._aliases_exception(node.subject)
        surviving: set[str] = set()
        exhaustive = False
        falls_through = False
        for case in node.cases:
            self.aliases = incoming.copy()
            self._bind_names(match_capture_names(case.pattern), subject_aliases_exception)
            if case.guard is not None:
                self.visit(case.guard)
            case_falls_through = self._visit_statements(case.body)
            if case_falls_through:
                surviving |= self.aliases
                falls_through = True
            exhaustive |= case.guard is None and pattern_is_irrefutable(case.pattern)
        if not exhaustive:
            surviving |= incoming
            falls_through = True
        self.aliases = surviving
        return falls_through

    def _visit_try_handlers(
        self, handlers: list[ast.ExceptHandler], handler_input: set[str]
    ) -> tuple[list[set[str]], set[str]]:
        normal_states: list[set[str]] = []
        exceptional_aliases: set[str] = set()
        for handler in handlers:
            self.aliases = handler_input.copy()
            handler_exception_input: set[str] = set()
            self._try_exception_inputs.append(handler_exception_input)
            handler_falls_through = self.visit(handler) is not False
            self._try_exception_inputs.pop()
            if handler.name is not None:
                handler_exception_input.discard(handler.name)
            exceptional_aliases |= handler_exception_input
            if handler_falls_through:
                normal_states.append(self.aliases.copy())
        return normal_states, exceptional_aliases

    def _visit_try(self, node: ast.Try | ast.TryStar) -> bool:
        break_bucket = self._loop_break_aliases[-1] if self._loop_break_aliases else None
        continue_bucket = self._loop_continue_aliases[-1] if self._loop_continue_aliases else None
        break_start = len(break_bucket) if break_bucket is not None else 0
        continue_start = len(continue_bucket) if continue_bucket is not None else 0
        incoming = self.aliases.copy()
        handler_input = incoming.copy()

        self.aliases = incoming.copy()
        self._try_handler_inputs.append(handler_input)
        body_exception_input: set[str] = set()
        self._try_exception_inputs.append(body_exception_input)
        body_falls_through = self._visit_statements(node.body)
        self._try_exception_inputs.pop()
        handler_input = self._try_handler_inputs.pop()
        normal_states: list[set[str]] = []
        exceptional_aliases: set[str] = set()
        if not any(handler.type is None for handler in node.handlers):
            exceptional_aliases |= body_exception_input

        if body_falls_through:
            else_exception_input: set[str] = set()
            self._try_exception_inputs.append(else_exception_input)
            else_falls_through = self._visit_statements(node.orelse)
            self._try_exception_inputs.pop()
            exceptional_aliases |= else_exception_input
            if else_falls_through:
                normal_states.append(self.aliases.copy())

        handler_states, handler_exception_aliases = self._visit_try_handlers(node.handlers, handler_input)
        normal_states.extend(handler_states)
        exceptional_aliases |= handler_exception_aliases

        break_aliases = break_bucket[break_start:] if break_bucket is not None else []
        continue_aliases = continue_bucket[continue_start:] if continue_bucket is not None else []
        if break_bucket is not None:
            del break_bucket[break_start:]
        if continue_bucket is not None:
            del continue_bucket[continue_start:]

        normal_falls_through = False
        normal_aliases: set[str] = set()
        if normal_states:
            self.aliases = set().union(*normal_states)
            normal_falls_through = self._visit_statements(node.finalbody)
            if normal_falls_through:
                normal_aliases = self.aliases.copy()

        propagated_exception_aliases: set[str] = set()
        if exceptional_aliases:
            self.aliases = exceptional_aliases
            if self._visit_statements(node.finalbody):
                propagated_exception_aliases = self.aliases.copy()
        if propagated_exception_aliases and self._try_exception_inputs:
            self._try_exception_inputs[-1].update(propagated_exception_aliases)
        if propagated_exception_aliases and self._try_handler_inputs:
            self._try_handler_inputs[-1].update(propagated_exception_aliases)

        for bucket, states in ((break_bucket, break_aliases), (continue_bucket, continue_aliases)):
            if bucket is None or not states:
                continue
            self.aliases = set().union(*states)
            if self._visit_statements(node.finalbody):
                bucket.append(self.aliases.copy())

        self.aliases = normal_aliases
        return normal_falls_through

    def visit_Try(self, node: ast.Try) -> bool:  # noqa: N802 - ast visitor API
        return self._visit_try(node)

    def visit_TryStar(self, node: ast.TryStar) -> bool:  # noqa: N802 - ast visitor API
        return self._visit_try(node)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> bool:  # noqa: N802 - ast visitor API
        handler_name = node.name
        if handler_name is not None:
            self.aliases.add(handler_name)
        falls_through = self._visit_statements(node.body)
        if handler_name is not None:
            self.aliases.discard(handler_name)
        return falls_through

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


@pytest.mark.parametrize("exit_statement", ["return", "raise RuntimeError"], ids=["return", "raise"])
def test_raw_exception_log_guard_drops_a_returned_or_raised_branch(exit_statement: str) -> None:
    """A branch that leaves through `return` or `raise` cannot reach the statement after it.

    Only break and continue answered the fall-through protocol, so the scanner merged the
    exceptional state of an unreachable path and reported a renderer that cannot run.
    """
    unreachable = (
        "def handler():\n"
        "    try:\n"
        "        work()\n"
        "    except Exception as exc:\n"
        '        detail = "authored detail"\n'
        "        if condition:\n"
        "            detail = exc\n"
        f"            {exit_statement}\n"
        '        logger.warning("event", detail=detail)\n'
    )
    reachable = unreachable.replace(f"            {exit_statement}\n", "")

    assert _raw_log_exception_renderers(unreachable) == []
    assert _raw_log_exception_renderers(reachable) == [8]


def test_raw_exception_log_guard_still_reads_a_returned_expression() -> None:
    """Cutting the path must not stop the scanner reading what the statement itself renders."""
    source = 'def handler():\n    try:\n        work()\n    except Exception as exc:\n        return logger.warning("event", detail=exc)\n'
    assert _raw_log_exception_renderers(source) == [5]


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


@pytest.mark.parametrize(
    ("source", "expected_line"),
    [
        (
            """\
for detail in [exc]:
    logger.warning("event", detail=detail)
""",
            2,
        ),
        (
            """\
for detail, authored in [(exc, "authored detail")]:
    logger.warning("event", detail=authored)
""",
            2,
        ),
        (
            """\
for [detail, authored] in [(exc, "authored detail")]:
    logger.warning("event", detail=authored)
""",
            2,
        ),
        (
            """\
async def run():
    async for detail in exception_stream(exc):
        logger.warning("event", detail=detail)
""",
            3,
        ),
    ],
)
def test_raw_exception_log_guard_tracks_loop_target_aliases(source: str, expected_line: int) -> None:
    assert _raw_log_exception_renderers(source) == [expected_line]


def test_raw_exception_log_guard_clears_loop_targets_bound_from_clean_iterables() -> None:
    source = """\
detail = exc
for detail in ["authored detail"]:
    logger.warning("event", detail=detail)
"""

    assert _raw_log_exception_renderers(source) == []


@pytest.mark.parametrize("exit_statement", ["break", "continue"])
def test_raw_exception_log_guard_preserves_loop_exit_aliases(exit_statement: str) -> None:
    source = f"""\
for item in items:
    detail = exc
    {exit_statement}
    detail = "authored detail"
logger.warning("event", detail=detail)
"""

    assert _raw_log_exception_renderers(source) == [5]


def test_raw_exception_log_guard_checks_aliases_from_previous_loop_iterations() -> None:
    source = """\
for item in items:
    logger.warning("event", detail=detail)
    detail = exc
"""

    assert _raw_log_exception_renderers(source) == [2]


@pytest.mark.parametrize("exit_statement", ["break", "continue"])
def test_raw_exception_log_guard_accepts_loop_paths_that_all_clear_aliases(exit_statement: str) -> None:
    source = f"""\
detail = exc
for item in items:
    detail = "authored detail"
    {exit_statement}
else:
    detail = "also authored"
logger.warning("event", detail=detail)
"""

    assert _raw_log_exception_renderers(source) == []


@pytest.mark.parametrize("handler_keyword", ["except", "except*"])
@pytest.mark.parametrize("exit_statement", ["break", "continue"])
def test_raw_exception_log_guard_checks_finally_for_loop_exit(handler_keyword: str, exit_statement: str) -> None:
    source = f"""\
for item in items:
    detail = "authored detail"
    try:
        if condition:
            detail = exc
            {exit_statement}
    {handler_keyword} Exception:
        detail = "authored detail"
    finally:
        logger.warning("event", detail=detail)
"""

    assert _raw_log_exception_renderers(source) == [10]


def test_raw_exception_log_guard_finally_can_replace_a_pending_loop_exit() -> None:
    source = """\
detail = exc
for item in items:
    try:
        break
    finally:
        detail = "authored detail"
        continue
else:
    detail = "also authored"
logger.warning("event", detail=detail)
"""

    assert _raw_log_exception_renderers(source) == []


@pytest.mark.parametrize(
    "source",
    [
        '[logger.warning("event", detail=detail) for detail in [exc]]',
        '{logger.warning("event", detail=detail) for detail in [exc]}',
        '{detail: logger.warning("event", detail=detail) for detail in [exc]}',
        '(logger.warning("event", detail=detail) for detail in [exc])',
    ],
)
def test_raw_exception_log_guard_tracks_comprehension_target_aliases(source: str) -> None:
    assert _raw_log_exception_renderers(source) == [1]


def test_raw_exception_log_guard_isolates_comprehension_target_aliases() -> None:
    source = """\
detail = exc
[logger.warning("event", detail=detail) for detail in ["authored detail"]]
logger.warning("event", detail=detail)
"""

    assert _raw_log_exception_renderers(source) == [3]


@pytest.mark.parametrize(
    ("source", "expected_line"),
    [
        (
            """\
with exc as detail:
    logger.warning("event", detail=detail)
""",
            2,
        ),
        (
            """\
with exc as (detail, authored):
    logger.warning("event", detail=authored)
""",
            2,
        ),
        (
            """\
async def run():
    async with exc as detail:
        logger.warning("event", detail=detail)
""",
            3,
        ),
    ],
)
def test_raw_exception_log_guard_tracks_with_target_aliases(source: str, expected_line: int) -> None:
    assert _raw_log_exception_renderers(source) == [expected_line]


def test_raw_exception_log_guard_clears_with_targets_bound_from_clean_contexts() -> None:
    source = """\
detail = exc
with context() as detail:
    logger.warning("event", detail=detail)
"""

    assert _raw_log_exception_renderers(source) == []


def test_raw_exception_log_guard_tracks_walrus_aliases() -> None:
    source = """\
if detail := exc:
    logger.warning("event", detail=detail)
"""

    assert _raw_log_exception_renderers(source) == [2]


def test_raw_exception_log_guard_clears_walrus_targets_bound_from_clean_values() -> None:
    source = """\
detail = exc
if detail := "authored detail":
    logger.warning("event", detail=detail)
"""

    assert _raw_log_exception_renderers(source) == []


@pytest.mark.parametrize(
    ("source", "expected_line"),
    [
        (
            """\
match exc:
    case detail:
        logger.warning("event", detail=detail)
""",
            3,
        ),
        (
            """\
match [exc, "authored detail"]:
    case [detail, authored]:
        logger.warning("event", detail=authored)
""",
            3,
        ),
        (
            """\
match [exc]:
    case [*details]:
        logger.warning("event", detail=details)
""",
            3,
        ),
        (
            """\
match {"detail": exc}:
    case {"detail": detail, **remaining}:
        logger.warning("event", detail=remaining)
""",
            3,
        ),
    ],
)
def test_raw_exception_log_guard_tracks_match_capture_aliases(source: str, expected_line: int) -> None:
    assert _raw_log_exception_renderers(source) == [expected_line]


def test_raw_exception_log_guard_clears_irrefutable_match_captures_bound_from_clean_subjects() -> None:
    source = """\
detail = exc
match "authored detail":
    case detail:
        pass
logger.warning("event", detail=detail)
"""

    assert _raw_log_exception_renderers(source) == []


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


def test_raw_exception_log_guard_preserves_the_post_with_body_state_for_exit_failures() -> None:
    source = """\
alias = "authored detail"
try:
    with context():
        alias = exc
except Exception:
    pass
else:
    alias = "authored detail"
logger.warning("event", detail=alias)
"""

    assert _raw_log_exception_renderers(source) == [9]


@pytest.mark.parametrize(
    "source",
    [
        """\
alias = "authored detail"
try:
    with context():
        alias = exc
        if condition:
            alias = "authored detail"
except Exception:
    pass
else:
    alias = "authored detail"
logger.warning("event", detail=alias)
""",
        """\
alias = "authored detail"
try:
    with context():
        alias = exc
        alias = "authored detail"
        alias = exc
except Exception:
    pass
else:
    alias = "authored detail"
logger.warning("event", detail=alias)
""",
    ],
)
def test_raw_exception_log_guard_preserves_taint_after_unsafe_context_overwrites(source: str) -> None:
    assert _raw_log_exception_renderers(source) == [11]


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
    assert "nso_adapter/core/removal.py" in outcome_paths
    # The C1c round widened the allowlist to the sinks that log a raised NSO/NetBox call:
    # each takes the device or interface identity as an argument, so the error repeats it.
    assert {
        "nso_adapter/core/failover.py",
        "nso_adapter/bindings/netbox/client.py",
        "nso_adapter/bindings/netbox/writer.py",
    } <= outcome_paths
    assert {path.name for path in _GUARDED_LOG_SINKS} <= {path.rsplit("/", maxsplit=1)[-1] for path in outcome_paths}
    identifier_paths = set(rules["nso-diagnostic-raw-identifier"]["paths"]["include"])
    assert {
        "nso_adapter/core/importer.py",
        "nso_adapter/core/redistribution.py",
        "nso_adapter/nso/client.py",
    } <= identifier_paths


def test_the_raised_message_guard_keeps_its_vocabulary_and_its_narrow_sink() -> None:
    """The raise guard is pinned whole: a silent narrowing is how this class came back before.

    Both sinks matter. It sinks on the MESSAGE, so a `detail=` field stays a separate
    question, and it carries no module allowlist, so the class cannot be scoped away one
    module at a time.
    """
    rules = {rule["id"]: rule for rule in yaml.safe_load(_RULES.read_text(encoding="utf-8"))["rules"]}
    rule = rules["nso-raised-message-raw-identifier"]

    assert set(_rule_patterns(rule["pattern-sources"])) == {
        "$D.nso_device_name",
        "$D.ned_id",
        "$D.sw_version",
        "device_name",
        "stream_url",
    }
    assert set(_rule_patterns(rule["pattern-sanitizers"])) == {"$D.id", "$RESPONSE.status_code"}, (
        "a response body is NOT sanitized: a device-named URL lets the server echo the name back"
    )
    assert "paths" not in rule, "the class applies to every module; an allowlist would scope it away"

    sink = rule["pattern-sinks"][0]["patterns"]
    assert {"pattern-inside": "raise $EXC(...)"} in sink, "the sink must stay inside a raise"
    assert set(_rule_patterns(sink)) == {
        'f"..."',
        "$TEMPLATE.format(...)",
        "$TEMPLATE % $VALUES",
        "$LEFT + $RIGHT",
    }, "the sink is the message expression, never the whole raise"


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


def _binds_formatter_name(node: ast.AST, name: str = "failure_detail") -> bool:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return node.name == name
    if isinstance(node, ast.Name):
        return node.id == name and isinstance(node.ctx, (ast.Store, ast.Del))
    if isinstance(node, ast.alias):
        imported_name = node.asname or node.name.split(".", maxsplit=1)[0]
        return imported_name == name or node.name == "*"
    return False


def _formatter_definition_ast(source: str, name: str = "failure_detail") -> str:
    """Return the one effective formatter definition with its docstring normalized."""
    tree = ast.parse(source)
    bindings = [node for node in ast.walk(tree) if _binds_formatter_name(node, name)]
    if len(bindings) != 1 or bindings[0] not in tree.body or not isinstance(bindings[0], ast.FunctionDef):
        raise ValueError(f"{name} must have one direct module function binding")
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
_APPROVED_FAILURE_DETAIL_AST = _formatter_definition_ast(_APPROVED_FAILURE_DETAIL)


_APPROVED_REJECTION_DETAIL = '''\
def rejection_detail(body: object) -> str:
    """Approved formatter contract."""
    if isinstance(body, dict):
        names = sorted(str(key) for key in body)
        return f"fields: {', '.join(names)}" if names else "fields: none"
    if isinstance(body, list):
        return f"errors: {len(body)}"
    return "unparsed"
'''
_APPROVED_REJECTION_DETAIL_AST = _formatter_definition_ast(_APPROVED_REJECTION_DETAIL, "rejection_detail")


def test_failure_detail_reads_only_closed_exception_properties() -> None:
    """Any executable change to the ratified formatter shape requires an explicit review."""
    actual = _formatter_definition_ast(_NSO_CLIENT.read_text(encoding="utf-8"))

    assert actual == _APPROVED_FAILURE_DETAIL_AST


def test_rejection_detail_keeps_only_the_field_names_of_a_rejection_body() -> None:
    """The NetBox rejection classifier is pinned: its messages carry the submitted values."""
    actual = _formatter_definition_ast(_NETBOX_CLIENT.read_text(encoding="utf-8"), "rejection_detail")

    assert actual == _APPROVED_REJECTION_DETAIL_AST


@pytest.mark.parametrize(
    "unsafe_body",
    [
        "    return str(body)\n",
        '    return f"{body}"\n',
        '    if isinstance(body, dict):\n        return ", ".join(f"{k}={v}" for k, v in body.items())\n    return "unparsed"\n',
    ],
)
def test_rejection_detail_guard_rejects_unratified_shapes(unsafe_body: str) -> None:
    candidate = f"def rejection_detail(body):\n{unsafe_body}"

    assert _formatter_definition_ast(candidate, "rejection_detail") != _APPROVED_REJECTION_DETAIL_AST


def test_rejection_detail_guard_rejects_an_alternate_binding() -> None:
    rebind = "\nrejection_detail = lambda body: str(body)\n"

    with pytest.raises(ValueError, match="one direct module function binding"):
        _formatter_definition_ast(_APPROVED_REJECTION_DETAIL + rebind, "rejection_detail")


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

    assert _formatter_definition_ast(candidate) != _APPROVED_FAILURE_DETAIL_AST


@pytest.mark.parametrize(
    "rebind",
    [
        "\ndef failure_detail(exc):\n    return exc.args[0]\n",
        "\nfailure_detail = lambda exc: exc.args[0]\n",
    ],
)
def test_failure_detail_guard_rejects_an_alternate_binding(rebind: str) -> None:
    with pytest.raises(ValueError, match="one direct module function binding"):
        _formatter_definition_ast(_APPROVED_FAILURE_DETAIL + rebind)


@pytest.mark.parametrize(
    "rebind",
    [
        "\nif True:\n    def failure_detail(exc):\n        return str(exc)\n",
        "\ntry:\n    from unsafe_it import *\nexcept ImportError:\n    pass\n",
    ],
)
def test_failure_detail_guard_rejects_a_conditional_binding(rebind: str) -> None:
    with pytest.raises(ValueError, match="one direct module function binding"):
        _formatter_definition_ast(_APPROVED_FAILURE_DETAIL + rebind)


def test_failure_detail_guard_rejects_a_decorator() -> None:
    decorated = _APPROVED_FAILURE_DETAIL.replace("def failure_detail", "@unsafe\ndef failure_detail", 1)

    assert _formatter_definition_ast(decorated) != _APPROVED_FAILURE_DETAIL_AST


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
        work()
        detail = "authored"
    {handler} ValueError:
        logger.warning("event", detail=detail)
"""
    assert _raw_log_exception_renderers(source.format(handler=handler)) == [7]


def test_a_finally_block_sees_an_alias_from_an_earlier_try_body_prefix() -> None:
    source = """\
def f():
    try:
        detail = exc
        work()
        detail = "authored"
    finally:
        logger.warning("event", detail=detail)
"""
    assert _raw_log_exception_renderers(source) == [7]


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
