# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""No ``raise X`` and no ``raise X from None`` inside an ``except`` handler.

Both spellings leave the caught exception on ``__context__``. A plain ``raise X`` attaches it
implicitly; ``from None`` attaches it and only sets ``__suppress_context__``. Either way any
secret in the caught exception survives every surface that walks the chain, and the traceback
module still prints it when a caller asks for the full chain. Suppression is not removal, and
saying nothing is not suppression.

Capture what the handler needs in a local (the sanitized exception itself is the usual one),
then raise AFTER the handler, where the interpreter attaches nothing. A raise outside a
handler has no context to attach and is not flagged, a bare ``raise`` re-raises the caught
exception on purpose, and ``raise X from exc`` chains it deliberately.

The rule is about what RUNS while the handler is active, not about what is written inside it:

* ``raise X from <name>`` where the name is bound to ``None`` is ``from None`` spelled with
  an alias, and it attaches exactly the same way.
* calling a helper that always raises is the same ``raise X`` one frame down: the interpreter
  attaches the caught exception to whatever the helper raised, and calling it under an alias
  bound by ``other = helper`` is the same call under another name.
* a function DEFINED in a handler runs its decorators and its default expressions THERE,
  and its body nowhere. Called after the handler exits, its raise attaches nothing; called
  inside it, it is the helper case above.

Every self-test below is checked against the interpreter first, so the analyzer is measured
against real ``__context__`` behaviour instead of against a claim about it.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

_PACKAGE = Path(__file__).resolve().parents[1] / "nso_adapter"

_FUNCTION_NODES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)


def _as_invocation(decorator: ast.expr) -> ast.expr:
    """``@name`` is ``name(fn)`` with the call left implicit; spell it as the call it is.

    ``@name(...)`` is already an ``ast.Call``, and any other decorator expression names no
    local helper, so both are returned unchanged.
    """
    if not isinstance(decorator, ast.Name):
        return decorator
    return ast.copy_location(ast.Call(func=decorator, args=[], keywords=[]), decorator)


def _definition_time_nodes(node: ast.AST) -> list[ast.AST]:
    """The parts of a function definition the interpreter evaluates where it is WRITTEN.

    Decorators and default expressions run at definition time, so one written in a handler
    runs in the handler. Applying a decorator CALLS it, with or without parentheses.
    A lambda has defaults but no decorators.
    """
    args = node.args
    defaults = [default for default in (*args.defaults, *args.kw_defaults) if default is not None]
    decorators = [_as_invocation(decorator) for decorator in getattr(node, "decorator_list", ())]
    return [*decorators, *defaults]


def _executes_in_handler(handler: ast.ExceptHandler) -> Iterator[ast.AST]:
    """Every node that RUNS while *handler* is active.

    A nested function or lambda BODY does not: defining it executes nothing there, and
    calling it later runs where the interpreter has no exception to attach. Walking into
    those bodies rejects code that is correct. What the definition itself evaluates —
    decorators and defaults — does run here, so those are walked.
    """
    stack: list[ast.AST] = list(handler.body)
    while stack:
        node = stack.pop()
        if isinstance(node, _FUNCTION_NODES):
            stack.extend(_definition_time_nodes(node))
            continue
        if isinstance(node, ast.GeneratorExp):
            stack.append(node.generators[0].iter)
            continue
        yield node
        stack.extend(ast.iter_child_nodes(node))


_UNKNOWN = object()
_MISSING = object()
_NONE = object()
_Function = ast.FunctionDef | ast.AsyncFunctionDef


@dataclass(frozen=True)
class _Alias:
    """One simple alias expression and the scope where Python evaluates it."""

    value: ast.Name
    scope: ast.AST


def _target_names(target: ast.AST) -> set[str]:
    """Return every plain name an assignment target binds."""
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, (ast.Tuple, ast.List)):
        return {name for item in target.elts for name in _target_names(item)}
    if isinstance(target, ast.Starred):
        return _target_names(target.value)
    return set()


def _pattern_names(pattern: ast.pattern) -> set[str]:
    """Return the names a structural pattern captures in its lexical scope."""
    names: set[str] = set()
    for part in ast.walk(pattern):
        if isinstance(part, (ast.MatchAs, ast.MatchStar)) and part.name:
            names.add(part.name)
        elif isinstance(part, ast.MatchMapping) and part.rest:
            names.add(part.rest)
    return names


class _LexicalIndex:
    """Resolve the guard's supported local calls to one function definition.

    The index is deliberately conservative. A reassignment, conflicting binding,
    unsupported decorator, or shadowed receiver makes that symbol unknown. It does
    not guess from a same-spelled definition in another scope.
    """

    def __init__(self, tree: ast.Module):
        self.tree = tree
        self.scope_of: dict[ast.AST, ast.AST] = {tree: tree}
        self.parent: dict[ast.AST, ast.AST] = {}
        self.scope_parent: dict[ast.AST, ast.AST | None] = {tree: None}
        self.bindings: dict[ast.AST, dict[str, object]] = {tree: {}}
        self.binding_count: dict[tuple[ast.AST, str], int] = {}
        self.functions: list[_Function] = []
        self.comprehension_scopes: set[ast.AST] = set()
        self.global_names: dict[ast.AST, set[str]] = {}
        self.nonlocal_names: dict[ast.AST, set[str]] = {}
        for statement in tree.body:
            self._walk(statement, tree, tree)

    def _record(self, scope: ast.AST, name: str, value: object) -> None:
        key = (scope, name)
        self.binding_count[key] = self.binding_count.get(key, 0) + 1
        bindings = self.bindings.setdefault(scope, {})
        bindings[name] = value if name not in bindings else _UNKNOWN

    def _assign(self, scope: ast.AST, name: str, value: object) -> None:
        """Record a binding in the scope selected by global or nonlocal."""
        if name in self.global_names.get(scope, set()):
            self._record(self.tree, name, value)
            return
        if name in self.nonlocal_names.get(scope, set()):
            outer = self.scope_parent.get(scope)
            while outer is not None:
                if isinstance(outer, _FUNCTION_NODES):
                    self._record(outer, name, _UNKNOWN)
                outer = self.scope_parent.get(outer)
            return
        self._record(scope, name, value)

    def _mark(self, node: ast.AST, scope: ast.AST, parent: ast.AST) -> None:
        self.scope_of[node] = scope
        self.parent[node] = parent

    def _walk(self, node: ast.AST, scope: ast.AST, parent: ast.AST) -> None:
        self._mark(node, scope, parent)
        handler = getattr(self, f"_walk_{type(node).__name__}", self._walk_children)
        handler(node, scope)

    def _walk_children(self, node: ast.AST, scope: ast.AST) -> None:
        for child in ast.iter_child_nodes(node):
            self._walk(child, scope, node)

    @staticmethod
    def _arguments(node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda) -> tuple[ast.arg, ...]:
        arguments = (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs)
        if node.args.vararg is not None:
            arguments = (*arguments, node.args.vararg)
        if node.args.kwarg is not None:
            arguments = (*arguments, node.args.kwarg)
        return arguments

    def _walk_function(self, node: _Function, scope: ast.AST) -> None:
        self._assign(scope, node.name, node)
        self.functions.append(node)
        for expression in (*node.decorator_list, *node.args.defaults, *node.args.kw_defaults):
            if expression is not None:
                self._walk(expression, scope, node)
        self.scope_parent[node] = scope
        self.bindings[node] = {}
        for argument in self._arguments(node):
            self._record(node, argument.arg, _UNKNOWN)
        for statement in node.body:
            self._walk(statement, node, node)

    def _walk_FunctionDef(self, node: ast.FunctionDef, scope: ast.AST) -> None:
        self._walk_function(node, scope)

    def _walk_AsyncFunctionDef(self, node: ast.AsyncFunctionDef, scope: ast.AST) -> None:
        self._walk_function(node, scope)

    def _walk_Lambda(self, node: ast.Lambda, scope: ast.AST) -> None:
        for expression in (*node.args.defaults, *node.args.kw_defaults):
            if expression is not None:
                self._walk(expression, scope, node)
        self.scope_parent[node] = scope
        self.bindings[node] = {}
        for argument in self._arguments(node):
            self._record(node, argument.arg, _UNKNOWN)
        self._walk(node.body, node, node)

    def _walk_ClassDef(self, node: ast.ClassDef, scope: ast.AST) -> None:
        self._assign(scope, node.name, _UNKNOWN)
        for expression in (*node.decorator_list, *node.bases, *(keyword.value for keyword in node.keywords)):
            self._walk(expression, scope, node)
        self.scope_parent[node] = scope
        self.bindings[node] = {}
        for statement in node.body:
            self._walk(statement, node, node)

    def _walk_ListComp(self, node: ast.ListComp, scope: ast.AST) -> None:
        self._walk_comprehension(node, scope)

    def _walk_SetComp(self, node: ast.SetComp, scope: ast.AST) -> None:
        self._walk_comprehension(node, scope)

    def _walk_GeneratorExp(self, node: ast.GeneratorExp, scope: ast.AST) -> None:
        self._walk_comprehension(node, scope)

    def _walk_DictComp(self, node: ast.DictComp, scope: ast.AST) -> None:
        self._walk_comprehension(node, scope)

    def _walk_Assign(self, node: ast.Assign, scope: ast.AST) -> None:
        self._walk(node.value, scope, node)
        for target in node.targets:
            self._walk(target, scope, node)
        alias = _Alias(node.value, scope) if len(node.targets) == 1 and isinstance(node.value, ast.Name) else None
        value = _NONE if isinstance(node.value, ast.Constant) and node.value.value is None else alias or _UNKNOWN
        for target in node.targets:
            for name in _target_names(target):
                self._assign(scope, name, value if isinstance(target, ast.Name) else _UNKNOWN)

    def _walk_AnnAssign(self, node: ast.AnnAssign, scope: ast.AST) -> None:
        if node.value is not None:
            self._walk(node.value, scope, node)
        self._walk(node.target, scope, node)
        alias = _Alias(node.value, scope) if isinstance(node.value, ast.Name) else None
        value = _NONE if isinstance(node.value, ast.Constant) and node.value.value is None else alias or _UNKNOWN
        for name in _target_names(node.target):
            self._assign(scope, name, value if isinstance(node.target, ast.Name) else _UNKNOWN)

    def _walk_NamedExpr(self, node: ast.NamedExpr, scope: ast.AST) -> None:
        self._walk(node.value, scope, node)
        binding_scope = scope
        while binding_scope in self.comprehension_scopes:
            binding_scope = self.scope_parent[binding_scope]
        self._walk(node.target, binding_scope, node)
        for name in _target_names(node.target):
            self._assign(binding_scope, name, _UNKNOWN)

    def _walk_Match(self, node: ast.Match, scope: ast.AST) -> None:
        self._walk(node.subject, scope, node)
        for case in node.cases:
            self._walk(case.pattern, scope, node)
            for name in _pattern_names(case.pattern):
                self._assign(scope, name, _UNKNOWN)
            if case.guard is not None:
                self._walk(case.guard, scope, node)
            for statement in case.body:
                self._walk(statement, scope, node)

    def _walk_for(self, node: ast.For | ast.AsyncFor, scope: ast.AST) -> None:
        self._walk(node.iter, scope, node)
        self._walk(node.target, scope, node)
        for name in _target_names(node.target):
            self._assign(scope, name, _UNKNOWN)
        for statement in (*node.body, *node.orelse):
            self._walk(statement, scope, node)

    def _walk_For(self, node: ast.For, scope: ast.AST) -> None:
        self._walk_for(node, scope)

    def _walk_AsyncFor(self, node: ast.AsyncFor, scope: ast.AST) -> None:
        self._walk_for(node, scope)

    def _walk_with(self, node: ast.With | ast.AsyncWith, scope: ast.AST) -> None:
        for item in node.items:
            self._walk(item.context_expr, scope, node)
            if item.optional_vars is not None:
                self._walk(item.optional_vars, scope, node)
                for name in _target_names(item.optional_vars):
                    self._assign(scope, name, _UNKNOWN)
        for statement in node.body:
            self._walk(statement, scope, node)

    def _walk_With(self, node: ast.With, scope: ast.AST) -> None:
        self._walk_with(node, scope)

    def _walk_AsyncWith(self, node: ast.AsyncWith, scope: ast.AST) -> None:
        self._walk_with(node, scope)

    def _walk_ExceptHandler(self, node: ast.ExceptHandler, scope: ast.AST) -> None:
        if node.type is not None:
            self._walk(node.type, scope, node)
        if node.name:
            self._assign(scope, node.name, _UNKNOWN)
        for statement in node.body:
            self._walk(statement, scope, node)

    def _walk_import(self, node: ast.Import | ast.ImportFrom, scope: ast.AST) -> None:
        for alias in node.names:
            self._assign(scope, alias.asname or alias.name.split(".", 1)[0], _UNKNOWN)

    def _walk_Import(self, node: ast.Import, scope: ast.AST) -> None:
        self._walk_import(node, scope)

    def _walk_ImportFrom(self, node: ast.ImportFrom, scope: ast.AST) -> None:
        self._walk_import(node, scope)

    def _walk_Global(self, node: ast.Global, scope: ast.AST) -> None:
        self.global_names.setdefault(scope, set()).update(node.names)

    def _walk_Nonlocal(self, node: ast.Nonlocal, scope: ast.AST) -> None:
        self.nonlocal_names.setdefault(scope, set()).update(node.names)

    def _walk_mutating_target(self, node: ast.AugAssign | ast.Delete, scope: ast.AST) -> None:
        self._walk_children(node, scope)
        targets = (node.target,) if isinstance(node, ast.AugAssign) else node.targets
        for target in targets:
            for name in _target_names(target):
                self._assign(scope, name, _UNKNOWN)

    def _walk_AugAssign(self, node: ast.AugAssign, scope: ast.AST) -> None:
        self._walk_mutating_target(node, scope)

    def _walk_Delete(self, node: ast.Delete, scope: ast.AST) -> None:
        self._walk_mutating_target(node, scope)

    def _walk_comprehension(self, node: ast.AST, outer: ast.AST) -> None:
        generators = node.generators
        self._walk(generators[0].iter, outer, node)
        self.scope_parent[node] = outer
        self.bindings[node] = {}
        self.comprehension_scopes.add(node)
        for index, generator in enumerate(generators):
            if index:
                self._walk(generator.iter, node, generator)
            self._walk(generator.target, node, generator)
            for name in _target_names(generator.target):
                self._record(node, name, _UNKNOWN)
            for condition in generator.ifs:
                self._walk(condition, node, generator)
        values = (node.key, node.value) if isinstance(node, ast.DictComp) else (node.elt,)
        for value in values:
            self._walk(value, node, node)

    def scope(self, node: ast.AST) -> ast.AST:
        """Return the evaluation scope, including synthetic bare-decorator calls."""
        scope = self.scope_of.get(node)
        if scope is None and isinstance(node, ast.Call):
            scope = self.scope_of.get(node.func)
        return scope or self.tree

    def _is_function_scope(self, scope: ast.AST) -> bool:
        return isinstance(scope, _FUNCTION_NODES) or scope in self.comprehension_scopes

    def _binding(self, name: str, start: ast.AST) -> object:
        return self._resolve_alias(self._binding_without_alias(name, start), set())

    def _resolve_alias(self, value: object, seen: set[tuple[ast.AST, str]]) -> object:
        if not isinstance(value, _Alias):
            return value
        key = (value.scope, value.value.id)
        if key in seen:
            return _UNKNOWN
        seen.add(key)
        target = self._binding_without_alias(value.value.id, value.scope)
        return self._resolve_alias(target, seen)

    def _binding_without_alias(self, name: str, start: ast.AST) -> object:
        scope: ast.AST | None = start
        started_in_function = self._is_function_scope(start)
        while scope is not None:
            if isinstance(scope, ast.ClassDef) and (started_in_function or scope is not start):
                scope = self.scope_parent.get(scope)
                continue
            if scope is not self.tree and name in self.global_names.get(scope, set()):
                scope = self.tree
                continue
            if name in self.nonlocal_names.get(scope, set()):
                scope = self.scope_parent.get(scope)
                continue
            value = self.bindings.get(scope, {}).get(name, _MISSING)
            if value is not _MISSING:
                return value
            if self._is_function_scope(scope):
                started_in_function = True
            scope = self.scope_parent.get(scope)
        return _MISSING

    def _descriptor(self, node: _Function) -> str:
        owner = self.scope_parent[node]
        if not isinstance(owner, ast.ClassDef):
            return "function" if not node.decorator_list else "decorated"
        if not node.decorator_list:
            return "instance"
        if len(node.decorator_list) != 1 or not isinstance(node.decorator_list[0], ast.Name):
            return "decorated"
        name = node.decorator_list[0].id
        if name not in {"classmethod", "staticmethod"} or self._binding(name, owner) is not _MISSING:
            return "decorated"
        return "class" if name == "classmethod" else "static"

    def classifiable(self, node: _Function) -> bool:
        """Whether calling this definition reaches its body with supported semantics."""
        return self._descriptor(node) != "decorated"

    def _receiver_class(self, name: str, start: ast.AST) -> ast.ClassDef | None:
        scope: ast.AST | None = start
        started_in_function = self._is_function_scope(start)
        while scope is not None:
            if isinstance(scope, ast.ClassDef) and (started_in_function or scope is not start):
                scope = self.scope_parent.get(scope)
                continue
            if scope is not self.tree and name in self.global_names.get(scope, set()):
                scope = self.tree
                continue
            if name in self.nonlocal_names.get(scope, set()):
                scope = self.scope_parent.get(scope)
                continue
            if name in self.bindings.get(scope, {}):
                if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    owner = self.scope_parent[scope]
                    positional = (*scope.args.posonlyargs, *scope.args.args)
                    descriptor = self._descriptor(scope)
                    expected = "self" if descriptor == "instance" else "cls" if descriptor == "class" else None
                    if (
                        isinstance(owner, ast.ClassDef)
                        and expected == name
                        and positional
                        and positional[0].arg == name
                        and self.binding_count.get((scope, name)) == 1
                    ):
                        return owner
                return None
            if self._is_function_scope(scope):
                started_in_function = True
            scope = self.scope_parent.get(scope)
        return None

    def resolve(self, expression: ast.expr, scope: ast.AST) -> _Function | None:
        """Resolve one supported callable expression to its exact definition."""
        if isinstance(expression, ast.Name):
            value = self._binding(expression.id, scope)
        elif (
            isinstance(expression, ast.Attribute)
            and isinstance(expression.value, ast.Name)
            and expression.value.id in {"self", "cls"}
        ):
            owner = self._receiver_class(expression.value.id, scope)
            value = self.bindings.get(owner, {}).get(expression.attr, _MISSING) if owner is not None else _MISSING
            value = self._resolve_alias(value, set())
        else:
            return None
        return value if isinstance(value, (ast.FunctionDef, ast.AsyncFunctionDef)) else None

    def is_none(self, expression: ast.Name) -> bool:
        """Whether this name resolves to a scoped literal-None binding."""
        return self._binding(expression.id, self.scope(expression)) is _NONE


def _executes_during_call(node: _Function) -> Iterator[ast.AST]:
    """Yield nodes executed by a call, pruning only deferred function bodies."""
    stack: list[ast.AST] = list(node.body)
    while stack:
        child = stack.pop()
        if isinstance(child, _FUNCTION_NODES):
            stack.extend(_definition_time_nodes(child))
            continue
        yield child
        stack.extend(ast.iter_child_nodes(child))


def _terminal_call(node: _Function) -> tuple[ast.Call, bool] | None:
    """Return the final direct call and whether the function awaits it."""
    last = node.body[-1]
    if not isinstance(last, (ast.Expr, ast.Return)) or last.value is None:
        return None
    value = last.value
    awaited = isinstance(value, ast.Await)
    if awaited:
        value = value.value
    return (value, awaited) if isinstance(value, ast.Call) else None


def _call_raises_now(call: ast.Call, *, awaited: bool, index: _LexicalIndex, helpers: set[_Function]) -> bool:
    target = index.resolve(call.func, index.scope(call))
    if target not in helpers:
        return False
    return not isinstance(target, ast.AsyncFunctionDef) or awaited


def _always_raising_helpers(tree: ast.Module, index: _LexicalIndex) -> set[_Function]:
    """Return exact function definitions whose supported invocation always raises."""
    candidates: list[tuple[_Function, list[ast.AST]]] = [
        (node, list(_executes_during_call(node))) for node in index.functions if node.body and index.classifiable(node)
    ]
    helpers = {
        node
        for node, executed in candidates
        if not any(isinstance(child, (ast.Return, ast.Yield, ast.YieldFrom)) for child in executed)
        and isinstance(node.body[-1], ast.Raise)
        and _attaches_context(node.body[-1], index)
    }
    grown = True
    while grown:
        grown = False
        for node, executed in candidates:
            if node in helpers or any(isinstance(child, (ast.Yield, ast.YieldFrom)) for child in executed):
                continue
            terminal = _terminal_call(node)
            if terminal is None:
                continue
            last = node.body[-1]
            returns = [child for child in executed if isinstance(child, ast.Return)]
            if returns and not (isinstance(last, ast.Return) and returns == [last]):
                continue
            call, awaited = terminal
            if _call_raises_now(call, awaited=awaited, index=index, helpers=helpers):
                helpers.add(node)
                grown = True
    return helpers


def _attaches_context(node: ast.Raise, index: _LexicalIndex) -> bool:
    """Whether this raise leaves the caught exception on ``__context__``."""
    if node.exc is None:
        return False  # a bare `raise` re-raises the caught exception on purpose
    cause = node.cause
    if cause is None:
        return True  # the implicit half: the interpreter attaches it itself
    if isinstance(cause, ast.Constant):
        return cause.value is None  # `from None` only SUPPRESSES; the context stays
    return isinstance(cause, ast.Name) and index.is_none(cause)


def scan_source(source: str, path: str) -> list[str]:
    """Every context-attaching site that RUNS inside an except handler, as ``path:line``."""
    tree = ast.parse(source, filename=path)
    index = _LexicalIndex(tree)
    helpers = _always_raising_helpers(tree, index)
    lines: set[int] = set()
    for handler in ast.walk(tree):
        if not isinstance(handler, ast.ExceptHandler):
            continue
        for node in _executes_in_handler(handler):
            if isinstance(node, ast.Raise) and _attaches_context(node, index):
                lines.add(node.lineno)
            elif isinstance(node, ast.Call) and _call_raises_now(
                node,
                awaited=isinstance(index.parent.get(node), ast.Await),
                index=index,
                helpers=helpers,
            ):
                lines.add(node.lineno)
    return [f"{path}:{line}" for line in sorted(lines)]


# ── the guard ────────────────────────────────────────────────────────────────


def test_no_raise_inside_an_except_handler_keeps_the_caught_exception_attached() -> None:
    violations: list[str] = []
    for path in sorted(_PACKAGE.rglob("*.py")):
        violations.extend(scan_source(path.read_text(encoding="utf-8"), str(path.relative_to(_PACKAGE.parent))))
    assert not violations, (
        "a plain `raise X` attaches the caught exception to __context__ and `from None` only "
        "hides it — build the sanitized exception in the handler and raise it AFTER the "
        "handler: " + ", ".join(violations)
    )


# ── analyzer self-tests (real AST parsing) ───────────────────────────────────


def test_flags_a_direct_raise_from_none() -> None:
    source = "try:\n    f()\nexcept ValueError:\n    raise Boom() from None\n"
    assert scan_source(source, "t.py") == ["t.py:4"]


def test_flags_a_conditional_raise_from_none() -> None:
    source = "try:\n    f()\nexcept ValueError as exc:\n    if exc.args:\n        raise Boom() from None\n"
    assert scan_source(source, "t.py") == ["t.py:5"]


def test_flags_every_site_in_one_handler() -> None:
    source = "try:\n    f()\nexcept ValueError:\n    if x:\n        raise A() from None\n    raise B() from None\n"
    assert scan_source(source, "t.py") == ["t.py:5", "t.py:6"]


def test_flags_a_plain_raise() -> None:
    """The implicit half of the class: the interpreter attaches the caught exception itself."""
    source = "try:\n    f()\nexcept ValueError:\n    raise Boom()\n"
    assert scan_source(source, "t.py") == ["t.py:4"]


def test_flags_a_conditional_plain_raise() -> None:
    source = "try:\n    f()\nexcept ValueError as exc:\n    if exc.args:\n        raise Boom()\n"
    assert scan_source(source, "t.py") == ["t.py:5"]


def test_flags_both_spellings_in_one_handler() -> None:
    source = "try:\n    f()\nexcept ValueError:\n    if x:\n        raise A()\n    raise B() from None\n"
    assert scan_source(source, "t.py") == ["t.py:5", "t.py:6"]


def test_a_plain_raise_outside_a_handler_stays_legal() -> None:
    """Nothing is being handled, so the interpreter has nothing to attach."""
    assert scan_source("if bad:\n    raise Boom()\n", "t.py") == []


def test_a_raise_from_none_outside_a_handler_stays_legal() -> None:
    """Nothing is being handled, so nothing attaches; the suppression is a no-op."""
    assert scan_source("if bad:\n    raise Boom() from None\n", "t.py") == []


def test_a_raise_after_the_handler_stays_legal() -> None:
    source = "err = None\ntry:\n    f()\nexcept ValueError:\n    err = Boom()\nif err is not None:\n    raise err\n"
    assert scan_source(source, "t.py") == []


def test_raise_from_exc_stays_legal() -> None:
    """Chaining deliberately is a different decision; this guard only bans the false erasure."""
    assert scan_source("try:\n    f()\nexcept ValueError as exc:\n    raise Boom() from exc\n", "t.py") == []


def test_a_bare_reraise_stays_legal() -> None:
    assert scan_source("try:\n    f()\nexcept ValueError:\n    raise\n", "t.py") == []


# ── runtime-backed self-tests: the analyzer is checked against the interpreter ──


class _Boom(Exception):
    """The exception a snippet raises from inside (or after) its handler."""


def _trigger() -> None:
    raise ValueError("placeholder-caught-text")


def _runtime_context(source: str) -> BaseException | None:
    """EXECUTE *source* and return what the interpreter attached to the raised exception."""
    namespace: dict = {"Boom": _Boom, "trigger": _trigger}
    raised: BaseException | None = None
    try:
        exec(compile(source, "runtime.py", "exec"), namespace)  # noqa: S102 — the behaviour IS the test
    except _Boom as exc:
        raised = exc
    assert raised is not None, "the snippet did not raise Boom"
    return raised.__context__


_NONE_ALIAS = "NO_CAUSE = None\ntry:\n    trigger()\nexcept ValueError:\n    raise Boom() from NO_CAUSE\n"
_RAISING_HELPER = "def _refuse():\n    raise Boom()\n\ntry:\n    trigger()\nexcept ValueError:\n    _refuse()\n"
_DEFINED_THEN_CALLED_AFTER = (
    "try:\n    trigger()\nexcept ValueError:\n    def later():\n        raise Boom()\nlater()\n"
)
_DEFINED_AND_CALLED_INSIDE = (
    "try:\n    trigger()\nexcept ValueError:\n    def refuse():\n        raise Boom()\n    refuse()\n"
)


def test_flags_a_raise_from_a_NONE_ALIAS() -> None:
    """``from <alias>`` where the alias is None is ``from None`` under another name."""
    assert isinstance(_runtime_context(_NONE_ALIAS), ValueError), "the interpreter kept the caught exception"
    assert scan_source(_NONE_ALIAS, "t.py") == ["t.py:5"]


def test_flags_a_call_to_an_ALWAYS_RAISING_HELPER_inside_a_handler() -> None:
    """The helper's raise runs while the handler is active, so the context attaches to it."""
    assert isinstance(_runtime_context(_RAISING_HELPER), ValueError), "the interpreter kept the caught exception"
    assert scan_source(_RAISING_HELPER, "t.py") == ["t.py:7"]


def test_a_function_DEFINED_in_a_handler_and_called_AFTER_it_stays_legal() -> None:
    """Defining a function executes nothing; the call runs where there is nothing to attach."""
    assert _runtime_context(_DEFINED_THEN_CALLED_AFTER) is None, "the interpreter attached nothing"
    assert scan_source(_DEFINED_THEN_CALLED_AFTER, "t.py") == []


def test_flags_a_function_defined_AND_CALLED_inside_the_handler() -> None:
    """The same definition, invoked one line earlier, is the helper case."""
    assert isinstance(_runtime_context(_DEFINED_AND_CALLED_INSIDE), ValueError)
    assert scan_source(_DEFINED_AND_CALLED_INSIDE, "t.py") == ["t.py:6"]


_HELPER_RAISING_FROM_NONE = (
    "def _refuse():\n    raise Boom() from None\n\ntry:\n    trigger()\nexcept ValueError:\n    _refuse()\n"
)
_DEFAULT_CALLS_A_RAISING_HELPER = (
    "def _refuse():\n"
    "    raise Boom()\n"
    "\n"
    "try:\n"
    "    trigger()\n"
    "except ValueError:\n"
    "    def later(x=_refuse()):\n"
    "        pass\n"
)
_DECORATOR_CALLS_A_RAISING_HELPER = (
    "def _refuse():\n"
    "    raise Boom()\n"
    "\n"
    "try:\n"
    "    trigger()\n"
    "except ValueError:\n"
    "    @_refuse()\n"
    "    def later():\n"
    "        pass\n"
)


def test_flags_a_helper_whose_raise_says_FROM_NONE() -> None:
    """``from None`` inside the helper suppresses nothing: the caught exception still attaches."""
    assert isinstance(_runtime_context(_HELPER_RAISING_FROM_NONE), ValueError)
    assert scan_source(_HELPER_RAISING_FROM_NONE, "t.py") == ["t.py:7"]


def test_flags_a_raising_call_in_a_DEFAULT_evaluated_by_the_definition() -> None:
    """A default expression runs where the ``def`` is WRITTEN, so it runs in the handler."""
    assert isinstance(_runtime_context(_DEFAULT_CALLS_A_RAISING_HELPER), ValueError)
    assert scan_source(_DEFAULT_CALLS_A_RAISING_HELPER, "t.py") == ["t.py:7"]


def test_flags_a_raising_call_in_a_DECORATOR_evaluated_by_the_definition() -> None:
    """So does a decorator expression."""
    assert isinstance(_runtime_context(_DECORATOR_CALLS_A_RAISING_HELPER), ValueError)
    assert scan_source(_DECORATOR_CALLS_A_RAISING_HELPER, "t.py") == ["t.py:7"]


_BARE_DECORATOR_IS_A_RAISING_HELPER = (
    "def refuse(fn):\n"
    "    raise Boom() from None\n"
    "\n"
    "try:\n"
    "    trigger()\n"
    "except ValueError:\n"
    "    @refuse\n"
    "    def later():\n"
    "        pass\n"
)


def test_flags_a_BARE_decorator_that_is_an_always_raising_helper() -> None:
    """``@refuse`` with no parentheses still CALLS ``refuse``, and the call runs in the handler."""
    assert isinstance(_runtime_context(_BARE_DECORATOR_IS_A_RAISING_HELPER), ValueError)
    assert scan_source(_BARE_DECORATOR_IS_A_RAISING_HELPER, "t.py") == ["t.py:7"]


def test_a_returning_helper_called_in_a_handler_stays_legal() -> None:
    """`api_error` and its siblings BUILD the refusal; the caller raises it after the handler."""
    source = (
        "def _build():\n"
        "    if bad:\n"
        "        raise Boom()\n"
        "    return Boom()\n"
        "\n"
        "err = None\n"
        "try:\n"
        "    trigger()\n"
        "except ValueError:\n"
        "    err = _build()\n"
        "if err is not None:\n"
        "    raise err\n"
    )
    assert scan_source(source, "t.py") == []


_ALIASED_RAISING_HELPER = (
    "def _refuse():\n    raise Boom()\n\nalias = _refuse\n\ntry:\n    trigger()\nexcept ValueError:\n    alias()\n"
)
_CHAINED_ALIASES_OF_A_RAISING_HELPER = (
    "def _refuse():\n"
    "    raise Boom()\n"
    "\n"
    "first = _refuse\n"
    "second = first\n"
    "\n"
    "try:\n"
    "    trigger()\n"
    "except ValueError:\n"
    "    second()\n"
)
_SELF_METHOD_RAISING_HELPER = (
    "class Example:\n"
    "    def _refuse(self):\n"
    "        raise Boom()\n"
    "\n"
    "    def run(self):\n"
    "        try:\n"
    "            trigger()\n"
    "        except ValueError:\n"
    "            self._refuse()\n"
    "\n"
    "Example().run()\n"
)
_CLASS_METHOD_RAISING_HELPER = (
    "class Example:\n"
    "    @classmethod\n"
    "    def _refuse(cls):\n"
    "        raise Boom()\n"
    "\n"
    "    @classmethod\n"
    "    def run(cls):\n"
    "        try:\n"
    "            trigger()\n"
    "        except ValueError:\n"
    "            cls._refuse()\n"
    "\n"
    "Example.run()\n"
)


def test_flags_an_ALIAS_of_an_always_raising_helper() -> None:
    """``alias = _refuse`` renames the helper; the call still raises inside the handler."""
    assert isinstance(_runtime_context(_ALIASED_RAISING_HELPER), ValueError), "the interpreter kept it"
    assert scan_source(_ALIASED_RAISING_HELPER, "t.py") == ["t.py:9"]


def test_flags_a_CHAIN_of_aliases_of_an_always_raising_helper() -> None:
    """Resolution repeats to a fixed point, so an alias of an alias resolves too."""
    assert isinstance(_runtime_context(_CHAINED_ALIASES_OF_A_RAISING_HELPER), ValueError)
    assert scan_source(_CHAINED_ALIASES_OF_A_RAISING_HELPER, "t.py") == ["t.py:10"]


@pytest.mark.parametrize(
    ("source", "line"),
    [
        (_SELF_METHOD_RAISING_HELPER, 9),
        (_CLASS_METHOD_RAISING_HELPER, 11),
    ],
)
def test_flags_a_METHOD_call_to_an_always_raising_helper(source: str, line: int) -> None:
    assert isinstance(_runtime_context(source), ValueError)
    assert scan_source(source, "t.py") == [f"t.py:{line}"]


def test_an_alias_of_a_RETURNING_helper_stays_legal() -> None:
    """Only an always-raising helper is aliased into the set; a builder still returns."""
    source = (
        "def _build():\n"
        "    return Boom()\n"
        "\n"
        "alias = _build\n"
        "err = None\n"
        "try:\n"
        "    trigger()\n"
        "except ValueError:\n"
        "    err = alias()\n"
        "if err is not None:\n"
        "    raise err\n"
    )
    assert scan_source(source, "t.py") == []


_WRAPPER_AROUND_A_RAISING_HELPER = (
    "def _refuse():\n"
    "    raise Boom()\n"
    "\n"
    "def _wrapper():\n"
    "    _refuse()\n"
    "\n"
    "try:\n"
    "    trigger()\n"
    "except ValueError:\n"
    "    _wrapper()\n"
)
_WRAPPER_CHAIN_AROUND_A_RAISING_HELPER = (
    "def _refuse():\n"
    "    raise Boom()\n"
    "\n"
    "def _inner():\n"
    "    _refuse()\n"
    "\n"
    "def _outer():\n"
    "    _inner()\n"
    "\n"
    "try:\n"
    "    trigger()\n"
    "except ValueError:\n"
    "    _outer()\n"
)
_WRAPPER_AROUND_AN_ALIASED_HELPER = (
    "def _refuse():\n"
    "    raise Boom()\n"
    "\n"
    "_same = _refuse\n"
    "\n"
    "def _wrapper():\n"
    "    _same()\n"
    "\n"
    "try:\n"
    "    trigger()\n"
    "except ValueError:\n"
    "    _wrapper()\n"
)
_RETURN_WRAPPER_AROUND_A_RAISING_HELPER = (
    "def _refuse():\n"
    "    raise Boom()\n"
    "\n"
    "def _wrapper():\n"
    "    return _refuse()\n"
    "\n"
    "try:\n"
    "    trigger()\n"
    "except ValueError:\n"
    "    _wrapper()\n"
)
_WRAPPER_THAT_MAY_RETURN = (
    "def _refuse():\n"
    "    raise Boom()\n"
    "\n"
    "def _maybe(flag):\n"
    "    if flag:\n"
    "        return None\n"
    "    _refuse()\n"
    "\n"
    "try:\n"
    "    trigger()\n"
    "except ValueError:\n"
    "    _maybe(True)\n"
    "raise Boom()\n"
)


def test_flags_a_WRAPPER_around_an_always_raising_helper() -> None:
    """One frame further down is still the handler's frame: the context attaches the same."""
    assert isinstance(_runtime_context(_WRAPPER_AROUND_A_RAISING_HELPER), ValueError)
    assert scan_source(_WRAPPER_AROUND_A_RAISING_HELPER, "t.py") == ["t.py:10"]


def test_flags_a_CHAIN_of_wrappers_around_an_always_raising_helper() -> None:
    """Resolution repeats to a fixed point, so depth cannot hide the raise."""
    assert isinstance(_runtime_context(_WRAPPER_CHAIN_AROUND_A_RAISING_HELPER), ValueError)
    assert scan_source(_WRAPPER_CHAIN_AROUND_A_RAISING_HELPER, "t.py") == ["t.py:13"]


def test_flags_a_wrapper_around_an_ALIASED_helper() -> None:
    """The alias IS the helper, so a wrapper around the alias is a wrapper around the raise."""
    assert isinstance(_runtime_context(_WRAPPER_AROUND_AN_ALIASED_HELPER), ValueError)
    assert scan_source(_WRAPPER_AROUND_AN_ALIASED_HELPER, "t.py") == ["t.py:12"]


def test_flags_a_RETURN_wrapper_around_an_always_raising_helper() -> None:
    assert isinstance(_runtime_context(_RETURN_WRAPPER_AROUND_A_RAISING_HELPER), ValueError)
    assert scan_source(_RETURN_WRAPPER_AROUND_A_RAISING_HELPER, "t.py") == ["t.py:10"]


def test_a_wrapper_that_CAN_RETURN_is_not_an_always_raising_helper() -> None:
    """It does not always raise, so calling it in a handler is not a raise written there."""
    assert _runtime_context(_WRAPPER_THAT_MAY_RETURN) is None, "the interpreter attached nothing"
    assert scan_source(_WRAPPER_THAT_MAY_RETURN, "t.py") == []


def test_an_unrelated_method_name_does_not_taint_a_module_builder() -> None:
    source = (
        "class Unused:\n"
        "    def build(self):\n"
        "        raise Boom()\n"
        "\n"
        "def build():\n"
        "    return Boom()\n"
        "\n"
        "try:\n"
        "    trigger()\n"
        "except ValueError:\n"
        "    err = build()\n"
        "raise err\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_a_return_in_a_nested_body_does_not_hide_an_outer_raise() -> None:
    source = (
        "def refuse():\n"
        "    def build():\n"
        "        return Boom()\n"
        "    raise Boom()\n"
        "\n"
        "try:\n"
        "    trigger()\n"
        "except ValueError:\n"
        "    refuse()\n"
    )
    assert isinstance(_runtime_context(source), ValueError)
    assert scan_source(source, "t.py") == ["t.py:9"]


def test_a_static_method_parameter_has_no_receiver_identity() -> None:
    source = (
        "class Safe:\n"
        "    def refuse(self):\n"
        "        return Boom()\n"
        "\n"
        "class Example:\n"
        "    def refuse(self):\n"
        "        raise Boom()\n"
        "\n"
        "    @staticmethod\n"
        "    def run(self):\n"
        "        try:\n"
        "            trigger()\n"
        "        except ValueError:\n"
        "            return self.refuse()\n"
        "\n"
        "raise Example.run(Safe())\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_a_comprehension_target_shadows_an_outer_raising_helper() -> None:
    source = (
        "def refuse():\n"
        "    raise Boom()\n"
        "\n"
        "def build():\n"
        "    return Boom()\n"
        "\n"
        "try:\n"
        "    trigger()\n"
        "except ValueError:\n"
        "    err = [refuse() for refuse in [build]][0]\n"
        "raise err\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_an_arbitrary_decorator_makes_the_helper_effect_unknown() -> None:
    source = (
        "def safe(fn):\n"
        "    return lambda: Boom()\n"
        "\n"
        "@safe\n"
        "def refuse():\n"
        "    raise Boom()\n"
        "\n"
        "try:\n"
        "    trigger()\n"
        "except ValueError:\n"
        "    err = refuse()\n"
        "raise err\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_creating_a_generator_in_a_handler_does_not_execute_its_raise() -> None:
    source = (
        "def refuse():\n"
        "    def later(value=(yield None)):\n"
        "        pass\n"
        "    raise Boom()\n"
        "\n"
        "generated = None\n"
        "try:\n"
        "    trigger()\n"
        "except ValueError:\n"
        "    generated = refuse()\n"
        "    next(generated)\n"
        "next(generated)\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_creating_a_coroutine_in_a_handler_does_not_execute_its_raise() -> None:
    source = (
        "import asyncio\n"
        "\n"
        "async def refuse():\n"
        "    raise Boom()\n"
        "\n"
        "pending = None\n"
        "try:\n"
        "    trigger()\n"
        "except ValueError:\n"
        "    pending = refuse()\n"
        "asyncio.run(pending)\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_a_class_body_default_uses_the_class_helper_binding() -> None:
    source = (
        "class Example:\n"
        "    def refuse():\n"
        "        raise Boom()\n"
        "    try:\n"
        "        trigger()\n"
        "    except ValueError:\n"
        "        def later(value=refuse()):\n"
        "            pass\n"
    )
    assert isinstance(_runtime_context(source), ValueError)
    assert scan_source(source, "t.py") == ["t.py:7"]


def test_a_generator_expression_body_created_in_a_handler_is_deferred() -> None:
    source = (
        "def refuse():\n"
        "    raise Boom()\n"
        "\n"
        "pending = None\n"
        "try:\n"
        "    trigger()\n"
        "except ValueError:\n"
        "    pending = (refuse() for item in [0])\n"
        "next(pending)\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_a_match_capture_shadows_an_outer_raising_helper() -> None:
    source = (
        "def refuse():\n"
        "    raise Boom()\n"
        "\n"
        "def build():\n"
        "    return Boom()\n"
        "\n"
        "def run():\n"
        "    match build:\n"
        "        case refuse:\n"
        "            pass\n"
        "    try:\n"
        "        trigger()\n"
        "    except ValueError:\n"
        "        return refuse()\n"
        "\n"
        "raise run()\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_a_comprehension_walrus_binds_in_the_enclosing_function() -> None:
    source = (
        "def refuse():\n"
        "    raise Boom()\n"
        "\n"
        "def build():\n"
        "    return Boom()\n"
        "\n"
        "def run():\n"
        "    [(refuse := build) for item in [0]]\n"
        "    try:\n"
        "        trigger()\n"
        "    except ValueError:\n"
        "        return refuse()\n"
        "\n"
        "raise run()\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_a_global_walrus_invalidates_the_module_helper_binding() -> None:
    source = (
        "def refuse():\n"
        "    raise Boom()\n"
        "\n"
        "def build():\n"
        "    return Boom()\n"
        "\n"
        "def replace():\n"
        "    global refuse\n"
        "    [(refuse := build) for item in [0]]\n"
        "\n"
        "replace()\n"
        "try:\n"
        "    trigger()\n"
        "except ValueError:\n"
        "    err = refuse()\n"
        "raise err\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_a_nonlocal_match_capture_invalidates_the_enclosing_helper_binding() -> None:
    source = (
        "def reject():\n"
        "    raise Boom()\n"
        "\n"
        "def build():\n"
        "    return Boom()\n"
        "\n"
        "def outer():\n"
        "    refuse = reject\n"
        "    def replace():\n"
        "        nonlocal refuse\n"
        "        match build:\n"
        "            case refuse:\n"
        "                pass\n"
        "    replace()\n"
        "    try:\n"
        "        trigger()\n"
        "    except ValueError:\n"
        "        return refuse()\n"
        "\n"
        "raise outer()\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_a_generator_expression_outer_iterable_executes_in_the_handler() -> None:
    source = (
        "def refuse():\n"
        "    raise Boom()\n"
        "\n"
        "try:\n"
        "    trigger()\n"
        "except ValueError:\n"
        "    pending = (item for item in refuse())\n"
    )
    assert isinstance(_runtime_context(source), ValueError)
    assert scan_source(source, "t.py") == ["t.py:7"]


def test_a_global_read_skips_an_enclosing_function_binding() -> None:
    source = (
        "def refuse():\n"
        "    return Boom()\n"
        "\n"
        "def outer():\n"
        "    def refuse():\n"
        "        raise Boom()\n"
        "\n"
        "    def inner():\n"
        "        global refuse\n"
        "        try:\n"
        "            trigger()\n"
        "        except ValueError:\n"
        "            return refuse()\n"
        "\n"
        "    return inner()\n"
        "\n"
        "raise outer()\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_an_ordinary_global_assignment_invalidates_the_module_helper() -> None:
    source = (
        "def refuse():\n"
        "    raise Boom()\n"
        "\n"
        "def build():\n"
        "    return Boom()\n"
        "\n"
        "def replace():\n"
        "    global refuse\n"
        "    refuse = build\n"
        "\n"
        "replace()\n"
        "try:\n"
        "    trigger()\n"
        "except ValueError:\n"
        "    err = refuse()\n"
        "raise err\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_a_global_receiver_skips_an_enclosing_method_receiver() -> None:
    source = (
        "class Safe:\n"
        "    def refuse(self):\n"
        "        return Boom()\n"
        "\n"
        "self = Safe()\n"
        "\n"
        "class Example:\n"
        "    def refuse(self):\n"
        "        raise Boom()\n"
        "\n"
        "    def run(self):\n"
        "        def inner():\n"
        "            global self\n"
        "            try:\n"
        "                trigger()\n"
        "            except ValueError:\n"
        "                return self.refuse()\n"
        "        return inner()\n"
        "\n"
        "raise Example().run()\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_a_method_skips_class_scope_declarations_and_bindings() -> None:
    source = (
        "def refuse():\n"
        "    raise Boom()\n"
        "\n"
        "def outer():\n"
        "    def refuse():\n"
        "        return Boom()\n"
        "\n"
        "    class Example:\n"
        "        global refuse\n"
        "\n"
        "        def run(self):\n"
        "            try:\n"
        "                trigger()\n"
        "            except ValueError:\n"
        "                return refuse()\n"
        "\n"
        "    return Example().run()\n"
        "\n"
        "raise outer()\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []


def test_a_nested_class_body_skips_its_enclosing_class_namespace() -> None:
    source = (
        "def refuse():\n"
        "    return Boom()\n"
        "\n"
        "class Outer:\n"
        "    def refuse():\n"
        "        raise Boom()\n"
        "\n"
        "    class Inner:\n"
        "        try:\n"
        "            trigger()\n"
        "        except ValueError:\n"
        "            err = refuse()\n"
        "\n"
        "raise Outer.Inner.err\n"
    )
    assert _runtime_context(source) is None, "the interpreter attached nothing"
    assert scan_source(source, "t.py") == []
