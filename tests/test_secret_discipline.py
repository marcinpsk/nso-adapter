# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""What the secret-discipline assertions must inspect and keep out of diagnostics."""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from tests._ast_scanner_support import (
    argument_names,
    match_capture_names,
    statement_may_raise,
    walrus_expressions,
)
from tests._secret_discipline import assert_chain_free_of, exception_chain

_SECRET = "placeholder-vault-secret"
_TEST_ROOT = Path(__file__).resolve().parent
#: Every test module. A fixed allowlist lets a new module's assertion escape the guard, and
#: three review rounds found exactly that escape before this list was retired.
_NON_DISCLOSURE_TESTS = tuple(sorted(_TEST_ROOT.rglob("test_*.py")))
#: These attributes and calls return complete values that pytest prints in assertion failures.
_RENDERED_SURFACE_ATTRIBUTES = {"json", "read_failures", "text", "value"}
_INSPECTED_CALLS = {"repr", "str"}
_NON_DISCLOSURE_HELPERS = {"assert_chain_free_of", "assert_records_free_of", "assert_text_free_of"}
#: How this repository writes protected material into a test (see the placeholder convention). A
#: body that names one is handling something protected, whether or not it calls a helper.
_PROTECTED_LITERAL_PREFIX = "placeholder-"


def test_main_lifespan_is_in_the_non_disclosure_registry() -> None:
    """The blanket sweep covers this module without anyone listing it."""
    assert _TEST_ROOT / "test_main_lifespan.py" in _NON_DISCLOSURE_TESTS


class _InspectedSurfaceReader(ast.NodeVisitor):
    """Find inspected values without importing deferred nested-scope bodies."""

    def __init__(self, aliases: set[str]) -> None:
        self.aliases = aliases
        self.found = False

    def visit(self, node: ast.AST) -> None:
        if self.found:
            return
        super().visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:  # noqa: N802 - ast visitor API
        if node.attr in _RENDERED_SURFACE_ATTRIBUTES:
            self.found = True

    def visit_Subscript(self, node: ast.Subscript) -> None:  # noqa: N802 - ast visitor API
        # A subscript narrows a decoded container to one member.
        return

    def visit_ListComp(self, node: ast.ListComp) -> None:  # noqa: N802 - ast visitor API
        self.visit(node.elt)

    def visit_SetComp(self, node: ast.SetComp) -> None:  # noqa: N802 - ast visitor API
        self.visit(node.elt)

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:  # noqa: N802 - ast visitor API
        self.visit(node.elt)

    def visit_DictComp(self, node: ast.DictComp) -> None:  # noqa: N802 - ast visitor API
        self.visit(node.key)
        self.visit(node.value)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802 - ast visitor API
        if isinstance(node.func, ast.Name) and node.func.id in _INSPECTED_CALLS:
            self.found = True
            return
        if isinstance(node.func, ast.Attribute) and node.func.attr in _RENDERED_SURFACE_ATTRIBUTES:
            self.found = True
            return
        if isinstance(node.func, ast.Lambda):
            self.visit(node.func.body)
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:  # noqa: N802 - ast visitor API
        if isinstance(node.ctx, ast.Load) and node.id in self.aliases:
            self.found = True

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802 - ast visitor API
        return


def _reads_an_inspected_surface(node: ast.AST, aliases: set[str] | None = None) -> bool:
    reader = _InspectedSurfaceReader(aliases or set())
    reader.visit(node)
    return reader.found


def _target_names(target: ast.AST) -> set[str]:
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, ast.Starred):
        return _target_names(target.value)
    if isinstance(target, (ast.List, ast.Tuple)):
        return set().union(*(_target_names(element) for element in target.elts), set())
    return set()


def _target_value_bindings(target: ast.AST, value: ast.AST | list[ast.AST]) -> list[tuple[str, list[ast.AST]]]:
    if isinstance(target, ast.Name):
        return [(target.id, value if isinstance(value, list) else [value])]
    if isinstance(target, ast.Starred):
        return _target_value_bindings(target.value, value)
    if not isinstance(target, (ast.List, ast.Tuple)):
        return []
    if not isinstance(value, (ast.List, ast.Tuple)):
        values = value if isinstance(value, list) else [value]
        return [(name, values) for name in _target_names(target)]

    targets = target.elts
    values = value.elts
    starred = next((index for index, element in enumerate(targets) if isinstance(element, ast.Starred)), None)
    if starred is None and len(targets) == len(values):
        return [
            binding
            for nested_target, nested_value in zip(targets, values, strict=True)
            for binding in _target_value_bindings(nested_target, nested_value)
        ]
    if starred is not None and len(values) >= len(targets) - 1:
        trailing = len(targets) - starred - 1
        bindings = [
            binding
            for nested_target, nested_value in zip(targets[:starred], values[:starred], strict=True)
            for binding in _target_value_bindings(nested_target, nested_value)
        ]
        starred_end = len(values) - trailing if trailing else len(values)
        bindings.extend(_target_value_bindings(targets[starred], list(values[starred:starred_end])))
        if trailing:
            bindings.extend(
                binding
                for nested_target, nested_value in zip(targets[-trailing:], values[-trailing:], strict=True)
                for binding in _target_value_bindings(nested_target, nested_value)
            )
        return bindings
    return [(name, list(values)) for name in _target_names(target)]


class _ScopeFacts(ast.NodeVisitor):
    """Collect one lexical scope's bindings, assertions, and child scopes."""

    def __init__(self) -> None:
        self.bindings: list[tuple[str, list[ast.AST]]] = []
        self.local_names: set[str] = set()
        self.assertions: list[ast.Assert] = []
        self.children: list[ast.AST] = []

    def _bind(self, target: ast.AST, value: ast.AST | list[ast.AST]) -> None:
        self.local_names.update(_target_names(target))
        self.bindings.extend(_target_value_bindings(target, value))

    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802 - ast visitor API
        for target in node.targets:
            self._bind(target, node.value)
        self.visit(node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:  # noqa: N802 - ast visitor API
        self.local_names.update(_target_names(node.target))
        if node.value is not None:
            self._bind(node.target, node.value)
            self.visit(node.value)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:  # noqa: N802 - ast visitor API
        self.local_names.update(_target_names(node.target))
        for name in _target_names(node.target):
            self.bindings.append((name, [ast.Name(id=name, ctx=ast.Load()), node.value]))
        self.visit(node.value)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:  # noqa: N802 - ast visitor API
        self._bind(node.target, node.value)
        self.visit(node.value)

    def visit_alias(self, node: ast.alias) -> None:  # noqa: N802 - ast visitor API
        # An import binds a name with no ast.Name store: `import a.b` binds `a`.
        self.local_names.add(node.asname or node.name.split(".", maxsplit=1)[0])

    def visit_For(self, node: ast.For) -> None:  # noqa: N802 - ast visitor API
        # The loop target binds the iterable's elements: `for value in [response.text]` makes
        # `value` the response text, exactly as an assignment would.
        iterated = node.iter.elts if isinstance(node.iter, (ast.List, ast.Tuple)) else node.iter
        self._bind(node.target, iterated)
        self.visit(node.iter)
        for statement in (*node.body, *node.orelse):
            self.visit(statement)

    visit_AsyncFor = visit_For  # type: ignore[assignment]

    def visit_Match(self, node: ast.Match) -> None:  # noqa: N802 - ast visitor API
        # A capture shadows an inherited alias, but it carries the SUBJECT's value with it.
        # Binding the name without its value would make `case [name]` launder the taint.
        self.visit(node.subject)
        for case in node.cases:
            for name in match_capture_names(case.pattern):
                self._bind(ast.Name(id=name, ctx=ast.Store()), node.subject)
            if case.guard is not None:
                self.visit(case.guard)
            for statement in case.body:
                self.visit(statement)

    def visit_Assert(self, node: ast.Assert) -> None:  # noqa: N802 - ast visitor API
        self.assertions.append(node)
        self.generic_visit(node)

    def _visit_definition_expressions(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        for expression in (
            *node.decorator_list,
            *node.args.defaults,
            *(default for default in node.args.kw_defaults if default is not None),
        ):
            self.visit(expression)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802 - ast visitor API
        self.local_names.add(node.name)
        self._visit_definition_expressions(node)
        self.children.append(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802 - ast visitor API
        self.visit_FunctionDef(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802 - ast visitor API
        self.local_names.add(node.name)
        for expression in (*node.decorator_list, *node.bases, *(keyword.value for keyword in node.keywords)):
            self.visit(expression)
        self.children.append(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: N802 - ast visitor API
        for expression in (*node.args.defaults, *(value for value in node.args.kw_defaults if value is not None)):
            self.visit(expression)
        self.children.append(node)

    def _visit_comprehension(self, node: ast.ListComp | ast.SetComp | ast.GeneratorExp | ast.DictComp) -> None:
        visited = {id(binding) for binding in walrus_expressions(node.generators[0].iter)}
        self.visit(node.generators[0].iter)
        # The comprehension is its own scope, but a walrus in it binds HERE (PEP 572), so its
        # VALUE belongs to these facts too, not only its name.
        for binding in walrus_expressions(node):
            if id(binding) not in visited:
                self._bind(binding.target, binding.value)
        self.children.append(node)

    def visit_ListComp(self, node: ast.ListComp) -> None:  # noqa: N802 - ast visitor API
        self._visit_comprehension(node)

    def visit_SetComp(self, node: ast.SetComp) -> None:  # noqa: N802 - ast visitor API
        self._visit_comprehension(node)

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:  # noqa: N802 - ast visitor API
        self._visit_comprehension(node)

    def visit_DictComp(self, node: ast.DictComp) -> None:  # noqa: N802 - ast visitor API
        self._visit_comprehension(node)


def _scope_facts(scope: ast.AST) -> _ScopeFacts:
    facts = _ScopeFacts()
    if isinstance(scope, ast.Module | ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
        if isinstance(scope, ast.FunctionDef | ast.AsyncFunctionDef):
            facts.local_names.update(argument_names(scope.args))
        for statement in scope.body:
            facts.visit(statement)
    elif isinstance(scope, ast.Lambda):
        facts.local_names.update(argument_names(scope.args))
        facts.visit(scope.body)
    return facts


def _binding_aliases(bindings: list[tuple[str, list[ast.AST]]], aliases: set[str]) -> set[str]:
    resolved: set[str] = set()
    changed = True
    while changed:
        changed = False
        for name, values in bindings:
            if name not in resolved and any(_reads_an_inspected_surface(value, aliases | resolved) for value in values):
                resolved.add(name)
                changed = True
    return resolved


_COMPREHENSIONS = (ast.ListComp, ast.SetComp, ast.GeneratorExp, ast.DictComp)


def _assertion_comparisons(test: ast.expr) -> list[ast.Compare]:
    """Return assertion comparisons outside comprehension filters."""
    comparisons: list[ast.Compare] = []
    stack: list[ast.AST] = [test]
    while stack:
        node = stack.pop()
        if isinstance(node, _COMPREHENSIONS):
            continue
        if isinstance(node, ast.Compare):
            comparisons.append(node)
        stack.extend(ast.iter_child_nodes(node))
    return comparisons


def _record_non_disclosure_assertions(assertions: list[ast.Assert], aliases: set[str], violations: list[int]) -> None:
    for node in assertions:
        if any(
            any(isinstance(operator, ast.NotIn) for operator in comparison.ops)
            # BOTH operands: ``assert resp.text not in allowed`` renders the response on the
            # left, and pytest prints the whole comparison either way.
            and any(_reads_an_inspected_surface(value, aliases) for value in (comparison.left, *comparison.comparators))
            for comparison in _assertion_comparisons(node.test)
        ):
            violations.append(node.lineno)


@dataclass
class _ObservedStates:
    exceptions: list[set[str]] = field(default_factory=list)
    returns: list[set[str]] = field(default_factory=list)


def _resolve_class_node(
    node: ast.AST,
    aliases: set[str],
    enclosing_aliases: set[str],
    violations: list[int] | None,
    inherit_current: bool,
) -> set[str]:
    facts = _ScopeFacts()
    facts.visit(node)
    bound_aliases = _binding_aliases(facts.bindings, aliases)
    aliases = aliases - facts.local_names | bound_aliases
    if violations is not None:
        _record_non_disclosure_assertions(facts.assertions, aliases, violations)
        for child in facts.children:
            if not (inherit_current and isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef)):
                _resolve_scope(child, aliases if inherit_current else enclosing_aliases, violations)
    return aliases


def _resolve_class_if(
    statement: ast.If,
    aliases: set[str],
    enclosing_aliases: set[str],
    violations: list[int] | None,
    observed_states: _ObservedStates | None,
    inherit_current: bool,
) -> set[str]:
    aliases = _resolve_class_node(statement.test, aliases, enclosing_aliases, violations, inherit_current)
    body_aliases = _resolve_class_statements(
        statement.body, aliases.copy(), enclosing_aliases, violations, observed_states, inherit_current
    )
    else_aliases = _resolve_class_statements(
        statement.orelse, aliases.copy(), enclosing_aliases, violations, observed_states, inherit_current
    )
    return body_aliases | else_aliases


def _resolve_class_try(
    statement: ast.Try | ast.TryStar,
    aliases: set[str],
    enclosing_aliases: set[str],
    violations: list[int] | None,
    observed_states: _ObservedStates | None,
    inherit_current: bool,
) -> set[str]:
    incoming = aliases.copy()
    body_states = _ObservedStates()
    body_aliases = _resolve_class_statements(
        statement.body, incoming.copy(), enclosing_aliases, violations, body_states, inherit_current
    )
    else_states = _ObservedStates()
    normal_aliases = _resolve_class_statements(
        statement.orelse, body_aliases.copy(), enclosing_aliases, violations, else_states, inherit_current
    )
    handler_input = set().union(*body_states.exceptions)
    handler_aliases = []
    handler_exception_states: list[set[str]] = []
    handler_return_states: list[set[str]] = []
    for handler in statement.handlers if body_states.exceptions else ():
        state = handler_input.copy()
        if handler.type is not None:
            state = _resolve_class_node(handler.type, state, enclosing_aliases, violations, inherit_current)
        if handler.name is not None:
            state.discard(handler.name)
        handler_states = _ObservedStates()
        state = _resolve_class_statements(
            handler.body, state, enclosing_aliases, violations, handler_states, inherit_current
        )
        if handler.name is not None:
            state.discard(handler.name)
            for handler_state in (*handler_states.exceptions, *handler_states.returns):
                handler_state.discard(handler.name)
        handler_exception_states.extend(handler_states.exceptions)
        handler_return_states.extend(handler_states.returns)
        handler_aliases.append(state)
    aliases = normal_aliases | set().union(*handler_aliases, set())
    normal_aliases = _resolve_class_statements(
        statement.finalbody, aliases, enclosing_aliases, violations, observed_states, inherit_current
    )
    exceptional_states = else_states.exceptions + handler_exception_states
    if not any(handler.type is None for handler in statement.handlers):
        exceptional_states += body_states.exceptions
    exceptional_aliases = set().union(*exceptional_states, set())
    if exceptional_aliases:
        propagated_aliases = _resolve_class_statements(
            statement.finalbody, exceptional_aliases, enclosing_aliases, violations, observed_states, inherit_current
        )
        if observed_states is not None:
            observed_states.exceptions.append(propagated_aliases)
    returning_states = body_states.returns + else_states.returns + handler_return_states
    if returning_states:
        returned_aliases = _resolve_class_statements(
            statement.finalbody,
            set().union(*returning_states),
            enclosing_aliases,
            violations,
            observed_states,
            inherit_current,
        )
        if observed_states is not None:
            observed_states.returns.append(returned_aliases)
    return normal_aliases


def _resolve_class_for(
    statement: ast.For | ast.AsyncFor,
    aliases: set[str],
    enclosing_aliases: set[str],
    violations: list[int] | None,
    observed_states: _ObservedStates | None,
    inherit_current: bool,
) -> set[str]:
    incoming = _resolve_class_node(statement.iter, aliases, enclosing_aliases, violations, inherit_current)
    loop_entry = incoming.copy()
    while True:
        target_aliases = _binding_aliases(_target_value_bindings(statement.target, statement.iter), loop_entry)
        iteration_aliases = loop_entry - _target_names(statement.target) | target_aliases
        body_aliases = _resolve_class_statements(
            statement.body, iteration_aliases, enclosing_aliases, violations, observed_states, inherit_current
        )
        expanded_entry = loop_entry | body_aliases
        if expanded_entry == loop_entry:
            break
        loop_entry = expanded_entry
    else_aliases = _resolve_class_statements(
        statement.orelse, loop_entry, enclosing_aliases, violations, observed_states, inherit_current
    )
    return loop_entry | else_aliases


def _resolve_class_while(
    statement: ast.While,
    aliases: set[str],
    enclosing_aliases: set[str],
    violations: list[int] | None,
    observed_states: _ObservedStates | None,
    inherit_current: bool,
) -> set[str]:
    initial = aliases.copy()
    loop_entry = initial.copy()
    while True:
        tested_aliases = _resolve_class_node(statement.test, loop_entry, enclosing_aliases, violations, inherit_current)
        body_aliases = _resolve_class_statements(
            statement.body, tested_aliases, enclosing_aliases, violations, observed_states, inherit_current
        )
        expanded_entry = loop_entry | body_aliases
        if expanded_entry == loop_entry:
            break
        loop_entry = expanded_entry
    else_aliases = _resolve_class_statements(
        statement.orelse, initial | loop_entry, enclosing_aliases, violations, observed_states, inherit_current
    )
    return initial | loop_entry | else_aliases


def _resolve_class_with(
    statement: ast.With | ast.AsyncWith,
    aliases: set[str],
    enclosing_aliases: set[str],
    violations: list[int] | None,
    observed_states: _ObservedStates | None,
    inherit_current: bool,
) -> set[str]:
    incoming = aliases.copy()
    for item in statement.items:
        incoming = _resolve_class_node(item.context_expr, incoming, enclosing_aliases, violations, inherit_current)
        if item.optional_vars is not None:
            target_aliases = _binding_aliases(_target_value_bindings(item.optional_vars, item.context_expr), incoming)
            incoming = incoming - _target_names(item.optional_vars) | target_aliases
    body_states = _ObservedStates() if observed_states is not None else None
    aliases = _resolve_class_statements(
        statement.body, incoming, enclosing_aliases, violations, body_states, inherit_current
    )
    if observed_states is not None:
        assert body_states is not None
        observed_states.exceptions.extend(body_states.exceptions)
        observed_states.exceptions.extend(body_states.returns)
        observed_states.returns.extend(body_states.returns)
        observed_states.exceptions.append(aliases.copy())
    return aliases


def _resolve_class_statements(
    statements: list[ast.stmt],
    aliases: set[str],
    enclosing_aliases: set[str],
    violations: list[int] | None,
    observed_states: _ObservedStates | None = None,
    inherit_current: bool = False,
) -> set[str]:
    for statement in statements:
        if observed_states is not None and statement_may_raise(statement):
            observed_states.exceptions.append(aliases.copy())
        if isinstance(statement, ast.If):
            aliases = _resolve_class_if(
                statement, aliases, enclosing_aliases, violations, observed_states, inherit_current
            )
        elif isinstance(statement, (ast.Try, ast.TryStar)):
            aliases = _resolve_class_try(
                statement, aliases, enclosing_aliases, violations, observed_states, inherit_current
            )
        elif isinstance(statement, (ast.For, ast.AsyncFor)):
            aliases = _resolve_class_for(
                statement, aliases, enclosing_aliases, violations, observed_states, inherit_current
            )
        elif isinstance(statement, ast.While):
            aliases = _resolve_class_while(
                statement, aliases, enclosing_aliases, violations, observed_states, inherit_current
            )
        elif isinstance(statement, (ast.With, ast.AsyncWith)):
            aliases = _resolve_class_with(
                statement, aliases, enclosing_aliases, violations, observed_states, inherit_current
            )
        else:
            aliases = _resolve_class_node(statement, aliases, enclosing_aliases, violations, inherit_current)
            if isinstance(statement, ast.Return | ast.Raise):
                if observed_states is not None and isinstance(statement, ast.Return):
                    observed_states.returns.append(aliases.copy())
                return set()
    return aliases


def _resolve_class_scope(scope: ast.ClassDef, enclosing_aliases: set[str], violations: list[int] | None) -> set[str]:
    return _resolve_class_statements(scope.body, enclosing_aliases.copy(), enclosing_aliases, violations)


class _ChildCallProof:
    """Keep each branch's aliases with the names bound to one nested function."""

    def __init__(self, child: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.child = child

    @staticmethod
    def _references(node: ast.AST, bound_names: set[str]) -> bool:
        return any(
            isinstance(part, ast.Name) and isinstance(part.ctx, ast.Load) and part.id in bound_names
            for part in ast.walk(node)
        )

    def _if(
        self, statement: ast.If, aliases: set[str], bound_names: set[str]
    ) -> list[tuple[set[str], set[str]]] | None:
        if self._references(statement.test, bound_names):
            return None
        tested = _resolve_class_node(statement.test, aliases, aliases, None, True)
        body = self.walk(statement.body, [(tested.copy(), bound_names.copy())])
        other = self.walk(statement.orelse, [(tested.copy(), bound_names.copy())])
        return None if body is None or other is None else body + other

    def _definition(
        self, statement: ast.FunctionDef | ast.AsyncFunctionDef, aliases: set[str], bound_names: set[str]
    ) -> list[tuple[set[str], set[str]]] | None:
        if statement is not self.child and any(
            isinstance(part, ast.Name) and isinstance(part.ctx, ast.Load) for part in ast.walk(statement)
        ):
            return None
        updated = bound_names - {statement.name}
        if statement is self.child:
            updated.add(statement.name)
        return [(aliases - {statement.name}, updated)]

    def _assignment(
        self, statement: ast.Assign, aliases: set[str], bound_names: set[str]
    ) -> list[tuple[set[str], set[str]]] | None:
        if not all(isinstance(target, ast.Name) for target in statement.targets):
            return None
        if self._references(statement.value, bound_names) and not isinstance(statement.value, ast.Name):
            return None
        updated = bound_names.copy()
        for target in statement.targets:
            if isinstance(statement.value, ast.Name) and statement.value.id in bound_names:
                updated.add(target.id)
            else:
                updated.discard(target.id)
        return [(_resolve_class_node(statement, aliases, aliases, None, True), updated)]

    def _call(
        self, statement: ast.Expr, aliases: set[str], bound_names: set[str]
    ) -> list[tuple[set[str], set[str]]] | None:
        call = statement.value
        assert isinstance(call, ast.Call)
        if isinstance(call.func, ast.Name) and call.func.id in {"locals", "vars", "eval", "exec"}:
            return None
        if isinstance(call.func, ast.Name) and call.func.id in bound_names:
            if call.args or call.keywords:
                return None
            violations: list[int] = []
            _resolve_scope(self.child, aliases, violations)
            if violations:
                return None
        elif self._references(call, bound_names):
            return None
        return [(_resolve_class_node(statement, aliases, aliases, None, True), bound_names)]

    def _step(
        self, statement: ast.stmt, aliases: set[str], bound_names: set[str]
    ) -> list[tuple[set[str], set[str]]] | None:
        if isinstance(statement, ast.If):
            return self._if(statement, aliases, bound_names)
        if isinstance(statement, ast.FunctionDef | ast.AsyncFunctionDef):
            return self._definition(statement, aliases, bound_names)
        if isinstance(statement, ast.Assign):
            return self._assignment(statement, aliases, bound_names)
        if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call):
            return self._call(statement, aliases, bound_names)
        if isinstance(statement, ast.Return | ast.Raise):
            return None if self._references(statement, bound_names) else []
        if isinstance(statement, ast.Pass):
            return [(aliases, bound_names)]
        return None

    def walk(
        self, statements: list[ast.stmt], paths: list[tuple[set[str], set[str]]]
    ) -> list[tuple[set[str], set[str]]] | None:
        for statement in statements:
            next_paths: list[tuple[set[str], set[str]]] = []
            for aliases, bound_names in paths:
                stepped = self._step(statement, aliases, bound_names)
                if stepped is None:
                    return None
                next_paths.extend(stepped)
            paths = next_paths
            if len(paths) > 64:
                return None
        return paths


def _child_calls_are_safe(
    scope: ast.FunctionDef | ast.AsyncFunctionDef,
    child: ast.FunctionDef | ast.AsyncFunctionDef,
    initial_aliases: set[str],
) -> bool:
    """Prove direct child calls safe; keep the conservative verdict when proof is incomplete."""
    if (
        child.decorator_list
        or child.args.defaults
        or any(default is not None for default in child.args.kw_defaults)
        or any(isinstance(node, ast.NamedExpr | ast.Nonlocal | ast.Global) for node in ast.walk(scope))
    ):
        return False
    return _ChildCallProof(child).walk(scope.body, [(initial_aliases.copy(), set())]) is not None


def _resolve_scope(scope: ast.AST, enclosing_aliases: set[str], violations: list[int] | None = None) -> set[str]:
    if isinstance(scope, ast.ClassDef):
        return _resolve_class_scope(scope, enclosing_aliases, violations)

    facts = _scope_facts(scope)
    aliases = (
        enclosing_aliases - facts.local_names
        if isinstance(scope, ast.FunctionDef | ast.AsyncFunctionDef)
        else enclosing_aliases
    )
    if isinstance(scope, ast.Module | ast.FunctionDef | ast.AsyncFunctionDef):
        initial_aliases = aliases.copy()
        aliases = _resolve_class_statements(scope.body, aliases, aliases, violations, inherit_current=True)
        if violations is not None:
            # A child can run after later bindings change an enclosing name.
            for child in facts.children:
                child_aliases = aliases
                if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                    if (
                        isinstance(scope, ast.FunctionDef | ast.AsyncFunctionDef)
                        and isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef)
                        and _child_calls_are_safe(scope, child, initial_aliases)
                    ):
                        continue
                    prior_statements = [statement for statement in scope.body if statement.lineno < child.lineno]
                    prior_aliases = _resolve_class_statements(
                        prior_statements, initial_aliases.copy(), initial_aliases, None
                    )
                    later_bindings = [
                        binding
                        for binding in facts.bindings
                        if any(getattr(value, "lineno", -1) >= child.lineno for value in binding[1])
                    ]
                    child_aliases = aliases | prior_aliases | _binding_aliases(later_bindings, prior_aliases)
                _resolve_scope(child, child_aliases, violations)
        return aliases
    return aliases | _binding_aliases(facts.bindings, aliases)


def _inspected_surface_aliases(tree: ast.AST) -> set[str]:
    return _resolve_scope(tree, set())


def _non_disclosure_assertion_lines(source: str) -> list[int]:
    """Return rewritten non-disclosure assertions over inspected values."""
    tree = ast.parse(source)
    violations: list[int] = []
    _resolve_scope(tree, set(), violations)
    return sorted(set(violations))


def _raised_with_context(inner: BaseException, outer: BaseException) -> BaseException:
    """Raise *outer* while *inner* is handled, so the interpreter attaches inner to it."""
    try:
        raise inner
    except BaseException:
        try:
            raise outer
        except BaseException as caught:
            return caught


def test_a_note_on_the_raised_exception_is_caught() -> None:
    """A note travels with the exception and a traceback prints it, message or not."""
    refusal = ValueError("The Vault operation failed (RuntimeError)")
    refusal.add_note(f"while reading {_SECRET}")

    if _SECRET in f"{refusal!r} {refusal}":
        raise AssertionError("the message itself must stay clean for this test")
    with pytest.raises(AssertionError, match="repeats secret material"):
        assert_chain_free_of(refusal, [_SECRET])


def test_a_note_DEEPER_in_the_chain_is_caught() -> None:
    """The provider's own exception stays on __context__, and so do its notes."""
    provider_failure = RuntimeError("vault: read failed")
    provider_failure.add_note(f"holding {_SECRET}")
    caught = _raised_with_context(provider_failure, ValueError("The Vault operation failed (RuntimeError)"))

    assert caught.__context__ is provider_failure
    with pytest.raises(AssertionError, match="repeats secret material"):
        assert_chain_free_of(caught, [_SECRET])


def test_a_GROUP_MEMBER_is_caught() -> None:
    """A task group raises a group: the members hang off it, not off cause/context."""
    member = RuntimeError("vault: read failed")
    member.add_note(f"holding {_SECRET}")
    try:
        raise ExceptionGroup("provisioning failed", [member])
    except ExceptionGroup as group:
        caught = group

    assert caught.__cause__ is None and caught.__context__ is None, "the member is reachable only as a member"
    if _SECRET in f"{caught!r} {caught}":
        raise AssertionError("only walking into the member can find this one")
    assert len(exception_chain(caught)) == 2, "the walker must reach the member"
    with pytest.raises(AssertionError, match="repeats secret material"):
        assert_chain_free_of(caught, [_SECRET])


def test_a_CLEAN_chain_passes() -> None:
    """The assertions above are not vacuous: a sanitized refusal with a clean note passes."""
    provider_failure = RuntimeError("vault: read failed")
    refusal = ValueError("The Vault operation failed (RuntimeError)")
    refusal.add_note("secrets.set operation_id=0123456789ab")
    caught = _raised_with_context(provider_failure, refusal)

    assert_chain_free_of(caught, [_SECRET])


def test_non_disclosure_checks_do_not_use_rewritten_assertions() -> None:
    violations = []
    for path in _NON_DISCLOSURE_TESTS:
        violations.extend(
            f"{path.relative_to(_TEST_ROOT.parent)}:{line}"
            for line in _non_disclosure_assertion_lines(path.read_text(encoding="utf-8"))
        )
    assert violations == []


def _reads_a_url_surface(node: ast.AST) -> bool:
    return any(isinstance(part, ast.Attribute) and part.attr == "url" for part in ast.walk(node))


def _url_membership_lines(source: str) -> list[int]:
    """Assertions that let pytest print a request URL, which carries the device name."""
    return [
        node.lineno
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Assert)
        and (
            any(
                any(isinstance(operator, ast.In | ast.NotIn) for operator in comparison.ops)
                and any(_reads_a_url_surface(value) for value in (comparison.left, *comparison.comparators))
                for comparison in _assertion_comparisons(node.test)
            )
            or (node.msg is not None and _reads_a_url_surface(node.msg))
        )
    ]


def test_url_membership_checks_go_through_a_non_disclosure_helper() -> None:
    """``assert "reconcile=" in str(req.url)`` prints the whole URL, device name included."""
    violations = []
    # Every module, not the curated registry: the URL surface IS the protected material here,
    # so a module that holds nothing of its own still discloses the device name through it.
    for path in sorted(_TEST_ROOT.rglob("test_*.py")):
        for lineno in _url_membership_lines(path.read_text(encoding="utf-8")):
            violations.append(f"{path.relative_to(_TEST_ROOT.parent)}:{lineno}")
    assert violations == []


def test_the_url_membership_rule_reads_the_assertion_and_not_its_filters() -> None:
    """A comprehension filter renders a count, and the helper call is not an assertion at all."""
    membership = 'assert "reconcile=" in str(put_req.url)'
    absence = 'assert "dry-run=native" not in str(request.url)'
    message = "assert verify, [str(r.url) for r in requests]"
    comprehension_filter = 'assert len([r for r in requests if "dry-run" not in str(r.url)]) == 1'
    helper = 'assert_text_contains(put_req.url, ["reconcile="])'

    assert _url_membership_lines(membership) == [1]
    assert _url_membership_lines(absence) == [1]
    assert _url_membership_lines(message) == [1]
    assert _url_membership_lines(comprehension_filter) == []
    assert _url_membership_lines(helper) == []


def _protected_roots(call: ast.Call) -> set[str]:
    """The names a non-disclosure helper was asked to clear, e.g. ``response`` for ``response.text``."""
    if not call.args:
        return set()
    return {part.id for part in ast.walk(call.args[0]) if isinstance(part, ast.Name)}


def _own_body(scope: ast.AST):
    """Every node of *scope* except the bodies of functions nested inside it.

    Line order is execution order only within one body. A callback defined before a clearing call
    runs after it, so judging its assertions by line number would report the wrong answer twice
    over: a false positive on the callback, and a false negative for a clear placed inside one.
    """
    stack = list(ast.iter_child_nodes(scope))
    while stack:
        node = stack.pop()
        yield node
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
            stack.extend(ast.iter_child_nodes(node))


_CONDITIONAL_CALL_CONTAINERS = (
    ast.If
    | ast.Try
    | ast.TryStar
    | ast.For
    | ast.AsyncFor
    | ast.While
    | ast.Match
    | ast.IfExp
    | ast.BoolOp
    | ast.ListComp
    | ast.SetComp
    | ast.DictComp
    | ast.GeneratorExp
)


def _unconditional_calls(scope: ast.AST):
    """Calls in *scope*'s function, with, or async with bodies only."""
    stack = list(scope.body)
    while stack:
        node = stack.pop()
        if isinstance(node, ast.Call):
            yield node
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda | _CONDITIONAL_CALL_CONTAINERS):
            stack.extend(ast.iter_child_nodes(node))


def _rebinding_lines(scope: ast.AST, root: str) -> list[int]:
    """The lines of *scope*'s own body that bind *root* to a new value."""
    lines = []
    for node in _own_body(scope):
        targets: list[ast.AST] = []
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
        elif isinstance(node, ast.AnnAssign | ast.AugAssign | ast.NamedExpr):
            targets = [node.target]
        elif isinstance(node, ast.For | ast.AsyncFor):
            targets = [node.target]
        elif isinstance(node, ast.withitem):
            targets = [node.optional_vars] if node.optional_vars is not None else []
        for target in targets:
            if any(isinstance(part, ast.Name) and part.id == root for part in ast.walk(target)):
                lines.append(getattr(node, "lineno", 0) or 0)
    return lines


def _renders_root(node: ast.AST, root: str) -> bool:
    """True when *node* renders *root* whole: the bare name, a text surface, or str/repr of it."""
    for part in ast.walk(node):
        if isinstance(part, ast.Name) and part.id == root:
            return True
        if isinstance(part, ast.Attribute) and part.attr in _RENDERED_SURFACE_ATTRIBUTES:
            if any(isinstance(inner, ast.Name) and inner.id == root for inner in ast.walk(part)):
                return True
    return False


def _renders_root_through_a_surface(node: ast.AST, root: str) -> bool:
    """True when *root* is rendered through a text surface or an explicit str/repr, never bare."""
    for part in ast.walk(node):
        reads_surface = (isinstance(part, ast.Attribute) and part.attr in _RENDERED_SURFACE_ATTRIBUTES) or (
            isinstance(part, ast.Call) and isinstance(part.func, ast.Name) and part.func.id in _INSPECTED_CALLS
        )
        if reads_surface and any(isinstance(inner, ast.Name) and inner.id == root for inner in ast.walk(part)):
            return True
    return False


def _rendered_surfaces(node: ast.AST) -> list[ast.AST]:
    """What *node* reads a rendered surface OFF, e.g. ``resp`` for ``resp.text``.

    Decided by the OUTERMOST expression, unlike :func:`_renders_root_through_a_surface`.
    Narrowing a surface produces a different, smaller value, so
    ``caught.value.response.status_code`` renders an int and ``resp.json()["error"]["code"]``
    renders one authored string: neither reads a surface off anything.
    """
    if isinstance(node, ast.Attribute) and node.attr in _RENDERED_SURFACE_ATTRIBUTES:
        return [node.value]
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr in _RENDERED_SURFACE_ATTRIBUTES:
            return [node.func.value]
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in _INSPECTED_CALLS:
        return list(node.args)
    if isinstance(node, ast.List | ast.Tuple | ast.Set):
        return [source for element in node.elts for source in _rendered_surfaces(element)]
    return []


def _renders_root_whole(node: ast.AST, root: str) -> bool:
    """True when *node* ITSELF is what pytest prints for *root*: the name, or a surface off it.

    A sequence renders every element, which is how ``== [caught.value]`` prints the exception
    and ``== [detail]`` prints the local.
    """
    if isinstance(node, ast.Name):
        return node.id == root
    if isinstance(node, ast.List | ast.Tuple | ast.Set):
        return any(_renders_root_whole(element, root) for element in node.elts)
    return any(
        any(isinstance(inner, ast.Name) and inner.id == root for inner in ast.walk(source))
        for source in _rendered_surfaces(node)
    )


def _direct_secret_equality_lines(source: str) -> list[int]:
    """Find pytest equality checks that would print a named secret on failure."""
    lines = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Assert) or not isinstance(node.test, ast.Compare):
            continue
        if not any(isinstance(op, ast.Eq | ast.NotEq) for op in node.test.ops):
            continue
        operands = (node.test.left, *node.test.comparators)
        roots = {
            part.id
            for operand in operands
            for part in ast.walk(operand)
            if isinstance(part, ast.Name) and "secret" in part.id.casefold()
        }
        if any(_renders_root_whole(operand, root) for root in roots for operand in operands):
            lines.append(node.lineno)
    return lines


def _discloses_protected_value(node: ast.Assert, root: str) -> bool:
    """True when a FAILURE of *node* prints the protected surface whole.

    Scoped to what the syntax decides on its own. A failure message is printed verbatim, and an
    operand that renders the protected value whole prints all of it: the bare name, a text
    surface, an explicit ``str``/``repr``, or a sequence holding one of those.

    The two operator kinds read their operands differently, so they are judged differently. A
    membership operand is the HAYSTACK being searched for the protected value, so narrowing it
    still renders text that came from the protected value: it is judged on the whole chain. A
    BARE container operand is rendered too, because pytest prints both operands of a membership
    test: ``assert "nso_device" not in record`` fails as ``assert 'nso_device' not in {'nso_device':
    'placeholder-secret'}``. Every other operator compares a value against an authored one, so
    narrowing produces a different, smaller value and is judged on the operand's outermost
    expression, which is why ``resp.json()["error"]["code"] == "vault_error"`` renders neither.
    """
    if node.msg is not None and _renders_root(node.msg, root):
        return True
    for part in ast.walk(node.test):
        if not isinstance(part, ast.Compare):
            continue
        operands = (part.left, *part.comparators)
        if any(isinstance(operator, ast.In | ast.NotIn) for operator in part.ops) and any(
            _renders_root_through_a_surface(operand, root) for operand in operands
        ):
            return True
        if any(_renders_root_whole(operand, root) for operand in operands):
            return True
    return False


def _declares_protected_material(scope: ast.AST) -> bool:
    """True when *scope*'s own body writes protected material by the placeholder convention."""
    return any(
        isinstance(part, ast.Constant)
        and isinstance(part.value, str)
        and part.value.startswith(_PROTECTED_LITERAL_PREFIX)
        for part in _own_body(scope)
    )


def _unnamed_protected_roots(asserts: list[ast.Assert]) -> set[str]:
    """The roots an assertion renders without any helper having named them.

    A helper call names what the author is protecting. A body that writes protected material but
    renders a surface into a diagnostic never named it, so the surface itself is the root: that
    assertion prints the value and no call has cleared it. An operand renders a surface the same
    way a message does, so both are read.
    """
    roots: set[str] = set()
    for node in asserts:
        sources = [node.msg] if node.msg is not None and _reads_an_inspected_surface(node.msg) else []
        for part in ast.walk(node.test):
            if isinstance(part, ast.Compare):
                for operand in (part.left, *part.comparators):
                    sources.extend(_rendered_surfaces(operand))
        for source in sources:
            roots.update(part.id for part in ast.walk(source) if isinstance(part, ast.Name))
    return roots


def _ordering_violations_in(scope: ast.AST) -> list[tuple[int, str]]:
    """The assertions in *scope*'s own body that render a protected value before it is cleared.

    A clearing call protects the value it was given from that line on. Re-binding the name after
    the clear produces a value nothing has checked, so the next assertion on it counts again.
    """
    clears: dict[str, list[int]] = {}
    asserts: list[ast.Assert] = []
    for part in _own_body(scope):
        if isinstance(part, ast.Call) and isinstance(part.func, ast.Name) and part.func.id in _NON_DISCLOSURE_HELPERS:
            for root in _protected_roots(part):
                clears.setdefault(root, [])
        elif isinstance(part, ast.Assert):
            asserts.append(part)
    for call in _unconditional_calls(scope):
        if isinstance(call.func, ast.Name) and call.func.id in _NON_DISCLOSURE_HELPERS:
            for root in _protected_roots(call):
                clears[root].append(call.lineno)

    candidates = dict(clears)
    if _declares_protected_material(scope):
        for root in _unnamed_protected_roots(asserts):
            candidates.setdefault(root, [])

    violations = []
    for node in asserts:
        for root, lines in candidates.items():
            if not _discloses_protected_value(node, root):
                continue
            earlier = [line for line in lines if line < node.lineno]
            if not earlier:
                violations.append((node.lineno, root))
                continue
            if any(max(earlier) < line < node.lineno for line in _rebinding_lines(scope, root)):
                violations.append((node.lineno, root))
    return sorted(set(violations))


def test_non_disclosure_checks_run_before_the_diagnostics() -> None:
    """pytest prints a failing assertion's operands, so the helper has to clear the value first."""
    violations = []
    for path in _NON_DISCLOSURE_TESTS:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for scope in ast.walk(tree):
            if not isinstance(scope, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            for lineno, root in _ordering_violations_in(scope):
                violations.append(f"{path.relative_to(_TEST_ROOT.parent)}:{lineno} discloses {root!r}")
    assert violations == []


def test_secret_constants_are_not_compared_with_rewritten_assertions() -> None:
    violations = [
        f"{path.relative_to(_TEST_ROOT.parent)}:{line}"
        for path in sorted(_TEST_ROOT.rglob("test_*.py"))
        for line in _direct_secret_equality_lines(path.read_text(encoding="utf-8"))
    ]
    assert violations == []


def test_secret_equality_guard_reads_rendered_operands() -> None:
    assert _direct_secret_equality_lines("assert received == SECRET_STREAM_URL") == [1]
    assert _direct_secret_equality_lines("assert len(received) == len(SECRET_STREAM_URL)") == []
    assert (
        _direct_secret_equality_lines(
            'if received != SECRET_STREAM_URL:\n    raise AssertionError("stream URL differed")'
        )
        == []
    )


def _rewritten_check_lines(source: str) -> list[int]:
    """Run the SAME rule ``test_non_disclosure_checks_do_not_use_rewritten_assertions`` runs.

    Delegated, not re-walked: a second walk has no alias resolution, so the snippets would
    exercise a rule the suite does not ship.
    """
    return _non_disclosure_assertion_lines(source)


def test_the_rewritten_assertion_rule_reads_the_assertion_and_not_its_filters() -> None:
    """A rendered surface discloses whatever the left operand is; a filter renders only a count."""
    named_value = "assert protected not in resp.text"
    marker_in_a_surface = 'assert "dry-run=native" not in str(request.url)'
    rendered_call = "assert secret not in repr(record)"
    comprehension_filter = 'assert len([r for r in requests if "dry-run" not in str(r.url)]) == 1'
    key_membership = 'assert "device_id" not in record'
    reverse_operand = "assert resp.text not in allowed"

    assert _rewritten_check_lines(named_value) == [1]
    assert _rewritten_check_lines(reverse_operand) == [1], "pytest prints the left operand too"
    assert _rewritten_check_lines(marker_in_a_surface) == [1], "the URL carries the device name"
    assert _rewritten_check_lines(rendered_call) == [1]
    assert _rewritten_check_lines(comprehension_filter) == []
    assert _rewritten_check_lines(key_membership) == []


def _ordering_violations(source: str) -> list[int]:
    """Run the SAME rule the test above runs, over one snippet."""
    return [lineno for lineno, _ in _ordering_violations_in(ast.parse(source).body[0])]


def test_the_ordering_rule_reads_the_shapes_that_disclose_and_no_others() -> None:
    """A failure message, a bare operand and a membership container all render the value."""
    message = """\
def t():
    assert resp.status_code == 422, resp.text
    assert_text_free_of(resp.text, [protected])
"""
    bare_operand = """\
def t():
    assert detail == "authored text"
    assert_text_free_of(detail, [protected])
"""
    key_membership = """\
def t():
    assert "device_id" not in record
    assert_records_free_of([record], [protected])
"""
    cleared_key_membership = """\
def t():
    assert_records_free_of([record], [protected])
    assert "device_id" not in record
"""
    narrowed_operand = """\
def t():
    assert record["error"] == "ReadTimeout"
    assert_records_free_of([record], [protected])
"""
    rendered_membership = """\
def t():
    assert "wanted" in str(caught.value)
    assert_chain_free_of(caught.value, [protected])
"""

    assert _ordering_violations(message) == [2]
    assert _ordering_violations(bare_operand) == [2]
    assert _ordering_violations(rendered_membership) == [2]
    # pytest prints BOTH operands of a membership test, so the container is rendered whole.
    assert _ordering_violations(key_membership) == [2]
    assert _ordering_violations(cleared_key_membership) == []
    assert _ordering_violations(narrowed_operand) == []


def test_the_ordering_rule_reads_equality_operands_the_way_pytest_prints_them() -> None:
    """Equality renders its operands too; only the OUTERMOST expression says what it prints."""
    rendered_equality = """\
def t():
    assert str(caught.value) == expected
    assert_chain_free_of(caught.value, [protected])
"""
    sequence_element = """\
def t():
    assert collected == [resp.text]
    assert_text_free_of(resp.text, [protected])
"""
    surface_equality = """\
def t():
    assert resp.text == expected
    assert_text_free_of(resp.text, [protected])
"""
    whole_json = """\
def t():
    assert resp.json() == expected
    assert_text_free_of(resp.text, [protected])
"""
    narrowed_surface = """\
def t():
    assert caught.value.response.status_code == 503
    assert_chain_free_of(caught.value, [protected])
"""
    narrowed_json = """\
def t():
    assert resp.json()["error"]["code"] == "vault_error"
    assert_text_free_of(resp.text, [protected])
"""

    assert _ordering_violations(rendered_equality) == [2]
    assert _ordering_violations(sequence_element) == [2], "a list prints every element it holds"
    assert _ordering_violations(surface_equality) == [2]
    assert _ordering_violations(whole_json) == [2]
    assert _ordering_violations(narrowed_surface) == [], "a status code is not the exception"
    assert _ordering_violations(narrowed_json) == [], "one authored code is not the body"


def test_an_operand_names_the_protected_root_when_no_helper_does() -> None:
    """Candidate discovery reads operands, not only messages, or an equality assertion hides."""
    operand_only = """\
def t():
    seeded = "placeholder-secret"
    assert resp.text == expected
"""
    message_only = """\
def t():
    seeded = "placeholder-secret"
    assert resp.status_code == 200, resp.text
"""

    assert _ordering_violations(operand_only) == [3]
    assert _ordering_violations(message_only) == [3]


def test_a_name_rebound_after_its_clear_is_unchecked_again() -> None:
    """The first clear protects the value it was given, not every later value of that name."""
    rebound = """\
def t():
    resp = post(a)
    assert_text_free_of(resp.text, [protected])
    resp = post(b)
    assert resp == expected
    assert_text_free_of(resp.text, [protected])
"""
    not_rebound = """\
def t():
    assert_text_free_of(caught.value, [protected])
    assert "wanted" in str(caught.value)
    assert_chain_free_of(caught.value, [protected])
"""

    assert _ordering_violations(rebound) == [5], "the value after the re-bind was never cleared"
    assert _ordering_violations(not_rebound) == [], "one clear covers the assertions after it"


def test_a_conditional_clear_does_not_protect_a_later_diagnostic() -> None:
    conditional_clear = """\
def t():
    if should_clear:
        assert_text_free_of(resp.text, [protected])
    assert resp.status_code == 200, resp.text
"""

    assert _ordering_violations(conditional_clear) == [4]


@pytest.mark.parametrize(
    "expression",
    [
        "assert_text_free_of(resp.text, [protected]) if should_clear else None",
        "should_clear and assert_text_free_of(resp.text, [protected])",
        "[assert_text_free_of(resp.text, [protected]) for item in items]",
        "{assert_text_free_of(resp.text, [protected]) for item in items}",
        "{item: assert_text_free_of(resp.text, [protected]) for item in items}",
        "(assert_text_free_of(resp.text, [protected]) for item in items)",
    ],
)
def test_expression_conditional_clears_do_not_protect_a_later_diagnostic(expression: str) -> None:
    source = f"def t():\n    {expression}\n    assert resp.status_code == 200, resp.text\n"

    assert _ordering_violations(source) == [3]


def test_a_body_holding_protected_material_needs_no_helper_call_to_be_judged() -> None:
    """A diagnostic that renders a response is uncleared even when no helper names it."""
    protected_and_uncleared = """\
def t():
    resp = post({"community": "placeholder-secret"})
    assert resp.status_code == 200, resp.text
"""
    protected_and_cleared = """\
def t():
    resp = post({"community": "placeholder-secret"})
    assert_text_free_of(resp.text, ["placeholder-secret"])
    assert resp.status_code == 200, resp.text
"""
    no_protected_material = """\
def t():
    resp = post({"name": "seed-device"})
    assert resp.status_code == 200, resp.text
"""

    assert _ordering_violations(protected_and_uncleared) == [3]
    assert _ordering_violations(protected_and_cleared) == []
    assert _ordering_violations(no_protected_material) == [], (
        "the ordinary status-and-body idiom is not a disclosure check"
    )


def test_ordering_is_judged_per_body_because_a_callback_runs_later() -> None:
    """A closure defined before the clear runs after it, so its line number proves nothing."""
    callback_defined_early = """\
def t():
    def on_event(record):
        assert record == expected
    run(on_event)
    assert_records_free_of([record], [protected])
"""

    assert _ordering_violations(callback_defined_early) == [], "the callback is not part of this body"


def test_a_clear_inside_an_immediately_invoked_lambda_earns_no_credit() -> None:
    nested_clear = """\
def t():
    protected = "placeholder-secret"
    (lambda: assert_text_free_of(resp.text, [protected]))()
    assert resp.status_code == 200, resp.text
"""

    assert _ordering_violations(nested_clear) == [4]


def test_membership_and_rendered_surface_rules_answer_separate_questions() -> None:
    """`"local_as" not in peer` asks whether a KEY is absent. `assert_text_free_of` cannot express
    that. It substring-matches the rendered mapping and would pass or fail for an unrelated
    reason. Whole-object equality prints the complete decoded response instead.
    """
    container = """\
body = response.json()
peer = body["peers"][0]
assert "local_as" not in peer
"""
    whole_json = """\
def t():
    assert response.json() == expected
    assert_text_free_of(response.text, [protected])
"""
    text = """\
body = response.text
assert protected not in body
"""

    assert _non_disclosure_assertion_lines(container) == []
    assert _non_disclosure_assertion_lines(text) == [2]
    assert _ordering_violations(whole_json) == [2]


def test_membership_guard_covers_complete_decoded_surfaces() -> None:
    source = """\
assert protected not in response.json()
assert protected not in caught.value
assert protected not in result.read_failures()
assert "device_id" not in response.json()["record"]
assert queued not in [record["id"] for record in response.json()]
"""

    assert _non_disclosure_assertion_lines(source) == [1, 2, 3]


def test_a_NOT_IN_used_as_a_comprehension_filter_is_not_a_disclosure_check() -> None:
    """The assertion renders the comprehension's RESULT — a count — never the element."""
    filtered = """\
assert len([item for item in requests if "dry-run" not in str(item.url)]) == 1
"""
    rendered = """\
assert protected not in str(request.url)
"""

    assert _non_disclosure_assertion_lines(filtered) == []
    assert _non_disclosure_assertion_lines(rendered) == [1]


def test_the_guard_reads_EVERY_test_module() -> None:
    """The allowlist is gone and must stay gone: three review rounds found assertions escaping
    through modules nobody had added to it."""
    assert set(_NON_DISCLOSURE_TESTS) == set(_TEST_ROOT.rglob("test_*.py"))
    assert len(_NON_DISCLOSURE_TESTS) > 200, "the sweep should see the whole suite"


def test_non_disclosure_aliases_follow_bindings_without_cross_scope_contamination() -> None:
    copied_alias = """\
captured = response.text
leaked = captured
assert protected not in leaked
"""
    cross_scope = """\
captured = "authored detail"
def nested():
    captured = response.text
assert protected not in captured
"""

    assert _non_disclosure_assertion_lines(copied_alias) == [3]
    assert _non_disclosure_assertion_lines(cross_scope) == []


def test_non_disclosure_aliases_follow_module_and_function_control_flow() -> None:
    module_overwrite = "captured = response.text\ncaptured = 'authored'\nassert protected not in captured\n"
    function_overwrite = (
        "def check(flag):\n"
        "    captured = response.text\n"
        "    if flag:\n"
        "        return\n"
        "    captured = 'authored'\n"
        "    assert protected not in captured\n"
    )
    exceptional_finally = (
        "def check():\n"
        "    try:\n"
        "        captured = response.text\n"
        "        captured = 'authored'\n"
        "    finally:\n"
        "        assert protected not in captured\n"
    )
    module_child = (
        "captured = response.text\ncaptured = 'authored'\ndef check():\n    assert protected not in captured\n"
    )
    terminal_return = (
        "def check(flag):\n"
        "    captured = 'authored'\n"
        "    if flag:\n"
        "        captured = response.text\n"
        "        return\n"
        "    assert protected not in captured\n"
    )
    terminal_raise = terminal_return.replace("return", "raise RuntimeError()")
    return_through_finally = (
        "def check():\n"
        "    try:\n"
        "        captured = response.text\n"
        "        return\n"
        "    finally:\n"
        "        assert protected not in captured\n"
    )

    for source in (
        module_overwrite,
        function_overwrite,
        exceptional_finally,
        module_child,
        terminal_return,
        terminal_raise,
    ):
        assert _non_disclosure_assertion_lines(source) == [], source
    assert _non_disclosure_assertion_lines(return_through_finally) == [6]


def test_deferred_scopes_see_aliases_bound_before_they_run() -> None:
    module_function = "def check():\n    assert protected not in captured\ncaptured = response.text\ncheck()\n"
    class_method = (
        "class Check:\n"
        "    def check(self):\n"
        "        assert protected not in captured\n"
        "captured = response.text\n"
        "Check().check()\n"
    )
    function_closure = (
        "def outer():\n"
        "    def check():\n"
        "        assert protected not in captured\n"
        "    captured = response.text\n"
        "    check()\n"
    )

    assert _non_disclosure_assertion_lines(module_function) == [2]
    assert _non_disclosure_assertion_lines(class_method) == [3]
    assert _non_disclosure_assertion_lines(function_closure) == [3]


def test_deferred_children_keep_later_aliases_across_returns_and_callbacks() -> None:
    returned = (
        "def factory():\n"
        "    def check():\n"
        "        assert protected not in captured\n"
        "    captured = response.text\n"
        "    return check\n"
    )
    aliased_call = (
        "def outer():\n"
        "    def check():\n"
        "        assert protected not in captured\n"
        "    saved = check\n"
        "    captured = response.text\n"
        "    saved()\n"
        "    captured = 'authored'\n"
    )
    callback = (
        "def run(callback):\n"
        "    callback()\n"
        "def outer():\n"
        "    def check():\n"
        "        assert protected not in captured\n"
        "    captured = response.text\n"
        "    run(check)\n"
        "    captured = 'authored'\n"
    )
    alias_crosses_child_definition = (
        "def factory():\n"
        "    base = response.text\n"
        "    def check():\n"
        "        assert protected not in captured\n"
        "    captured = base\n"
        "    return check\n"
    )

    assert _non_disclosure_assertion_lines(returned) == [3]
    assert _non_disclosure_assertion_lines(aliased_call) == [3]
    assert _non_disclosure_assertion_lines(callback) == [5]
    assert _non_disclosure_assertion_lines(alias_crosses_child_definition) == [4]


def test_nested_function_calls_follow_the_bound_function_on_each_path() -> None:
    cleared_before_call = (
        "def outer():\n"
        "    captured = response.text\n"
        "    def check():\n"
        "        assert protected not in captured\n"
        "    captured = 'authored'\n"
        "    check()\n"
    )
    correlated_binding = (
        "def outer(flag):\n"
        "    captured = response.text\n"
        "    def check():\n"
        "        assert protected not in captured\n"
        "    def harmless():\n"
        "        return None\n"
        "    if flag:\n"
        "        bound = harmless\n"
        "    else:\n"
        "        captured = 'authored'\n"
        "        bound = check\n"
        "    bound()\n"
    )
    leaking_binding = correlated_binding.replace("bound = harmless", "bound = check")
    indirect_call = (
        "def outer():\n"
        "    def run():\n"
        "        check()\n"
        "    captured = response.text\n"
        "    def check():\n"
        "        assert protected not in captured\n"
        "    run()\n"
        "    captured = 'authored'\n"
    )
    default_taints_before_call = (
        "def outer():\n"
        "    captured = 'authored'\n"
        "    def check(value=(captured := response.text)):\n"
        "        assert protected not in captured\n"
        "    check()\n"
    )
    argument_taints_before_call = (
        "def outer():\n"
        "    captured = 'authored'\n"
        "    def check(value):\n"
        "        assert protected not in captured\n"
        "    check(captured := response.text)\n"
    )

    assert _non_disclosure_assertion_lines(cleared_before_call) == []
    assert _non_disclosure_assertion_lines(correlated_binding) == []
    assert _non_disclosure_assertion_lines(leaking_binding) == [4]
    assert _non_disclosure_assertion_lines(indirect_call) == [6]
    assert _non_disclosure_assertion_lines(default_taints_before_call) == [4]
    assert _non_disclosure_assertion_lines(argument_taints_before_call) == [4]


def test_bare_return_does_not_enter_an_exception_handler() -> None:
    no_exception = (
        "def check():\n"
        "    try:\n"
        "        captured = response.text\n"
        "        return\n"
        "    except Exception:\n"
        "        assert protected not in captured\n"
    )
    possible_exception = no_exception.replace("return", "return might_raise()")
    no_exception_with_incoming_alias = (
        "def check():\n"
        "    captured = response.text\n"
        "    try:\n"
        "        return\n"
        "    except Exception:\n"
        "        assert protected not in captured\n"
    )
    context_exit_can_raise = (
        "def check():\n"
        "    try:\n"
        "        with context():\n"
        "            captured = response.text\n"
        "            return\n"
        "    except Exception:\n"
        "        assert protected not in captured\n"
    )

    assert _non_disclosure_assertion_lines(no_exception) == []
    assert _non_disclosure_assertion_lines(possible_exception) == [6]
    assert _non_disclosure_assertion_lines(no_exception_with_incoming_alias) == []
    assert _non_disclosure_assertion_lines(context_exit_can_raise) == [7]


def test_an_immediately_invoked_lambda_is_part_of_the_disclosure_surface() -> None:
    immediately_invoked = """\
def t():
    protected = "placeholder-secret"
    assert protected not in (lambda: resp.text)()
"""

    assert _non_disclosure_assertion_lines(immediately_invoked) == [3]


def test_a_lambda_passed_as_a_value_is_not_part_of_the_disclosure_surface() -> None:
    passed_as_a_value = """\
def t():
    protected = "placeholder-secret"
    assert protected not in inspect_later(lambda: resp.text)
"""

    assert _non_disclosure_assertion_lines(passed_as_a_value) == []


def test_a_for_target_carries_the_iterables_values() -> None:
    """A loop target is a binding like any other: without it the scanner saw a bare name.

    The scanner already binds assignment, walrus and match-capture targets, so a surface
    iterated over in a `for` was the one spelling that reached an assertion unrecognized.
    """
    iterated = "for value in [response.text]:\n    assert protected not in value\n"
    authored = 'for value in ["authored detail"]:\n    assert protected not in value\n'
    unpacked = "for value, _ in [(response.text, 0)]:\n    assert protected not in value\n"

    assert _non_disclosure_assertion_lines(iterated) == [2]
    assert _non_disclosure_assertion_lines(authored) == []
    assert _non_disclosure_assertion_lines(unpacked) == [2]


def test_text_non_disclosure_failure_does_not_echo_the_material() -> None:
    from tests._secret_discipline import assert_text_free_of

    protected = "placeholder-protected-material"
    try:
        assert_text_free_of({"interface_id": protected}, [protected])
    except AssertionError as exc:
        if protected in str(exc):
            raise AssertionError("the non-disclosure check echoed the protected material") from None
    else:
        raise AssertionError("the non-disclosure check accepted protected material")
