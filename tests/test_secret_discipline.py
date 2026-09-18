# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""What the secret-discipline assertions must inspect and keep out of diagnostics."""

from __future__ import annotations

import ast
from functools import cache
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
#: How this repository writes protected material into a test (see the placeholder convention). A
#: body that names one is handling something protected, whether or not it calls a helper.
_PROTECTED_LITERAL_PREFIX = "placeholder-"
_NON_DISCLOSURE_HELPERS = {"assert_chain_free_of", "assert_records_free_of", "assert_text_free_of"}


def _handles_protected_material(tree: ast.AST) -> bool:
    """True when a module holds something protected, by either way this repository says so."""
    return any(
        (isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in _NON_DISCLOSURE_HELPERS)
        or (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and node.value.startswith(_PROTECTED_LITERAL_PREFIX)
        )
        for node in ast.walk(tree)
    )


@cache
def _guarded_modules() -> tuple[Path, ...]:
    """The modules both rules below read, DERIVED from what each one handles.

    A hand-kept list omits a module the moment it starts handling protected material, and both
    rules then skip it in silence: that is how ``core/test_capability.py`` reached review with
    neither rule covering it. Deriving the membership removes the omission rather than the
    symptom. A blanket sweep of every test module is a different rule with a different cost,
    so the derivation stays on what a module actually holds.
    """
    return tuple(
        path
        for path in sorted(_TEST_ROOT.rglob("test_*.py"))
        if _handles_protected_material(ast.parse(path.read_text(encoding="utf-8")))
    )


_INSPECTED_ATTRIBUTES = {"json", "read_failures", "text", "value"}
_INSPECTED_CALLS = {"repr", "str"}


def test_main_lifespan_is_in_the_non_disclosure_registry() -> None:
    """It calls the helpers, so the derivation has to pick it up without anyone listing it."""
    assert _TEST_ROOT / "test_main_lifespan.py" in _guarded_modules()


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
        if node.attr in _INSPECTED_ATTRIBUTES:
            self.found = True
            return
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802 - ast visitor API
        if isinstance(node.func, ast.Name) and node.func.id in _INSPECTED_CALLS:
            self.found = True
            return
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


def _resolve_bindings(bindings: list[tuple[str, list[ast.AST]]], aliases: set[str]) -> set[str]:
    return aliases | _binding_aliases(bindings, aliases)


def _record_non_disclosure_assertions(assertions: list[ast.Assert], aliases: set[str], violations: list[int]) -> None:
    for node in assertions:
        if any(
            any(isinstance(operator, ast.NotIn) for operator in comparison.ops)
            and any(_reads_an_inspected_surface(value, aliases) for value in comparison.comparators)
            for comparison in _assertion_comparisons(node.test)
        ):
            violations.append(node.lineno)


def _resolve_class_node(
    node: ast.AST,
    aliases: set[str],
    enclosing_aliases: set[str],
    violations: list[int] | None,
) -> set[str]:
    facts = _ScopeFacts()
    facts.visit(node)
    bound_aliases = _binding_aliases(facts.bindings, aliases)
    aliases = aliases - facts.local_names | bound_aliases
    if violations is not None:
        _record_non_disclosure_assertions(facts.assertions, aliases, violations)
        for child in facts.children:
            _resolve_scope(child, enclosing_aliases, violations)
    return aliases


def _resolve_class_if(
    statement: ast.If,
    aliases: set[str],
    enclosing_aliases: set[str],
    violations: list[int] | None,
    observed_states: list[set[str]] | None,
) -> set[str]:
    aliases = _resolve_class_node(statement.test, aliases, enclosing_aliases, violations)
    body_aliases = _resolve_class_statements(
        statement.body, aliases.copy(), enclosing_aliases, violations, observed_states
    )
    else_aliases = _resolve_class_statements(
        statement.orelse, aliases.copy(), enclosing_aliases, violations, observed_states
    )
    return body_aliases | else_aliases


def _resolve_class_try(
    statement: ast.Try | ast.TryStar,
    aliases: set[str],
    enclosing_aliases: set[str],
    violations: list[int] | None,
    observed_states: list[set[str]] | None,
) -> set[str]:
    incoming = aliases.copy()
    body_states: list[set[str]] = []
    body_aliases = _resolve_class_statements(
        statement.body, incoming.copy(), enclosing_aliases, violations, body_states
    )
    else_states: list[set[str]] = []
    normal_aliases = _resolve_class_statements(
        statement.orelse, body_aliases.copy(), enclosing_aliases, violations, else_states
    )
    handler_input = set().union(incoming, *body_states)
    handler_aliases = []
    handler_exception_states: list[set[str]] = []
    for handler in statement.handlers:
        state = handler_input.copy()
        if handler.type is not None:
            state = _resolve_class_node(handler.type, state, enclosing_aliases, violations)
        if handler.name is not None:
            state.discard(handler.name)
        handler_states: list[set[str]] = []
        state = _resolve_class_statements(handler.body, state, enclosing_aliases, violations, handler_states)
        if handler.name is not None:
            state.discard(handler.name)
            for handler_state in handler_states:
                handler_state.discard(handler.name)
        handler_exception_states.extend(handler_states)
        handler_aliases.append(state)
    aliases = normal_aliases | set().union(*handler_aliases, set())
    normal_aliases = _resolve_class_statements(
        statement.finalbody, aliases, enclosing_aliases, violations, observed_states
    )
    exceptional_states = else_states + handler_exception_states
    if not any(handler.type is None for handler in statement.handlers):
        exceptional_states += body_states
    exceptional_aliases = set().union(*exceptional_states, set())
    if exceptional_aliases:
        propagated_aliases = _resolve_class_statements(
            statement.finalbody, exceptional_aliases, enclosing_aliases, violations, observed_states
        )
        if observed_states is not None:
            observed_states.append(propagated_aliases)
    return normal_aliases


def _resolve_class_for(
    statement: ast.For | ast.AsyncFor,
    aliases: set[str],
    enclosing_aliases: set[str],
    violations: list[int] | None,
    observed_states: list[set[str]] | None,
) -> set[str]:
    incoming = _resolve_class_node(statement.iter, aliases, enclosing_aliases, violations)
    loop_entry = incoming.copy()
    while True:
        target_aliases = _binding_aliases(_target_value_bindings(statement.target, statement.iter), loop_entry)
        iteration_aliases = loop_entry - _target_names(statement.target) | target_aliases
        body_aliases = _resolve_class_statements(
            statement.body, iteration_aliases, enclosing_aliases, violations, observed_states
        )
        expanded_entry = loop_entry | body_aliases
        if expanded_entry == loop_entry:
            break
        loop_entry = expanded_entry
    else_aliases = _resolve_class_statements(
        statement.orelse, loop_entry, enclosing_aliases, violations, observed_states
    )
    return loop_entry | else_aliases


def _resolve_class_while(
    statement: ast.While,
    aliases: set[str],
    enclosing_aliases: set[str],
    violations: list[int] | None,
    observed_states: list[set[str]] | None,
) -> set[str]:
    initial = aliases.copy()
    loop_entry = initial.copy()
    while True:
        tested_aliases = _resolve_class_node(statement.test, loop_entry, enclosing_aliases, violations)
        body_aliases = _resolve_class_statements(
            statement.body, tested_aliases, enclosing_aliases, violations, observed_states
        )
        expanded_entry = loop_entry | body_aliases
        if expanded_entry == loop_entry:
            break
        loop_entry = expanded_entry
    else_aliases = _resolve_class_statements(
        statement.orelse, initial | loop_entry, enclosing_aliases, violations, observed_states
    )
    return initial | loop_entry | else_aliases


def _resolve_class_with(
    statement: ast.With | ast.AsyncWith,
    aliases: set[str],
    enclosing_aliases: set[str],
    violations: list[int] | None,
    observed_states: list[set[str]] | None,
) -> set[str]:
    incoming = aliases.copy()
    for item in statement.items:
        incoming = _resolve_class_node(item.context_expr, incoming, enclosing_aliases, violations)
        if item.optional_vars is not None:
            target_aliases = _binding_aliases(_target_value_bindings(item.optional_vars, item.context_expr), incoming)
            incoming = incoming - _target_names(item.optional_vars) | target_aliases
    aliases = _resolve_class_statements(statement.body, incoming, enclosing_aliases, violations, observed_states)
    if observed_states is not None:
        observed_states.append(aliases.copy())
    return aliases


def _resolve_class_statements(
    statements: list[ast.stmt],
    aliases: set[str],
    enclosing_aliases: set[str],
    violations: list[int] | None,
    observed_states: list[set[str]] | None = None,
) -> set[str]:
    for statement in statements:
        if observed_states is not None and statement_may_raise(statement):
            observed_states.append(aliases.copy())
        if isinstance(statement, ast.If):
            aliases = _resolve_class_if(statement, aliases, enclosing_aliases, violations, observed_states)
        elif isinstance(statement, (ast.Try, ast.TryStar)):
            aliases = _resolve_class_try(statement, aliases, enclosing_aliases, violations, observed_states)
        elif isinstance(statement, (ast.For, ast.AsyncFor)):
            aliases = _resolve_class_for(statement, aliases, enclosing_aliases, violations, observed_states)
        elif isinstance(statement, ast.While):
            aliases = _resolve_class_while(statement, aliases, enclosing_aliases, violations, observed_states)
        elif isinstance(statement, (ast.With, ast.AsyncWith)):
            aliases = _resolve_class_with(statement, aliases, enclosing_aliases, violations, observed_states)
        else:
            aliases = _resolve_class_node(statement, aliases, enclosing_aliases, violations)
    return aliases


def _resolve_class_scope(scope: ast.ClassDef, enclosing_aliases: set[str], violations: list[int] | None) -> set[str]:
    return _resolve_class_statements(scope.body, enclosing_aliases.copy(), enclosing_aliases, violations)


def _resolve_scope(scope: ast.AST, enclosing_aliases: set[str], violations: list[int] | None = None) -> set[str]:
    if isinstance(scope, ast.ClassDef):
        return _resolve_class_scope(scope, enclosing_aliases, violations)

    facts = _scope_facts(scope)
    aliases = _resolve_bindings(facts.bindings, enclosing_aliases - facts.local_names)

    if violations is not None:
        _record_non_disclosure_assertions(facts.assertions, aliases, violations)
        for child in facts.children:
            _resolve_scope(child, aliases, violations)
    return aliases


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


_COMPREHENSIONS = (ast.ListComp, ast.SetComp, ast.GeneratorExp, ast.DictComp)


def _assertion_comparisons(test: ast.expr) -> list[ast.Compare]:
    """The assertion's own comparisons, skipping any inside a comprehension.

    A `not in` used as a comprehension filter is a per-element test. The assertion renders the
    comprehension's RESULT - a count, a list - never the element, so it discloses nothing.
    """
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


def test_the_guarded_membership_is_derived_from_what_a_module_handles() -> None:
    """A module that starts holding protected material joins both rules with no edit here."""
    helper_call = "def t():\n    assert_text_free_of(resp.text, [protected])\n"
    placeholder_literal = 'def t():\n    secret = "placeholder-token"\n'
    neither = "def t():\n    assert resp.status_code == 200\n"

    assert _handles_protected_material(ast.parse(helper_call))
    assert _handles_protected_material(ast.parse(placeholder_literal))
    assert not _handles_protected_material(ast.parse(neither))

    every_module = set(_TEST_ROOT.rglob("test_*.py"))
    guarded = set(_guarded_modules())
    assert Path(__file__).resolve() in guarded, "this module holds material and must guard itself"
    assert guarded < every_module, "a derivation of what is held, not a blanket sweep"
    assert len(guarded) > 40, "the derivation must reach the modules that hold material"


def test_non_disclosure_checks_do_not_use_rewritten_assertions() -> None:
    violations = []
    for path in _guarded_modules():
        violations.extend(
            f"{path.relative_to(_TEST_ROOT.parent)}:{line}"
            for line in _non_disclosure_assertion_lines(path.read_text(encoding="utf-8"))
        )
    assert violations == []


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
        if isinstance(part, ast.Attribute) and part.attr in _INSPECTED_ATTRIBUTES:
            if any(isinstance(inner, ast.Name) and inner.id == root for inner in ast.walk(part)):
                return True
    return False


def _renders_root_through_a_surface(node: ast.AST, root: str) -> bool:
    """True when *root* is rendered through a text surface or an explicit str/repr, never bare."""
    for part in ast.walk(node):
        reads_surface = (isinstance(part, ast.Attribute) and part.attr in _INSPECTED_ATTRIBUTES) or (
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
    if isinstance(node, ast.Attribute) and node.attr in _INSPECTED_ATTRIBUTES:
        return [node.value]
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr in _INSPECTED_ATTRIBUTES:
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


def _discloses_protected_value(node: ast.Assert, root: str) -> bool:
    """True when a FAILURE of *node* prints the protected surface whole.

    Scoped to what the syntax decides on its own. A failure message is printed verbatim, and an
    operand that renders the protected value whole prints all of it: the bare name, a text
    surface, an explicit ``str``/``repr``, or a sequence holding one of those.

    The two operator kinds read their operands differently, so they are judged differently. A
    membership operand is the HAYSTACK being searched for the protected value, so narrowing it
    still renders text that came from the protected value: it is judged on the whole chain.
    Every other operator compares a value against an authored one, so narrowing produces a
    different, smaller value: it is judged on the operand's outermost expression. That is why
    ``"device_id" not in record`` renders a boolean while ``record == expected`` renders the
    record, and why ``resp.json()["error"]["code"] == "vault_error"`` renders neither.
    """
    if node.msg is not None and _renders_root(node.msg, root):
        return True
    for part in ast.walk(node.test):
        if not isinstance(part, ast.Compare):
            continue
        operands = (part.left, *part.comparators)
        if any(isinstance(operator, ast.In | ast.NotIn) for operator in part.ops):
            if any(_renders_root_through_a_surface(operand, root) for operand in operands):
                return True
            continue
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
    for path in _guarded_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for scope in ast.walk(tree):
            if not isinstance(scope, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            for lineno, root in _ordering_violations_in(scope):
                violations.append(f"{path.relative_to(_TEST_ROOT.parent)}:{lineno} discloses {root!r}")
    assert violations == []


def _rewritten_check_lines(source: str) -> list[int]:
    """Run the SAME rule ``test_non_disclosure_checks_do_not_use_rewritten_assertions`` runs."""
    return [
        node.lineno
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Assert)
        and any(
            any(isinstance(operator, ast.NotIn) for operator in comparison.ops)
            and any(_reads_an_inspected_surface(value) for value in comparison.comparators)
            for comparison in _assertion_comparisons(node.test)
        )
    ]


def test_the_rewritten_assertion_rule_reads_the_assertion_and_not_its_filters() -> None:
    """A rendered surface discloses whatever the left operand is; a filter renders only a count."""
    named_value = "assert protected not in resp.text"
    marker_in_a_surface = 'assert "dry-run=native" not in str(request.url)'
    rendered_call = "assert secret not in repr(record)"
    comprehension_filter = 'assert len([r for r in requests if "dry-run" not in str(r.url)]) == 1'
    key_membership = 'assert "device_id" not in record'

    assert _rewritten_check_lines(named_value) == [1]
    assert _rewritten_check_lines(marker_in_a_surface) == [1], "the URL carries the device name"
    assert _rewritten_check_lines(rendered_call) == [1]
    assert _rewritten_check_lines(comprehension_filter) == []
    assert _rewritten_check_lines(key_membership) == []


def _ordering_violations(source: str) -> list[int]:
    """Run the SAME rule the test above runs, over one snippet."""
    return [lineno for lineno, _ in _ordering_violations_in(ast.parse(source).body[0])]


def test_the_ordering_rule_reads_the_shapes_that_disclose_and_no_others() -> None:
    """A failure message and a bare operand render the value; a key test renders a boolean."""
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
    assert _ordering_violations(key_membership) == []
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


def test_non_disclosure_aliases_converge_without_cross_scope_contamination() -> None:
    reversed_order = """\
leaked = captured
captured = response.text
assert protected not in leaked
"""
    cross_scope = """\
captured = "authored detail"
def nested():
    captured = response.text
assert protected not in captured
"""

    assert _non_disclosure_assertion_lines(reversed_order) == [3]
    assert _non_disclosure_assertion_lines(cross_scope) == []


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
