# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""What the secret-discipline assertions must inspect and keep out of diagnostics."""

from __future__ import annotations

import ast
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
_NON_DISCLOSURE_TESTS = (
    _TEST_ROOT / "api" / "test_actions_direct.py",
    _TEST_ROOT / "api" / "test_api.py",
    _TEST_ROOT / "api" / "test_api_capability.py",
    _TEST_ROOT / "api" / "test_api_lag_config.py",
    _TEST_ROOT / "api" / "test_api_onboarding.py",
    _TEST_ROOT / "api" / "test_api_provision_async.py",
    _TEST_ROOT / "api" / "test_api_secrets.py",
    _TEST_ROOT / "api" / "test_api_snmp_intent.py",
    _TEST_ROOT / "api" / "test_api_vlan.py",
    _TEST_ROOT / "core" / "test_action_apply_promotion.py",
    _TEST_ROOT / "core" / "test_apply_error_secrets.py",
    _TEST_ROOT / "core" / "test_envelope_classification.py",
    _TEST_ROOT / "core" / "test_onboarding.py",
    _TEST_ROOT / "core" / "test_redistribution.py",
    _TEST_ROOT / "core" / "test_refresh_engine_envelope.py",
    _TEST_ROOT / "core" / "test_vlan.py",
    _TEST_ROOT / "nso" / "test_device_state_client.py",
    _TEST_ROOT / "test_secret_discipline.py",
    _TEST_ROOT / "test_vault_provider.py",
)
_INSPECTED_ATTRIBUTES = {"json", "read_failures", "text", "value"}


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
        if isinstance(node.func, ast.Name) and node.func.id in {"repr", "str"}:
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
        comparisons = [part for part in ast.walk(node.test) if isinstance(part, ast.Compare)]
        if any(
            any(isinstance(operator, ast.NotIn) for operator in comparison.ops)
            and any(_reads_an_inspected_surface(value, aliases) for value in comparison.comparators)
            for comparison in comparisons
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


def test_non_disclosure_checks_do_not_use_rewritten_assertions() -> None:
    violations = []
    for path in _NON_DISCLOSURE_TESTS:
        violations.extend(
            f"{path.relative_to(_TEST_ROOT.parent)}:{line}"
            for line in _non_disclosure_assertion_lines(path.read_text(encoding="utf-8"))
        )
    assert violations == []


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
