# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""What the secret-discipline assertions must inspect and keep out of diagnostics."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

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
    _TEST_ROOT / "core" / "test_capability.py",
    _TEST_ROOT / "core" / "test_envelope_classification.py",
    _TEST_ROOT / "core" / "test_onboarding.py",
    _TEST_ROOT / "core" / "test_redistribution.py",
    _TEST_ROOT / "core" / "test_refresh_engine_envelope.py",
    _TEST_ROOT / "nso" / "test_apply_send.py",
    _TEST_ROOT / "nso" / "test_device_state_client.py",
    _TEST_ROOT / "nso" / "test_nso_client_methods.py",
    _TEST_ROOT / "nso" / "test_persistent_subscriber.py",
    _TEST_ROOT / "nso" / "test_sse_subscriber.py",
    _TEST_ROOT / "secrets" / "test_local.py",
    _TEST_ROOT / "secrets" / "test_refs.py",
    _TEST_ROOT / "test_main_lifespan.py",
    _TEST_ROOT / "test_secret_discipline.py",
    _TEST_ROOT / "test_vault_provider.py",
)
_INSPECTED_ATTRIBUTES = {"json", "read_failures", "text", "value"}
_INSPECTED_CALLS = {"repr", "str"}
_NON_DISCLOSURE_HELPERS = {"assert_chain_free_of", "assert_records_free_of", "assert_text_free_of"}
#: How this repository writes protected material into a test (see the placeholder convention). A
#: body that names one is handling something protected, whether or not it calls a helper.
_PROTECTED_LITERAL_PREFIX = "placeholder-"


def test_main_lifespan_is_in_the_non_disclosure_registry() -> None:
    assert _TEST_ROOT / "test_main_lifespan.py" in _NON_DISCLOSURE_TESTS


def _reads_an_inspected_surface(node: ast.AST) -> bool:
    return any(
        isinstance(part, ast.Attribute)
        and part.attr in _INSPECTED_ATTRIBUTES
        or isinstance(part, ast.Call)
        and isinstance(part.func, ast.Name)
        and part.func.id in _INSPECTED_CALLS
        for part in ast.walk(node)
    )


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


def test_non_disclosure_checks_do_not_use_rewritten_assertions() -> None:
    violations = []
    for path in _NON_DISCLOSURE_TESTS:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assert):
                continue
            if any(
                any(isinstance(operator, ast.NotIn) for operator in comparison.ops)
                and any(_reads_an_inspected_surface(value) for value in comparison.comparators)
                for comparison in _assertion_comparisons(node.test)
            ):
                violations.append(f"{path.relative_to(_TEST_ROOT.parent)}:{node.lineno}")
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
    for path in _NON_DISCLOSURE_TESTS:
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
