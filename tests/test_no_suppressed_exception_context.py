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
  attaches the caught exception to whatever the helper raised.
* a function DEFINED in a handler runs its decorators and its default expressions THERE,
  and its body nowhere. Called after the handler exits, its raise attaches nothing; called
  inside it, it is the helper case above.

Every self-test below is checked against the interpreter first, so the analyzer is measured
against real ``__context__`` behaviour instead of against a claim about it.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from pathlib import Path

_PACKAGE = Path(__file__).resolve().parents[1] / "nso_adapter"

_FUNCTION_NODES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)


def _definition_time_nodes(node: ast.AST) -> list[ast.AST]:
    """The parts of a function definition the interpreter evaluates where it is WRITTEN.

    Decorators and default expressions run at definition time, so one written in a handler
    runs in the handler. A lambda has defaults but no decorators.
    """
    args = node.args
    defaults = [default for default in (*args.defaults, *args.kw_defaults) if default is not None]
    return [*getattr(node, "decorator_list", ()), *defaults]


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
        yield node
        stack.extend(ast.iter_child_nodes(node))


def _none_aliases(tree: ast.Module) -> set[str]:
    """Every name the module binds to the literal ``None``.

    ``raise X from <alias>`` where the alias IS None behaves exactly like ``from None``:
    the interpreter sets ``__suppress_context__`` and leaves ``__context__`` attached.
    """
    names: set[str] = set()
    for node in ast.walk(tree):
        value = getattr(node, "value", None)
        if not (isinstance(value, ast.Constant) and value.value is None):
            continue
        if isinstance(node, ast.Assign):
            names |= {target.id for target in node.targets if isinstance(target, ast.Name)}
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return names


def _always_raising_helpers(tree: ast.Module, none_aliases: set[str]) -> set[str]:
    """Functions whose CALL always raises AND attaches: the body ends in such a raise.

    Calling one inside a handler is the same `raise X`, written one frame down. The
    interpreter attaches the caught exception to it exactly as it would in the handler,
    and ``from None`` one frame down suppresses just as little — so the helper's raise is
    judged by the SAME cause classification as a raise written in the handler.
    """
    found: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or not node.body:
            continue
        last = node.body[-1]
        if not isinstance(last, ast.Raise) or not _attaches_context(last, none_aliases):
            continue
        if any(isinstance(child, ast.Return) for child in ast.walk(node)):
            continue
        found.add(node.name)
    return found


def _attaches_context(node: ast.Raise, none_aliases: set[str]) -> bool:
    """Whether this raise leaves the caught exception on ``__context__``."""
    if node.exc is None:
        return False  # a bare `raise` re-raises the caught exception on purpose
    cause = node.cause
    if cause is None:
        return True  # the implicit half: the interpreter attaches it itself
    if isinstance(cause, ast.Constant):
        return cause.value is None  # `from None` only SUPPRESSES; the context stays
    return isinstance(cause, ast.Name) and cause.id in none_aliases


def scan_source(source: str, path: str) -> list[str]:
    """Every context-attaching site that RUNS inside an except handler, as ``path:line``."""
    tree = ast.parse(source, filename=path)
    none_aliases = _none_aliases(tree)
    helpers = _always_raising_helpers(tree, none_aliases)
    lines: set[int] = set()
    for handler in ast.walk(tree):
        if not isinstance(handler, ast.ExceptHandler):
            continue
        for node in _executes_in_handler(handler):
            if isinstance(node, ast.Raise) and _attaches_context(node, none_aliases):
                lines.add(node.lineno)
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in helpers:
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
