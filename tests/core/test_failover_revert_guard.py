# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Every temporary failover flip must restore an address read in its own function."""

from __future__ import annotations

import ast
from pathlib import Path
from textwrap import dedent

import pytest

_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
_COMPREHENSIONS = (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)


def _local_nodes(node):
    yield node
    for child in ast.iter_child_nodes(node):
        if isinstance(child, _SCOPES):
            yield child
        elif isinstance(child, ast.comprehension):
            yield from _local_nodes(child.iter)
            for condition in child.ifs:
                yield from _local_nodes(condition)
        else:
            yield from _local_nodes(child)


def _address_bindings(node, name, *, visible=True, writes_outer=True):
    """Collect writes to the target binding across lexical scopes."""
    if writes_outer and _binds_name(node, name):
        yield node
    if isinstance(node, _COMPREHENSIONS):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.comprehension):
                yield from _address_bindings(child.iter, name, visible=visible, writes_outer=writes_outer)
                for condition in child.ifs:
                    yield from _address_bindings(condition, name, visible=visible, writes_outer=writes_outer)
            else:
                yield from _address_bindings(child, name, visible=visible, writes_outer=writes_outer)
        return
    if isinstance(node, _SCOPES):
        for field, value in ast.iter_fields(node):
            if field == "body":
                continue
            children = value if isinstance(value, list) else [value]
            for child in children:
                if isinstance(child, ast.AST):
                    yield from _address_bindings(child, name, visible=visible, writes_outer=writes_outer)
        body = node.body if isinstance(node.body, list) else [node.body]
        local_nodes = [
            child
            for statement in body
            for child in ([statement] if isinstance(statement, _SCOPES) else _local_nodes(statement))
        ]
        is_global = any(isinstance(child, ast.Global) and name in child.names for child in local_nodes)
        is_nonlocal = any(isinstance(child, ast.Nonlocal) and name in child.names for child in local_nodes)
        local_binding = any(_binds_name(child, name) for child in local_nodes)
        if hasattr(node, "args"):
            arguments = node.args
            parameters = [*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs]
            parameters += [argument for argument in (arguments.vararg, arguments.kwarg) if argument is not None]
            local_binding |= any(argument.arg == name for argument in parameters)
        child_writes_outer = is_global or (is_nonlocal and visible)
        child_visible = child_writes_outer or (visible and not local_binding)
        if isinstance(node, ast.ClassDef):
            # Methods resolve enclosing names without the class namespace.
            child_visible = visible
        for statement in body:
            yield from _address_bindings(statement, name, visible=child_visible, writes_outer=child_writes_outer)
        return
    for child in ast.iter_child_nodes(node):
        yield from _address_bindings(child, name, visible=visible, writes_outer=writes_outer)


def _binds_name(node, name: str) -> bool:
    if isinstance(node, ast.Name):
        return node.id == name and isinstance(node.ctx, (ast.Store, ast.Del))
    if isinstance(node, (ast.MatchAs, ast.MatchStar, ast.ExceptHandler)):
        return node.name == name
    if isinstance(node, ast.MatchMapping):
        return node.rest == name
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return node.name == name
    if isinstance(node, ast.alias):
        return (node.asname or node.name.split(".")[0]) == name
    return False


def _unsafe_reverts(source: str) -> list[int]:
    unsafe = []
    for function in ast.walk(ast.parse(source)):
        if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        nodes = list(_local_nodes(function))
        parents = {id(child): node for node in nodes for child in ast.iter_child_nodes(node)}
        for call in nodes:
            if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Name):
                continue
            if call.func.id != "_revert_address":
                continue
            target = (
                call.args[2]
                if len(call.args) > 2
                else next((keyword.value for keyword in call.keywords if keyword.arg == "address"), None)
            )
            if not isinstance(target, ast.Name):
                unsafe.append(call.lineno)
                continue
            reads = []
            invalid = False
            for node in (binding for statement in function.body for binding in _address_bindings(statement, target.id)):
                assignment = parents.get(id(node))
                if not isinstance(node, ast.Name) or not isinstance(assignment, (ast.Assign, ast.AnnAssign)):
                    invalid = True
                    continue
                value = assignment.value
                if isinstance(value, ast.Constant) and value.value is None:
                    continue
                if (
                    isinstance(value, ast.Await)
                    and isinstance(value.value, ast.Call)
                    and isinstance(value.value.func, ast.Attribute)
                    and value.value.func.attr == "get_address"
                    and assignment.lineno < call.lineno
                ):
                    reads.append(assignment)
                else:
                    invalid = True
            if invalid or len(reads) != 1:
                unsafe.append(call.lineno)
    return unsafe


@pytest.mark.parametrize(
    ("body", "safe"),
    [
        ("before = await client.get_address(name)\nawait _revert_address(client, name, before, 1)", True),
        ("before: str = await client.get_address(name)\nawait _revert_address(address=before)", True),
        ("await _revert_address(client, name, fo.primary_ip, 1)", False),
        ("await _revert_address(client, name, before, 1)\nbefore = await client.get_address(name)", False),
        (
            "before = await client.get_address(name)\nbefore = fo.primary_ip\nawait _revert_address(address=before)",
            False,
        ),
        (
            "before = await client.get_address(name)\nbefore, other = fo.primary_ip, 1\nawait _revert_address(address=before)",
            False,
        ),
        (
            "before = await client.get_address(name)\nif (before := fo.primary_ip): pass\nawait _revert_address(address=before)",
            False,
        ),
        (
            "before = await client.get_address(name)\nfor before in addresses: pass\nawait _revert_address(address=before)",
            False,
        ),
        (
            "async def read():\n    before = await client.get_address(name)\nawait _revert_address(address=before)",
            False,
        ),
        (
            "before = await client.get_address(name)\nasync def restore():\n    await _revert_address(address=before)",
            False,
        ),
        (
            "try:\n    before = await client.get_address(name)\nexcept Exception:\n    before = None\nawait _revert_address(address=before)",
            True,
        ),
    ],
)
def test_revert_guard_fixtures(body, safe):
    source = "async def probe(client, name):\n" + "\n".join("    " + line for line in body.splitlines())
    assert bool(_unsafe_reverts(dedent(source))) is not safe


@pytest.mark.parametrize(
    "binding",
    [
        "match fo.primary_ip:\n    case before: pass",
        "match addresses:\n    case [*before]: pass",
        "match addresses:\n    case {'primary': _, **before}: pass",
        "match addresses:\n    case [before, _]: pass",
        "if (before := fo.primary_ip): pass",
        "for before in addresses: pass",
        "async for before in addresses: pass",
        "with context() as before: pass",
        "async with context() as before: pass",
        "try: pass\nexcept Exception as before: pass",
        "def replace():\n    nonlocal before\n    before = fo.primary_ip\nreplace()",
        "async def replace():\n    nonlocal before\n    before = fo.primary_ip\nawait replace()",
        "def before(): pass",
        "class before: pass",
        "import module as before",
        "from module import address as before",
        "del before",
    ],
)
def test_revert_guard_rejects_rebound_observed_address(binding):
    body = f"before = await client.get_address(name)\n{binding}\nawait _revert_address(address=before)"
    source = "async def probe(client, name):\n" + "\n".join("    " + line for line in body.splitlines())
    assert _unsafe_reverts(source)


def test_failover_reverts_use_observed_addresses():
    root = Path(__file__).resolve().parents[2] / "nso_adapter"
    failures = [
        f"{path.relative_to(root)}:{line}" for path in root.rglob("*.py") for line in _unsafe_reverts(path.read_text())
    ]
    assert failures == []


@pytest.mark.parametrize(
    "binding",
    [
        "[before for before in addresses]",
        "{before for before in addresses}",
        "{before: before for before in addresses}",
        "(before for before in addresses)",
        "def local():\n    before = '198.18.0.1'\nlocal()",
        "async def local():\n    before = '198.18.0.1'\nawait local()",
        "local = lambda before: before",
        "class Local:\n    before = '198.18.0.1'",
        "def local():\n    before = '198.18.0.1'\n    def replace():\n        nonlocal before\n        before = '198.18.0.2'\n    replace()\nlocal()",
    ],
)
def test_revert_guard_accepts_local_address_shadowing(binding):
    body = f"before = await client.get_address(name)\n{binding}\nawait _revert_address(address=before)"
    source = "async def probe(client, name):\n" + "\n".join("    " + line for line in body.splitlines())
    assert _unsafe_reverts(source) == []


@pytest.mark.parametrize(
    "binding",
    [
        "[(before := address) for address in addresses]",
        "[address for address in (before := addresses)]",
        "def local(value=(before := fo.primary_ip)): pass",
        "local = lambda value=(before := fo.primary_ip): value",
        "class Local((before := Base)): pass",
        "def replace():\n    global before\n    before = fo.primary_ip\nreplace()",
        "def local():\n    def replace():\n        nonlocal before\n        before = fo.primary_ip\n    replace()\nlocal()",
    ],
)
def test_revert_guard_rejects_writes_from_child_scopes(binding):
    body = f"before = await client.get_address(name)\n{binding}\nawait _revert_address(address=before)"
    source = "async def probe(client, name):\n" + "\n".join("    " + line for line in body.splitlines())
    assert _unsafe_reverts(source)
