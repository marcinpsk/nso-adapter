# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Appendix S §3.2: exactly ONE module performs terminal job-status writes.

Sixteen physical terminal writes across six modules were the failure mode: a
seventeenth added later that forgets the CAS — and, from S2, the settlement sequence —
produces a terminal job that is permanently invisible to settlement, silently. A review
rule cannot hold that line, so this build failure does.

Outside ``nso_adapter/core/claim.py`` no module may assign a terminal ``JobStatus`` to a
``.status`` attribute, pass one as a ``status=`` keyword, or write ``settle_seq`` at all.
Route the write through ``core.claim.terminalize`` (or ``terminalize_queued_bulk``).
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

_PACKAGE = Path(__file__).resolve().parents[1] / "nso_adapter"
# The single owner. Every physical terminal write lives here, with its CAS.
_OWNER = _PACKAGE / "core" / "claim.py"

_TERMINAL_MEMBERS = frozenset({"succeeded", "failed"})
_SEQUENCE_ATTR = "settle_seq"
# The sanctioned route. Naming one of these IS the fix, so a ``status=`` keyword handed to
# them is not a bypass. The set is explicit: a new helper that writes a terminal status
# without going through ``core.claim`` cannot be exempted by accident.
_SANCTIONED_WRITERS = frozenset({"terminalize", "terminalize_queued_bulk", "terminalize_running"})


@dataclass(frozen=True)
class Violation:
    path: str
    line: int
    what: str

    def __str__(self) -> str:  # pragma: no cover - message formatting only
        return f"{self.path}:{self.line}: {self.what}"


def _enum_names(tree: ast.Module) -> frozenset[str]:
    """Every local binding of the ``JobStatus`` enum in this module.

    ``from ... import JobStatus as _JS`` would otherwise walk straight past the guard,
    which is the one evasion an accidental regression can reach without meaning to.
    """
    names = {"JobStatus"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            names.update(alias.asname or alias.name for alias in node.names if alias.name == "JobStatus")
    return frozenset(names)


def _is_job_status_enum(node: ast.expr, names: frozenset[str]) -> bool:
    """The enum by any local binding, or through any module alias (``models.JobStatus``)."""
    if isinstance(node, ast.Name):
        return node.id in names
    if isinstance(node, ast.Attribute):
        return node.attr in names
    return False


def _is_terminal_status(node: ast.expr | None, names: frozenset[str]) -> bool:
    return isinstance(node, ast.Attribute) and node.attr in _TERMINAL_MEMBERS and _is_job_status_enum(node.value, names)


def _target_attr(node: ast.expr) -> str | None:
    return node.attr if isinstance(node, ast.Attribute) else None


def _subscript_key(node: ast.expr) -> str | None:
    """The column a ``values["status"] = ...`` assignment writes into a bound mapping."""
    if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
        return node.slice.value if isinstance(node.slice.value, str) else None
    return None


def _called_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


_SCOPE_NODES = (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)


def _nodes_in_scope(scope: ast.AST) -> list[ast.AST]:
    """Return nodes whose nearest lexical scope is *scope*."""
    nodes: list[ast.AST] = []
    pending = list(ast.iter_child_nodes(scope))
    while pending:
        node = pending.pop()
        if isinstance(node, _SCOPE_NODES):
            continue
        nodes.append(node)
        pending.extend(ast.iter_child_nodes(node))
    return nodes


def _values_mapping_names(nodes: list[ast.AST]) -> frozenset[str]:
    """Return bound mappings passed to SQLAlchemy's ``values`` writer."""
    names: set[str] = set()
    for node in nodes:
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "values"):
            continue
        names.update(arg.id for arg in node.args if isinstance(arg, ast.Name))
        names.update(
            keyword.value.id for keyword in node.keywords if keyword.arg is None and isinstance(keyword.value, ast.Name)
        )
    return frozenset(names)


def scan_source(source: str, path: str) -> list[Violation]:
    """Every direct terminal-status or settle_seq write in *source*."""
    found: list[Violation] = []
    tree = ast.parse(source)
    names = _enum_names(tree)
    for scope in (node for node in ast.walk(tree) if isinstance(node, _SCOPE_NODES)):
        nodes = _nodes_in_scope(scope)
        mapping_names = _values_mapping_names(nodes)
        for node in nodes:
            targets: list[ast.expr] = []
            if isinstance(node, ast.Assign):
                targets = list(node.targets)
            elif isinstance(node, ast.AnnAssign | ast.AugAssign):
                targets = [node.target]
            for target in targets:
                attr = _target_attr(target)
                if (
                    attr is None
                    and isinstance(target, ast.Subscript)
                    and isinstance(target.value, ast.Name)
                    and target.value.id in mapping_names
                ):
                    attr = _subscript_key(target)
                if attr == _SEQUENCE_ATTR:
                    found.append(Violation(path, node.lineno, "writes Job.settle_seq directly"))
                elif attr == "status" and _is_terminal_status(getattr(node, "value", None), names):
                    found.append(Violation(path, node.lineno, "assigns a terminal JobStatus directly"))
            # A mapping bound to a name is a write only if that same binding reaches
            # SQLAlchemy's ``values`` method in this lexical scope.
            if (
                isinstance(node, ast.Assign | ast.AnnAssign)
                and isinstance(node.value, ast.Dict)
                and any(isinstance(target, ast.Name) and target.id in mapping_names for target in targets)
            ):
                found.extend(_dict_literal_violations(node.value, names, path, node.lineno))

            if isinstance(node, ast.Call):
                found.extend(_call_violations(node, names, path))
    return found


def _mapping_key_name(key: ast.expr | None) -> str | None:
    """The column a mapping key writes: ``"status"`` or the column object ``Job.status``."""
    if isinstance(key, ast.Constant):
        return key.value if isinstance(key.value, str) else None
    return _target_attr(key) if key is not None else None


def _dict_literal_violations(arg: ast.Dict, names: frozenset[str], path: str, lineno: int) -> list[Violation]:
    found: list[Violation] = []
    for key, value in zip(arg.keys, arg.values):
        name = _mapping_key_name(key)
        if name == _SEQUENCE_ATTR:
            found.append(Violation(path, lineno, "writes Job.settle_seq directly"))
        elif name == "status" and _is_terminal_status(value, names):
            found.append(Violation(path, lineno, "assigns a terminal JobStatus directly"))
    return found


def _mapping_literal(node: ast.expr) -> ast.Dict | None:
    """The mapping an argument carries, through a named expression (``vals := {...}``) if used."""
    if isinstance(node, ast.Dict):
        return node
    if isinstance(node, ast.NamedExpr) and isinstance(node.value, ast.Dict):
        return node.value
    return None


def _call_violations(node: ast.Call, names: frozenset[str], path: str) -> list[Violation]:
    """Every write a call carries: ``status=``/``settle_seq=`` keywords and mapping arguments."""
    if _called_name(node) in _SANCTIONED_WRITERS:
        return []
    found: list[Violation] = []
    for kw in node.keywords:
        if kw.arg is None:
            # ``**{...}`` carries the same write with no keyword name to read.
            if mapping := _mapping_literal(kw.value):
                found.extend(_dict_literal_violations(mapping, names, path, node.lineno))
        elif kw.arg == _SEQUENCE_ATTR:
            found.append(Violation(path, node.lineno, "writes Job.settle_seq directly"))
        elif kw.arg == "status" and _is_terminal_status(kw.value, names):
            found.append(Violation(path, node.lineno, "assigns a terminal JobStatus directly"))
    # SQLAlchemy's .values() equally accepts a mapping: .values({"status": ...}).
    for arg in node.args:
        if mapping := _mapping_literal(arg):
            found.extend(_dict_literal_violations(mapping, names, path, node.lineno))
    return found


def scan_tree() -> list[Violation]:
    """Every violation in the shipped package, excluding the one owning module."""
    found: list[Violation] = []
    for path in sorted(_PACKAGE.rglob("*.py")):
        if path == _OWNER:
            continue
        found.extend(scan_source(path.read_text(encoding="utf-8"), str(path.relative_to(_PACKAGE.parent))))
    return found


# ── the guard ────────────────────────────────────────────────────────────────


def test_only_claim_module_writes_terminal_status():
    """S1.6 — a module outside ``core/claim.py`` writing a terminal status fails the build."""
    bad = scan_tree()
    assert not bad, (
        "Direct terminal job-status / settle_seq write(s) outside nso_adapter/core/claim.py:\n"
        + "\n".join(f"  {v}" for v in bad)
        + "\n\nRoute the write through core.claim.terminalize (or terminalize_queued_bulk), "
        "which owns the CAS and the settlement sequence."
    )


# ── analyzer self-tests (real AST parsing) ───────────────────────────────────


def test_flags_a_direct_terminal_assignment():
    hits = scan_source("def f(job):\n    job.status = JobStatus.succeeded\n", "t.py")
    assert [(h.line, h.what) for h in hits] == [(2, "assigns a terminal JobStatus directly")]


def test_flags_a_terminal_status_keyword():
    hits = scan_source("def f(u):\n    u.values(status=JobStatus.failed)\n", "t.py")
    assert len(hits) == 1


def test_flags_an_aliased_enum_reference():
    hits = scan_source("def f(job):\n    job.status = models.JobStatus.failed\n", "t.py")
    assert len(hits) == 1


def test_flags_an_import_aliased_enum():
    """``from ... import JobStatus as _JS`` is the one evasion a regression reaches by accident."""
    src = "from nso_adapter.store.models import JobStatus as _JS\n\ndef f(u):\n    u.values(status=_JS.failed)\n"
    assert len(scan_source(src, "t.py")) == 1


def test_flags_any_settle_seq_write():
    hits = scan_source("def f(job, u):\n    job.settle_seq = 5\n    u.values(settle_seq=6)\n", "t.py")
    assert len(hits) == 2


def test_flags_a_dict_literal_values_mapping():
    """SQLAlchemy also accepts ``.values({...})`` — as reachable by accident as a keyword."""
    src = 'def f(u):\n    u.values({"status": JobStatus.failed, "settle_seq": 3})\n'
    assert len(scan_source(src, "t.py")) == 2


def test_flags_a_dict_literal_bound_to_a_name_first():
    """``vals = {...}`` handed to ``.values(vals)`` is the same physical write, one line up."""
    src = 'def f(u):\n    vals = {"status": JobStatus.failed, "settle_seq": 3}\n    u.values(vals)\n'
    hits = scan_source(src, "t.py")
    assert [(h.line, h.what) for h in hits] == [
        (2, "assigns a terminal JobStatus directly"),
        (2, "writes Job.settle_seq directly"),
    ]


def test_flags_a_subscript_write_into_a_bound_mapping():
    """``vals["settle_seq"] = 3`` reaches the row through the mapping built one line up."""
    src = 'def f(u):\n    vals = {"job_type": 1}\n    vals["settle_seq"] = 3\n    u.values(vals)\n'
    assert [(h.line, h.what) for h in scan_source(src, "t.py")] == [(3, "writes Job.settle_seq directly")]


def test_ignores_a_returned_read_serialization():
    """A bound response mapping reads job fields. It does not write them."""
    src = (
        "def f(j, update):\n"
        '    write_values = {"status": JobStatus.failed, "settle_seq": 3}\n'
        '    response = {"status": JobStatus.failed, "settle_seq": j.settle_seq}\n'
        "    update.values(write_values)\n"
        "    return response\n"
    )
    assert [(hit.line, hit.what) for hit in scan_source(src, "t.py")] == [
        (2, "assigns a terminal JobStatus directly"),
        (2, "writes Job.settle_seq directly"),
    ]


def test_flags_a_double_star_unpacked_dict_literal():
    """``.values(**{...})`` reaches the same write with no keyword the loop can see."""
    src = 'def f(u):\n    u.values(**{"status": JobStatus.failed})\n'
    assert len(scan_source(src, "t.py")) == 1


def test_flags_a_double_star_unpacked_settle_seq():
    src = 'def f(u):\n    u.values(**{"settle_seq": 6})\n'
    assert len(scan_source(src, "t.py")) == 1


def test_flags_a_double_star_unpacked_bound_mapping():
    """``vals`` bound before ``.values(**vals)`` is still a physical row write."""
    src = 'def f(u):\n    vals = {"status": JobStatus.failed, "settle_seq": 3}\n    u.values(**vals)\n'
    hits = scan_source(src, "t.py")
    assert [(hit.line, hit.what) for hit in hits] == [
        (2, "assigns a terminal JobStatus directly"),
        (2, "writes Job.settle_seq directly"),
    ]


def test_flags_a_walrus_bound_values_mapping():
    """``.values(vals := {...})`` binds and writes the row in one expression."""
    src = 'def f(u):\n    u.values(vals := {"status": JobStatus.failed, "settle_seq": 3})\n'
    assert len(scan_source(src, "t.py")) == 2


def test_flags_a_double_star_unpacked_walrus_mapping():
    """``.values(**(vals := {...}))`` hides the same mapping behind a named expression."""
    src = 'def f(u):\n    u.values(**(vals := {"settle_seq": 3}))\n'
    assert len(scan_source(src, "t.py")) == 1


def test_flags_column_attribute_keys_in_a_values_mapping():
    """SQLAlchemy accepts the column object as the key: ``.values({Job.status: ...})``."""
    src = "def f(u):\n    u.values({Job.status: JobStatus.failed, Job.settle_seq: 3})\n"
    assert len(scan_source(src, "t.py")) == 2


def test_ignores_a_non_literal_double_star_mapping():
    """A name after ``**`` is not statically resolvable, so it stays out of scope."""
    assert scan_source("def f(u, kwargs):\n    u.values(**kwargs)\n", "t.py") == []


def test_allows_the_sanctioned_writer_but_not_a_lookalike():
    """Naming ``terminalize`` IS the fix; a bare ``values()`` next to it still fails."""
    ok = "async def f(db):\n    await terminalize(db, 1, status=JobStatus.failed, expect=JobStatus.running)\n"
    assert scan_source(ok, "t.py") == []
    bad = "def f(u):\n    u.update().values(status=JobStatus.failed)\n"
    assert len(scan_source(bad, "t.py")) == 1


def test_allows_non_terminal_transitions_and_reads():
    src = (
        "def f(job, q):\n"
        "    job.status = JobStatus.running\n"
        "    job.status = JobStatus.queued\n"
        "    return q.where(Job.status == JobStatus.failed)\n"
    )
    assert scan_source(src, "t.py") == []
