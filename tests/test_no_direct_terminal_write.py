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


def _called_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def scan_source(source: str, path: str) -> list[Violation]:
    """Every direct terminal-status or settle_seq write in *source*."""
    found: list[Violation] = []
    tree = ast.parse(source)
    names = _enum_names(tree)
    for node in ast.walk(tree):
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
        elif isinstance(node, ast.AnnAssign | ast.AugAssign):
            targets = [node.target]
        for target in targets:
            attr = _target_attr(target)
            if attr == _SEQUENCE_ATTR:
                found.append(Violation(path, node.lineno, "writes Job.settle_seq directly"))
            elif attr == "status" and _is_terminal_status(getattr(node, "value", None), names):
                found.append(Violation(path, node.lineno, "assigns a terminal JobStatus directly"))

        if isinstance(node, ast.Call) and _called_name(node) not in _SANCTIONED_WRITERS:
            for kw in node.keywords:
                if kw.arg == _SEQUENCE_ATTR:
                    found.append(Violation(path, node.lineno, "writes Job.settle_seq directly"))
                elif kw.arg == "status" and _is_terminal_status(kw.value, names):
                    found.append(Violation(path, node.lineno, "assigns a terminal JobStatus directly"))
            # SQLAlchemy's .values() equally accepts a mapping: .values({"status": ...}).
            for arg in node.args:
                if isinstance(arg, ast.Dict):
                    found.extend(_dict_literal_violations(arg, names, path, node.lineno))
    return found


def _dict_literal_violations(arg: ast.Dict, names: set[str], path: str, lineno: int) -> list[Violation]:
    found: list[Violation] = []
    for key, value in zip(arg.keys, arg.values):
        if not isinstance(key, ast.Constant):
            continue
        if key.value == _SEQUENCE_ATTR:
            found.append(Violation(path, lineno, "writes Job.settle_seq directly"))
        elif key.value == "status" and _is_terminal_status(value, names):
            found.append(Violation(path, lineno, "assigns a terminal JobStatus directly"))
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
