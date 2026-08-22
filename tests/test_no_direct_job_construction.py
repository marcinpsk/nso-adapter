# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Only ``core.jobs`` constructs or inserts Job rows."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

_PACKAGE = Path(__file__).resolve().parents[1] / "nso_adapter"
_OWNER = _PACKAGE / "core" / "jobs.py"


@dataclass(frozen=True)
class Violation:
    path: str
    line: int
    what: str

    def __str__(self) -> str:  # pragma: no cover - message formatting only
        return f"{self.path}:{self.line}: {self.what}"


def _job_names(tree: ast.Module) -> frozenset[str]:
    names = {"Job"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            names.update(alias.asname or alias.name for alias in node.names if alias.name == "Job")
    return frozenset(names)


def _insert_names(tree: ast.Module) -> frozenset[str]:
    names = {"insert", "pg_insert"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            names.update(alias.asname or alias.name for alias in node.names if alias.name == "insert")
    return frozenset(names)


def _is_job(node: ast.expr, names: frozenset[str]) -> bool:
    return (isinstance(node, ast.Name) and node.id in names) or (isinstance(node, ast.Attribute) and node.attr == "Job")


def _called_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _inserts_job(node: ast.Call, job_names: frozenset[str], insert_names: frozenset[str]) -> bool:
    if _called_name(node) in insert_names and node.args and _is_job(node.args[0], job_names):
        return True
    if not (isinstance(node.func, ast.Attribute) and node.func.attr == "insert"):
        return False
    table = node.func.value
    return isinstance(table, ast.Attribute) and table.attr == "__table__" and _is_job(table.value, job_names)


def scan_source(source: str, path: str) -> list[Violation]:
    """Return direct Job construction and insertion in *source*."""
    tree = ast.parse(source)
    job_names = _job_names(tree)
    insert_names = _insert_names(tree)
    found: list[Violation] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if _is_job(node.func, job_names):
            found.append(Violation(path, node.lineno, "constructs Job directly"))
        elif _inserts_job(node, job_names, insert_names):
            found.append(Violation(path, node.lineno, "inserts Job directly"))
    return found


def scan_tree() -> list[Violation]:
    """Return violations in the shipped package, excluding the owner module."""
    found: list[Violation] = []
    for path in sorted(_PACKAGE.rglob("*.py")):
        if path == _OWNER:
            continue
        found.extend(scan_source(path.read_text(encoding="utf-8"), str(path.relative_to(_PACKAGE.parent))))
    return found


def test_only_jobs_module_constructs_jobs():
    bad = scan_tree()
    assert not bad, (
        "Direct Job construction or insertion outside nso_adapter/core/jobs.py:\n"
        + "\n".join(f"  {violation}" for violation in bad)
        + "\n\nUse admit_coalescible_job, create_dedicated_job, or enqueue_provision_job."
    )


def test_flags_direct_and_aliased_construction():
    source = "from nso_adapter.store.models import Job as StoredJob\nJob()\nStoredJob()\nmodels.Job()\n"
    assert [hit.what for hit in scan_source(source, "t.py")] == ["constructs Job directly"] * 3


def test_flags_sqlalchemy_insert_forms():
    source = (
        "from sqlalchemy import insert as sql_insert\nsql_insert(Job)\nsa.insert(models.Job)\nJob.__table__.insert()\n"
    )
    assert [hit.what for hit in scan_source(source, "t.py")] == ["inserts Job directly"] * 3


def test_allows_job_reads():
    assert scan_source("select(Job)\nawait db.get(Job, 1)\n", "t.py") == []
