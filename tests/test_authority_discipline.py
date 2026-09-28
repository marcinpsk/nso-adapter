# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Keep projection authority writes explicit for the value and owner guards."""

import ast
from pathlib import Path

import pytest


def unpacked_authority_writes(source):
    authority = {"authorized_document", "authorized_revision", "applied_revision"}
    return sorted(
        {
            part.lineno
            for target in ast.walk(ast.parse(source))
            if isinstance(target, (ast.Tuple, ast.List)) and isinstance(target.ctx, ast.Store)
            for part in ast.walk(target)
            if isinstance(part, ast.Attribute) and isinstance(part.ctx, ast.Store) and part.attr in authority
        }
    )


@pytest.mark.parametrize(
    "assignment",
    [
        "row.authorized_revision, row.applied_revision = values",
        "row.authorized_revision, row.applied_revision = 7, 8",
        "[row.authorized_document, other] = values",
        "(other, [*row.authorized_document]) = values",
        "(other, (more, row.applied_revision)) = values",
        "target = (row.authorized_revision, other) = values",
        "(row.authorized_revision,) = values",
    ],
)
@pytest.mark.parametrize("owner", ["rogue", "deauthorize_for_cutover", "create_generation"])
def test_unpacking_authority_is_rejected_for_every_owner(assignment, owner):
    assert unpacked_authority_writes(f"async def {owner}(row, values):\n    {assignment}\n") == [2]


@pytest.mark.parametrize(
    "statement",
    [
        "for row.authorized_revision, other in values: pass",
        "async for [other, row.applied_revision] in values: pass",
        "for (other, [*row.authorized_document]) in values: pass",
        "[other for row.authorized_revision, other in values]",
        "{other for row.authorized_revision, other in values}",
        "{other: 0 for row.authorized_revision, other in values}",
        "(other for row.authorized_revision, other in values)",
        "[other async for row.authorized_revision, other in values]",
        "with manager() as (row.authorized_revision, other): pass",
        "async with manager() as [row.authorized_document, other]: pass",
    ],
)
def test_unpacking_authority_is_rejected_in_every_binding_form(statement):
    assert unpacked_authority_writes(f"async def consumer():\n    {statement}\n") == [2]


@pytest.mark.parametrize(
    "source",
    [
        "row.authorized_revision = 0",
        "row.authorized_document = sql_null()",
        "row.normal, other = row.authorized_revision, row.applied_revision",
        "items[row.authorized_revision], other = values",
        "items[(row.authorized_revision, key)], other = values",
        "row.authorized_document.normal, other = values",
        "for items[row.authorized_revision], other in values: pass",
        "[row.authorized_revision for normal, other in values]",
        "with manager(row.authorized_revision) as (normal, other): pass",
    ],
)
def test_explicit_writes_and_reads_are_left_to_existing_guards(source):
    assert unpacked_authority_writes(source) == []


def test_production_authority_writes_use_explicit_targets():
    root = Path(__file__).resolve().parents[1]
    violations = [
        f"{path.relative_to(root)}:{line}"
        for path in sorted((root / "nso_adapter").rglob("*.py"))
        for line in unpacked_authority_writes(path.read_text())
    ]
    assert not violations, "Use explicit authority assignments: " + ", ".join(violations)
