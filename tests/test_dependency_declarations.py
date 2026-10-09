# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Direct runtime imports are declared with the version floors the code needs."""

from __future__ import annotations

import tomllib
from pathlib import Path

from packaging.requirements import Requirement

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _declared(name: str) -> Requirement:
    project = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text())["project"]
    (requirement,) = [Requirement(spec) for spec in project["dependencies"] if Requirement(spec).name == name]
    return requirement


def test_pydantic_floor_keeps_exclude_if_fields_optional_in_the_schema() -> None:
    # Field(exclude_if=...) arrived in 2.12.0; 2.12.4 drops such fields from the JSON Schema "required" list.
    specifier = _declared("pydantic").specifier
    assert not specifier.contains("2.12.3")
    assert specifier.contains("2.12.4")
