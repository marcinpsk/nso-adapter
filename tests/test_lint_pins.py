# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The commit hook and CI must run compatible zizmor checks."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
PRE_COMMIT = ROOT / ".pre-commit-config.yaml"
PYPROJECT = ROOT / "pyproject.toml"

_HOOK_FILES_BY_VERSION = {
    "1.29.0": re.compile(r"(?:\.github/(?:workflows/.*|dependabot\.ya?ml)|action\.ya?ml)$"),
}


def _declared_zizmor_version() -> str:
    groups = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["dependency-groups"]["dev"]
    [declared] = [entry for entry in groups if entry.startswith("zizmor")]
    found = re.fullmatch(r"zizmor==(\S+)", declared)
    assert found, f"the dev group declares an unpinned zizmor: {declared!r}"
    return found.group(1)


def _pre_commit_zizmor_version() -> str:
    found = re.search(
        r"repo: https://github\.com/zizmorcore/zizmor-pre-commit\s*\n\s*rev: v(\S+)",
        PRE_COMMIT.read_text(encoding="utf-8"),
    )
    assert found, "the zizmor pre-commit hook has no rev"
    return found.group(1)


def _ci_zizmor_scope() -> str:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    commands = [
        step["run"].strip()
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        if isinstance(step.get("run"), str) and re.search(r"(?<![\w-])zizmor(?![\w-])", step["run"])
    ]
    assert len(commands) == 1, f"expected one zizmor CI invocation, found: {commands}"
    found = re.fullmatch(r"uv run --native-tls -- zizmor (\S+)", commands[0])
    assert found, f"CI does not run the dependency-group zizmor over one explicit scope: {commands[0]!r}"
    return found.group(1)


def test_zizmor_pin_and_scope_match_pre_commit():
    versions = {
        "pyproject.toml": _declared_zizmor_version(),
        ".pre-commit-config.yaml": _pre_commit_zizmor_version(),
    }
    assert len(set(versions.values())) == 1, f"zizmor versions have drifted apart: {versions}"
    version = versions["pyproject.toml"]
    hook_pattern = _HOOK_FILES_BY_VERSION.get(version)
    assert hook_pattern is not None, f"the upstream files pattern is not recorded for zizmor {version}"

    scope = (ROOT / _ci_zizmor_scope()).resolve()
    assert scope.is_dir() and scope.is_relative_to(ROOT.resolve()), f"CI zizmor scope is invalid: {scope}"
    hook_files = [
        path for path in ROOT.rglob("*") if path.is_file() and hook_pattern.fullmatch(path.relative_to(ROOT).as_posix())
    ]
    assert hook_files, "the zizmor hook selects no repository files"
    uncovered = [path.relative_to(ROOT).as_posix() for path in hook_files if not path.resolve().is_relative_to(scope)]
    assert not uncovered, f"CI zizmor scope does not cover hook files: {uncovered}"
