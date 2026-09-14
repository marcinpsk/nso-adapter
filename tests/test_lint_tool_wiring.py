# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Zizmor consumers must execute the locked uv dependency."""

from __future__ import annotations

import re
import shlex
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
PRE_COMMIT = ROOT / ".pre-commit-config.yaml"

_REMOTE_ZIZMOR_HOOK = "https://github.com/zizmorcore/zizmor-pre-commit"
_ZIZMOR_UV_PREFIX = ["uv", "run", "--locked", "--native-tls", "--", "zizmor"]
_ZIZMOR_COLLECTIONS = {"workflows", "actions", "dependabot"}


def _ci_zizmor_command() -> list[str]:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    commands = [
        shlex.split(step["run"])
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        if isinstance(step.get("run"), str) and re.search(r"(?<![\w-])zizmor(?![\w-])", step["run"])
    ]
    assert len(commands) == 1, f"expected one Zizmor CI invocation, found: {commands}"
    return commands[0]


def test_zizmor_consumers_share_locked_uv_dependency():
    config = yaml.safe_load(PRE_COMMIT.read_text(encoding="utf-8"))
    assert all(repository["repo"] != _REMOTE_ZIZMOR_HOOK for repository in config["repos"])

    hooks = [
        hook
        for repository in config["repos"]
        if repository["repo"] == "local"
        for hook in repository["hooks"]
        if hook["id"] == "zizmor"
    ]
    assert len(hooks) == 1, f"expected one local Zizmor hook, found: {hooks}"
    hook = hooks[0]
    assert shlex.split(hook["entry"]) == _ZIZMOR_UV_PREFIX
    assert hook["language"] == "system"
    assert hook["types"] == ["yaml"]
    assert hook["pass_filenames"] is True
    assert hook["require_serial"] is True
    assert hook["args"] == ["--no-progress"]

    hook_files = re.compile(hook["files"])
    for path in (
        ".github/workflows/ci.yml",
        ".github/dependabot.yml",
        "action.yml",
        "action.yaml",
    ):
        assert hook_files.search(path), f"the Zizmor hook omits {path}"
    assert hook_files.search("tools/release/action.yml")
    assert not hook_files.search("deployment.yaml")

    ci_command = _ci_zizmor_command()
    assert ci_command[: len(_ZIZMOR_UV_PREFIX)] == _ZIZMOR_UV_PREFIX
    assert "--no-progress" in ci_command
    assert ci_command[-1] == "."
    collections = {token.removeprefix("--collect=") for token in ci_command if token.startswith("--collect=")}
    assert collections == _ZIZMOR_COLLECTIONS
