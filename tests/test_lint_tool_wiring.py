# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Local lint consumers must resolve their declared tools."""

from __future__ import annotations

import ast
import os
import re
import shlex
import subprocess
from pathlib import Path

import pytest
import yaml

from tests._secret_discipline import assert_text_omits

ROOT = Path(__file__).parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
PRE_COMMIT = ROOT / ".pre-commit-config.yaml"
REVIEW_PATTERNS = ROOT / "scripts" / "check-review-patterns"

# Bare program name: subprocess resolves it through the PATH below, not the caller's.
_SCAN_ARGV = ["bash", str(REVIEW_PATTERNS), "scan"]
_RESTRICTED_PATH = "/usr/bin:/bin"
# The PATH alone does not isolate the prerequisite: a host with /usr/bin/opengrep resolves it.
_MISSING_OPENGREP = "opengrep-that-this-test-never-installs"

_REMOTE_ZIZMOR_HOOK = "https://github.com/zizmorcore/zizmor-pre-commit"
_ZIZMOR_UV_PREFIX = ["uv", "run", "--locked", "--native-tls", "--", "zizmor"]
_ZIZMOR_COLLECTIONS = {"workflows", "actions", "dependabot"}
_INTERPRETER_NAMES = {"bash", "env", "python", "python3", "sh", "zsh"}


def _pinned_partial_paths() -> set[str]:
    """Read the pin from the script itself: a second copy here would drift silently."""
    block = re.search(r"^expected_partial_paths = (\{.*?^\})", REVIEW_PATTERNS.read_text(encoding="utf-8"), re.M | re.S)
    assert block is not None, "check-review-patterns no longer declares expected_partial_paths"
    return ast.literal_eval(block.group(1))


_EXPECTED_PARTIAL_PATHS = _pinned_partial_paths()
_PINNED_PARTIAL_PATH = sorted(_EXPECTED_PARTIAL_PATHS)[0]
_UNPINNED_PARTIAL_PATH = "scratch/unpinned-review-pattern.py"
assert _UNPINNED_PARTIAL_PATH not in _EXPECTED_PARTIAL_PATHS


def _write_opengrep_stub(
    tmp_path: Path,
    partial_paths: set[str],
    *,
    results: list[dict[str, object]] | None = None,
    errors: list[dict[str, object]] | None = None,
    exit_code: int = 0,
) -> tuple[Path, Path]:
    payload = {
        "errors": [
            {
                "path": path,
                "type": ["PartialParsing", []],
            }
            for path in sorted(partial_paths)
        ]
        + (errors or []),
        "results": results or [],
    }
    stub = tmp_path / "opengrep-stub"
    invocations = tmp_path / "invocations"
    stub.write_text(
        "#!/usr/bin/env python3\n"
        "import json\n"
        "import sys\n"
        "from pathlib import Path\n"
        f"with Path({str(invocations)!r}).open('a', encoding='utf-8') as stream:\n"
        "    stream.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        f"print(json.dumps({payload!r}))\n"
        f"raise SystemExit({exit_code})\n",
        encoding="utf-8",
    )
    stub.chmod(0o755)
    return stub, invocations


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


def test_review_pattern_hook_resolves_its_interpreter_through_the_restricted_path() -> None:
    """``/usr/bin/bash`` is absent on macOS, and an absolute program ignores the PATH below."""
    assert os.path.dirname(_SCAN_ARGV[0]) == "", "the interpreter must resolve through the supplied PATH"


def test_subprocess_argv_literals_resolve_interpreters_through_path() -> None:
    violations = []
    for source_path in sorted((ROOT / "tests").rglob("*.py")):
        tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
        for call in ast.walk(tree):
            if not (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and isinstance(call.func.value, ast.Name)
                and call.func.value.id == "subprocess"
                and call.func.attr in {"Popen", "check_output", "run"}
            ):
                continue
            argv = (
                call.args[0]
                if call.args
                else next(
                    (keyword.value for keyword in call.keywords if keyword.arg == "args"),
                    None,
                )
            )
            if not isinstance(argv, (ast.List, ast.Tuple)) or not argv.elts:
                continue
            interpreter = argv.elts[0]
            if (
                isinstance(interpreter, ast.Constant)
                and isinstance(interpreter.value, str)
                and interpreter.value.startswith("/")
                and Path(interpreter.value).name in _INTERPRETER_NAMES
            ):
                violations.append(f"{source_path.relative_to(ROOT)}:{call.lineno}")

    assert not violations, "absolute interpreter argv literals:\n" + "\n".join(violations)


def test_review_pattern_hook_explains_its_opengrep_prerequisite() -> None:
    result = subprocess.run(
        _SCAN_ARGV,
        check=False,
        capture_output=True,
        text=True,
        env={"PATH": _RESTRICTED_PATH, "OPENGREP_BIN": _MISSING_OPENGREP},
    )

    assert result.returncode == 127
    assert result.stderr.strip() == "OpenGrep is required. Install it or set OPENGREP_BIN. See README.md."


def test_the_opengrep_prerequisite_holds_where_opengrep_is_on_the_path(tmp_path):
    """The restricted PATH is this host's layout, not a guarantee: some carry /usr/bin/opengrep."""
    planted = tmp_path / "opengrep"
    planted.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    planted.chmod(0o755)

    result = subprocess.run(
        _SCAN_ARGV,
        check=False,
        capture_output=True,
        text=True,
        env={"PATH": f"{tmp_path}:{_RESTRICTED_PATH}", "OPENGREP_BIN": _MISSING_OPENGREP},
    )

    assert result.returncode == 127
    assert result.stderr.strip() == "OpenGrep is required. Install it or set OPENGREP_BIN. See README.md."


@pytest.mark.parametrize(
    ("partial_paths", "changed_path", "heading"),
    [
        (
            _EXPECTED_PARTIAL_PATHS | {"scratch/new-pep695.py"},
            "scratch/new-pep695.py",
            "New partially analysed files:",
        ),
        (
            _EXPECTED_PARTIAL_PATHS - {_PINNED_PARTIAL_PATH},
            _PINNED_PARTIAL_PATH,
            "Expected partially analysed files no longer reported:",
        ),
    ],
)
def test_review_pattern_scan_rejects_partial_parse_drift(
    tmp_path: Path,
    partial_paths: set[str],
    changed_path: str,
    heading: str,
) -> None:
    stub, _invocations = _write_opengrep_stub(tmp_path, partial_paths)

    result = subprocess.run(
        _SCAN_ARGV,
        check=False,
        capture_output=True,
        text=True,
        env=os.environ | {"OPENGREP_BIN": str(stub)},
    )

    assert result.returncode != 0
    assert "kb #1718" in result.stderr
    assert heading in result.stderr
    assert changed_path in result.stderr
    for unchanged_path in _EXPECTED_PARTIAL_PATHS & partial_paths:
        assert_text_omits(result.stderr, [unchanged_path])


def test_review_pattern_scan_accepts_the_pinned_partial_paths(tmp_path: Path) -> None:
    stub, _invocations = _write_opengrep_stub(tmp_path, _EXPECTED_PARTIAL_PATHS)

    result = subprocess.run(
        _SCAN_ARGV,
        check=False,
        capture_output=True,
        text=True,
        env=os.environ | {"OPENGREP_BIN": str(stub)},
    )

    assert result.returncode == 0
    expected = f"OpenGrep partial-parse pin matches exactly ({len(_EXPECTED_PARTIAL_PATHS)} files)."
    assert result.stdout.strip() == expected
    assert not result.stderr


@pytest.mark.parametrize(
    ("target", "partial_paths", "expected_count"),
    [
        (_UNPINNED_PARTIAL_PATH, set(), 0),
        (_PINNED_PARTIAL_PATH, {_PINNED_PARTIAL_PATH}, 1),
    ],
)
def test_review_pattern_targeted_scan_checks_only_pins_in_scope(
    tmp_path: Path,
    target: str,
    partial_paths: set[str],
    expected_count: int,
) -> None:
    stub, invocations = _write_opengrep_stub(tmp_path, partial_paths)

    result = subprocess.run(
        _SCAN_ARGV + [target],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ | {"OPENGREP_BIN": str(stub)},
    )

    assert result.returncode == 0
    noun = "file" if expected_count == 1 else "files"
    assert result.stdout.strip() == f"OpenGrep partial-parse pin matches scan scope ({expected_count} {noun})."
    assert not result.stderr
    assert target in invocations.read_text(encoding="utf-8")


def test_review_pattern_targeted_scan_rejects_a_new_partial_parse(tmp_path: Path) -> None:
    target = _UNPINNED_PARTIAL_PATH
    stub, _invocations = _write_opengrep_stub(tmp_path, {target})

    result = subprocess.run(
        _SCAN_ARGV + [target],
        check=False,
        capture_output=True,
        text=True,
        env=os.environ | {"OPENGREP_BIN": str(stub)},
    )

    assert result.returncode != 0
    assert result.stdout == ""
    assert result.stderr.splitlines() == [
        "OpenGrep partial parsing changed. Inspect kb #1718 and update the pin by hand.",
        "New partially analysed files:",
        f"  {target}",
    ]


def test_review_pattern_scan_renders_findings_from_one_json_scan(tmp_path: Path) -> None:
    message = "Keep this finding text intact: punctuation!"
    stub, invocations = _write_opengrep_stub(
        tmp_path,
        _EXPECTED_PARTIAL_PATHS,
        results=[
            {
                "path": "review-patterns.py",
                "start": {"line": 17},
                "check_id": "nso-test-rule",
                "extra": {"message": message},
            }
        ],
        exit_code=1,
    )

    result = subprocess.run(
        _SCAN_ARGV,
        check=False,
        capture_output=True,
        text=True,
        env=os.environ | {"OPENGREP_BIN": str(stub)},
    )

    assert result.returncode == 1
    assert f"review-patterns.py:17  nso-test-rule  {message}" in result.stdout
    calls = invocations.read_text(encoding="utf-8").splitlines()
    assert len(calls) == 1
    assert "--json" in calls[0]


def test_the_scan_fails_when_opengrep_drops_a_malformed_rule(tmp_path: Path):
    """A rule OpenGrep cannot parse is reported here and nowhere else.

    OpenGrep drops the rule, scans with the rest and still exits 0, so before this the gate
    went green with the guard silently disabled. Measured for real: a `pattern-not-inside`
    holding a bare `except` clause took `nso-api-error-raw-exception-renderer` out of a scan
    that reported success.
    """
    stub, _invocations = _write_opengrep_stub(
        tmp_path,
        _EXPECTED_PARTIAL_PATHS,
        errors=[{"type": "Rule parse error", "message": "Invalid pattern for Python"}],
    )

    result = subprocess.run(
        _SCAN_ARGV,
        check=False,
        capture_output=True,
        text=True,
        env=os.environ | {"OPENGREP_BIN": str(stub)},
    )

    assert result.returncode == 1, "a dropped rule must fail the scan, not pass it"
    assert "Rule parse error" in result.stderr
