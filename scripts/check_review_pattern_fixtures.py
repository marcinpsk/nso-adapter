#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Check path-scoped OpenGrep fixtures against the JSON scan report."""

import json
import re
import sys
from pathlib import Path

import yaml

ANNOTATION = re.compile(r"#\s*(ruleid|ok|todoruleid|todook)\s*:\s*(nso-[\w-]+(?:\s*,\s*nso-[\w-]+)*)\s*$")
ANNOTATION_LIKE = re.compile(r"#\s*(?:[\w]*ruleid|[\w]*ok)\b", re.IGNORECASE)
AUTHORITY = ("nso-authority", "nso-generation")


def _unexpected_authority_findings(found: dict, relative: str, annotated_lines: set[int]) -> list[str]:
    failures = []
    for (found_path, line), rules in sorted(found.items()):
        if found_path != relative or line in annotated_lines:
            continue
        unexpected = {rule for rule in rules if rule.startswith(AUTHORITY)}
        if unexpected:
            failures.append(f"{relative}:{line}: unexpected {sorted(unexpected)}")
    return failures


def check(root: Path, report: dict, rule_ids: set[str]) -> tuple[int, list[str]]:
    if report.get("errors"):
        return 0, [f"OpenGrep authority fixture errors: {report['errors']}"]
    found = {}
    for result in report["results"]:
        key = (result["path"], result["start"]["line"])
        found.setdefault(key, set()).add(result["check_id"].split(".")[-1])
    failures = []
    checked = 0
    red_rules = set()
    paths = sorted((root / "nso_adapter").rglob("*.py"))
    if not paths:
        failures.append("no path-scoped authority fixtures")
    for path in paths:
        lines = path.read_text(encoding="utf-8").splitlines()
        relative = str(path.relative_to(root))
        file_checked = 0
        annotated_lines = set()
        for line_number, line in enumerate(lines, 1):
            if not ANNOTATION_LIKE.search(line):
                continue
            marker = ANNOTATION.search(line)
            if not marker:
                failures.append(f"{relative}:{line_number}: invalid OpenGrep annotation")
                continue
            kind, names = marker.groups()
            wanted = {name.strip() for name in names.split(",")}
            if kind == "ruleid":
                red_rules.update(wanted)
            unknown = wanted - rule_ids
            if unknown:
                failures.append(f"{relative}:{line_number}: unknown rule ids {sorted(unknown)}")
            checked += 1
            file_checked += 1
            annotated_lines.add(line_number + 1)
            actual = {rule for rule in found.get((relative, line_number + 1), set()) if rule.startswith(AUTHORITY)}
            expected = wanted if kind in {"ruleid", "todook"} else set()
            if actual != expected:
                failures.append(f"{relative}:{line_number + 1}: expected {sorted(expected)}, found {sorted(actual)}")
        failures.extend(_unexpected_authority_findings(found, relative, annotated_lines))
        if not file_checked:
            failures.append(f"{relative}: no checked expectations")
    if not checked:
        failures.append("no path-scoped authority expectations")
    for rule_id in sorted(rule_ids):
        if rule_id.startswith(AUTHORITY) and rule_id not in red_rules:
            failures.append(f"{rule_id}: no # ruleid: red line")
    return checked, failures


def selftest() -> None:
    import subprocess
    import tempfile

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        path = root / "nso_adapter" / "fixture.py"
        path.parent.mkdir()
        rules_path = root / "rules.yaml"
        rules_path.write_text("rules:\n  - id: nso-authority-write\n", encoding="utf-8")
        (root / "authority-results.json").write_text('{"results": [], "errors": []}', encoding="utf-8")
        cases = {
            "spacing": "#ruleid: nso-authority-write\nx = 1\n",
            "invalid": "# ruleid nso-authority-write\nx = 1\n",
            "zero_expectations": "x = 1\n",
            "unknown": "# ruleid: nso-authority-unknown\nx = 1\n",
            "missing_red": "# ok: nso-authority-write\nx = 1\n",
        }
        expected_errors = {
            "spacing": "expected",
            "invalid": "invalid OpenGrep annotation",
            "zero_expectations": "no path-scoped authority expectations",
            "unknown": "unknown rule ids",
            "missing_red": "no # ruleid: red line",
        }
        for name, source in cases.items():
            path.write_text(source, encoding="utf-8")
            result = subprocess.run(
                [sys.executable, __file__, str(root), str(rules_path)],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0:
                raise SystemExit(f"self-test {name} unexpectedly exited zero")
            if expected_errors[name] not in result.stderr:
                raise SystemExit(f"self-test {name} missed its expected failure: {result.stderr}")
        path.unlink()
        result = subprocess.run(
            [sys.executable, __file__, str(root), str(rules_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0 or "no path-scoped authority fixtures" not in result.stderr:
            raise SystemExit("self-test zero_fixtures missed its expected failure")
        path.write_text("# ruleid: nso-authority-write\nx = 1\n", encoding="utf-8")
        for kind, findings in (
            ("ok", []),
            ("todoruleid", []),
            ("todook", [{"path": "nso_adapter/fixture.py", "start": {"line": 2}, "check_id": "nso-authority-write"}]),
        ):
            path.write_text(
                f"#{kind}: nso-authority-write\nx = 1\n# ruleid: nso-authority-write\ny = 1\n", encoding="utf-8"
            )
            findings.append({"path": "nso_adapter/fixture.py", "start": {"line": 4}, "check_id": "nso-authority-write"})
            count, failures = check(root, {"results": findings, "errors": []}, {"nso-authority-write"})
            if count != 2 or failures:
                raise SystemExit(f"self-test {kind} failed: {failures}")
        path.write_text("# ruleid: nso-authority-write\nx = 1\ny = 2\n", encoding="utf-8")
        findings = [
            {"path": "nso_adapter/fixture.py", "start": {"line": line}, "check_id": "nso-authority-write"}
            for line in (2, 3)
        ]
        count, failures = check(root, {"results": findings, "errors": []}, {"nso-authority-write"})
        if count != 1 or not any("unexpected" in failure for failure in failures):
            raise SystemExit(f"self-test extra_finding missed its expected failure: {failures}")
        print("7 fixture-runner failure modes and 3 annotation kinds passed")


if __name__ == "__main__":
    if sys.argv[1:] == ["selftest"]:
        selftest()
    else:
        root = Path(sys.argv[1])
        report = json.loads((root / "authority-results.json").read_text(encoding="utf-8"))
        rules = yaml.safe_load(Path(sys.argv[2]).read_text(encoding="utf-8"))
        count, failures = check(root, report, {rule["id"] for rule in rules["rules"]})
        if failures:
            raise SystemExit("\n".join(failures))
        print(f"{count} path-scoped authority fixtures passed")
