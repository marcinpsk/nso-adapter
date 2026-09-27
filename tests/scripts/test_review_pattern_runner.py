# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Exercise the shell runner with a deterministic external scanner protocol fake."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("binary_form", ["relative", "absolute", "command"])
def test_test_mode_can_invoke_scanner_after_changing_directory(tmp_path, binary_form):
    repo = Path(__file__).resolve().parents[2]
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    for name in ("check-review-patterns", "check_review_pattern_fixtures.py"):
        shutil.copy2(repo / "scripts" / name, scripts / name)
    fixtures = tmp_path / ".opengrep/tests/nso_adapter"
    fixtures.mkdir(parents=True)
    (tmp_path / ".opengrep/nso-rules.yaml").write_text("rules:\n  - id: nso-authority-write\n")
    (fixtures / "example.py").write_text("# ruleid: nso-authority-write\nrow.authorized_revision = 5\n")
    report = {
        "results": [{"path": "nso_adapter/example.py", "start": {"line": 2}, "check_id": "nso-authority-write"}],
        "errors": [],
    }
    scanner = scripts / "scanner"
    scanner.write_text(
        f"#!{sys.executable}\n"
        "import json, sys\n"
        "from pathlib import Path\n"
        "if sys.argv[1] == 'test':\n"
        "    assert (Path(sys.argv[2]) / 'review-patterns.yaml').is_file()\n"
        "elif sys.argv[1] == 'scan':\n"
        "    assert Path('review-patterns.yaml').is_file()\n"
        f"    print(json.dumps({report!r}))\n"
        "else:\n"
        "    raise SystemExit(2)\n"
    )
    scanner.chmod(0o755)
    binary = {"relative": "./scripts/scanner", "absolute": str(scanner), "command": "scanner"}[binary_form]
    result = subprocess.run(
        [str(scripts / "check-review-patterns"), "test"],
        cwd=tmp_path.parent,
        env={
            **os.environ,
            "OPENGREP_BIN": binary,
            "PATH": f"{scripts}:{Path(sys.executable).parent}:{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "1 path-scoped authority fixtures passed" in result.stdout
