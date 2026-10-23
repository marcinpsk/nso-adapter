# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Every data file inside the package ships in its distributions (#1833)."""

from __future__ import annotations

import fnmatch
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _data_files() -> list[Path]:
    return [
        path
        for path in sorted((ROOT / "nso_adapter").rglob("*"))
        if path.is_file() and path.suffix not in {".py", ".pyc"} and "__pycache__" not in path.parts
    ]


def test_every_data_file_in_the_package_is_declared_package_data():
    declared = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["tool"]["setuptools"][
        "package-data"
    ]
    files = _data_files()
    assert ROOT / "nso_adapter" / "nso" / "collection_manifest.json" in files
    for path in files:
        package = ".".join(path.parent.relative_to(ROOT).parts)
        patterns = declared.get(package, [])
        assert any(fnmatch.fnmatch(path.name, pattern) for pattern in patterns), path.relative_to(ROOT)
