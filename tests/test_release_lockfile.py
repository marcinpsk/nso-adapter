# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The version chain includes uv.lock, and the release is what keeps it in step.

``pyproject.toml`` and ``nso_adapter/__init__.py`` are pinned against each other elsewhere.
``uv.lock`` is the third copy: it records this project as an editable package WITH its
version, so a bump that does not reach the lock leaves the released sdist pinning the
previous version of its own package. semantic-release refreshes and stages it from
``build_command``; these tests are what notices when that stops happening.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _locked_version() -> str:
    lock = tomllib.loads((_REPO_ROOT / "uv.lock").read_text())
    (entry,) = [package for package in lock["package"] if package["name"] == "nso-adapter"]
    return entry["version"]


def test_the_lockfile_records_the_declared_version():
    """A stale lock here means the release stopped refreshing it."""
    from nso_adapter import __version__

    assert _locked_version() == __version__


def test_the_release_refreshes_and_ships_the_lockfile():
    """The two halves of the mechanism: regenerate the lock, and put it in the release commit.

    Shipping it is the load-bearing half — semantic-release carries only ``assets`` into the
    version commit, so dropping that entry leaves the refreshed lock behind on the tag.
    """
    release = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text())["tool"]["semantic_release"]

    assert "uv lock" in release["build_command"]
    assert "uv.lock" in release["assets"]
    # build_command only runs when the release action is asked to build.
    assert "build: true" in (_REPO_ROOT / ".github" / "workflows" / "release.yml").read_text()
