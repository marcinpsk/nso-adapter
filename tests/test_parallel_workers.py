# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""``-n auto`` must stop at the worker ceiling, whatever the host reports.

The suite runs with ``-n auto`` in addopts, so on a 32-core host xdist would start 32
workers and each one clones its own databases from the single test Postgres.
"""

from __future__ import annotations

import pytest

from tests.conftest import MAX_PARALLEL_WORKERS, pytest_xdist_auto_num_workers


@pytest.mark.parametrize(
    ("detected_workers", "expected"),
    [("2", 2), (str(MAX_PARALLEL_WORKERS), MAX_PARALLEL_WORKERS), ("32", MAX_PARALLEL_WORKERS)],
)
def test_auto_worker_count_never_exceeds_the_ceiling(monkeypatch, detected_workers, expected):
    """xdist's own detector reads this env var first, so it stands in for the host's CPU count."""
    monkeypatch.setenv("PYTEST_XDIST_AUTO_NUM_WORKERS", detected_workers)

    assert pytest_xdist_auto_num_workers(None) == expected
