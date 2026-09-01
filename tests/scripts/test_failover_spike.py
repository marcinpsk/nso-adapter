# SPDX-License-Identifier: Apache-2.0
"""Tests for the management-IP failover performance spike."""

from __future__ import annotations

from scripts.failover_spike import _report


def test_report_renders_live_device_unreachable_timing(capsys):
    results = {
        "reach_connect_primary": [],
        "unreach_capped_temp": [],
        "unreach_true_temp": [],
        "unreach_true_live": [1.25],
        "flip_cycle": [],
        "op_set_address": [],
        "op_disconnect": [],
        "op_get_address": [],
    }

    _report(results, probe_timeout=5.0)

    output = capsys.readouterr().out
    assert "unreachable connect NSO-true (trusted)" in output
    assert "1.250s" in output
