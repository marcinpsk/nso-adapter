# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Development Compose lifecycle invariants."""

from pathlib import Path

import yaml


def test_dev_runtime_services_restart_with_docker_daemon() -> None:
    """A host/Docker restart must restore both halves of the adapter runtime."""
    compose_path = Path(__file__).resolve().parents[1] / "docker-compose.dev.yml"
    services = yaml.safe_load(compose_path.read_text(encoding="utf-8"))["services"]

    for service_name in ("adapter-db", "nso-adapter"):
        assert services[service_name].get("restart") == "unless-stopped", service_name


def test_shutdown_grace_covers_the_worker_drain() -> None:
    """Docker's default 10s stop grace SIGKILLs mid-drain: stop_workers may legally take
    up to _SHUTDOWN_TASK_WAIT, and cutting it off interrupts disposition and claim release
    — the exact cleanup the restart policy exists to protect. Both compose files must
    grant at least that long."""
    from nso_adapter.core.worker import _SHUTDOWN_TASK_WAIT

    for compose_name in ("docker-compose.yml", "docker-compose.dev.yml"):
        compose_path = Path(__file__).resolve().parents[1] / compose_name
        service = yaml.safe_load(compose_path.read_text(encoding="utf-8"))["services"]["nso-adapter"]
        grace = service.get("stop_grace_period")
        assert grace is not None, f"{compose_name}: no stop_grace_period"
        assert grace.endswith("s") and float(grace[:-1]) >= _SHUTDOWN_TASK_WAIT, compose_name
