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
