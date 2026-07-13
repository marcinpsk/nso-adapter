# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Shared fixtures for the core worker tests (CR-A17: the SNMP community Vault fake)."""

from __future__ import annotations

import hashlib
import threading

import pytest

# The one community both integrity checks have to reason about, and the ref that names it.
SNMP_COMMUNITY = "s3cr3t-ro"
SNMP_VAULT_REF = "network/netbox/snmp/community/prod-ro#community"
_SNMP_VAULT_PATH = ("network", "netbox/snmp/community/prod-ro")


def community_export_name(secret: str) -> str:
    """The EXPORT's community key — sha256(community string)[:16], never the intent label.

    Deliberately reimplemented here rather than imported from the adapter: this is the
    network-state-export side of the contract, and a test that computes the expected value with the
    very function under test proves only that the function agrees with itself.
    """
    return hashlib.sha256(secret.encode()).hexdigest()[:16]


class FakeVault:
    """The mount-explicit read surface of VaultSecretsProvider — a real object, not a Mock.

    A MagicMock would answer ``read_path`` with a MagicMock, whose ``.get(key)`` is another
    MagicMock: ``secret_fingerprint`` would happily hash its repr and the test would go green
    against a digest that matches nothing on any device. The point of these tests is that two
    independent code paths derive the SAME digest from the SAME plaintext, so the fake must hold
    real bytes.
    """

    def __init__(self, secrets: dict[tuple[str, str], dict[str, str]], *, fail: bool = False):
        self._secrets = secrets
        self._fail = fail
        self.reads = 0
        self.read_threads: list[int] = []

    def read_path(self, mount: str, path: str) -> dict[str, str]:
        self.reads += 1
        # CR-A13: hvac is blocking `requests`. A read that happens on the event-loop thread freezes
        # the whole adapter for the round-trip — /health stops answering (a liveness probe can then
        # kill the container mid-write) and the scheduler tick driving failover probes stalls.
        # Recording the thread is what lets a test PROVE the to_thread hop is really there.
        self.read_threads.append(threading.get_ident())
        if self._fail:
            raise RuntimeError("vault: connection refused")
        return dict(self._secrets.get((mount, path), {}))


@pytest.fixture
def vault():
    """Register a Vault provider for the WORKERS (they run outside the request scope), then clear it.

    Call the yielded factory to install one: ``vault()`` for a healthy Vault, ``vault(fail=True)``
    for an outage. Not calling it at all models the local/env provider — no ``read_path`` — which
    must land on the same "unverifiable" verdict as an outage.
    """
    from nso_adapter.core import snmp_verify

    def _register(**kwargs) -> FakeVault:
        provider = FakeVault({_SNMP_VAULT_PATH: {"community": SNMP_COMMUNITY}}, **kwargs)
        snmp_verify.register_secrets_provider(provider)
        return provider

    yield _register
    snmp_verify.register_secrets_provider(None)
