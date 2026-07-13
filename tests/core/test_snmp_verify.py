# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""CR-A17: the ref→digest resolver itself, exercised directly.

The integration tests in test_removal / test_apply drive this through the workers, and they do
prove it runs (they assert the provider was read). But they run it inside the ``anyio.to_thread``
worker — which is the whole point of it, and also where coverage's tracer does not follow — so the
per-ref failure modes below would go unmeasured and, worse, untested at their own grain.

Each one matters on its own, because the caller's contract is *"an unresolvable label is ABSENT from
the result"* and every one of these has to land there rather than raising, returning a partial
digest, or hashing something that is not the secret.
"""

from __future__ import annotations

import pytest

from nso_adapter.core import snmp_verify
from nso_adapter.core.snmp_verify import _fingerprints_blocking, community_fingerprints
from nso_adapter.secrets.refs import secret_fingerprint
from tests.core.conftest import SNMP_COMMUNITY, SNMP_VAULT_REF, FakeVault, community_export_name

_PATH = ("network", "netbox/snmp/community/prod-ro")


def _provider(**kwargs) -> FakeVault:
    return FakeVault({_PATH: {"community": SNMP_COMMUNITY}}, **kwargs)


def test_a_resolvable_ref_yields_the_digest_the_DEVICE_EXPORT_uses():
    """The one assertion the whole feature rests on: the digest the adapter computes from the Vault
    plaintext is byte-for-byte the identifier network-state-export puts on the wire for that
    community. Computed here by an independent implementation of the export's rule, not by the
    function under test — a test that derives the expected value from the code it is testing proves
    only that the code agrees with itself.
    """
    out = _fingerprints_blocking(_provider(), {"prod-ro": SNMP_VAULT_REF})
    assert out == {"prod-ro": community_export_name(SNMP_COMMUNITY)}
    assert out["prod-ro"] == secret_fingerprint(SNMP_COMMUNITY)


def test_a_MALFORMED_ref_is_skipped_not_raised():
    """A ref with no ``#key`` cannot name a field. It must drop out of the result, not take the
    whole batch (and with it the residue check for every other community) down with it.
    """
    assert _fingerprints_blocking(_provider(), {"prod-ro": "no-key-here"}) == {}


def test_a_ref_pointing_at_a_MISSING_PATH_is_skipped():
    assert _fingerprints_blocking(_provider(), {"prod-ro": "network/nope#community"}) == {}


def test_a_ref_pointing_at_a_MISSING_KEY_at_a_real_path_is_skipped():
    """The path exists but holds no such field — e.g. the operator renamed the key in Vault."""
    ref = f"{_PATH[0]}/{_PATH[1]}#wrong-field"
    assert _fingerprints_blocking(_provider(), {"prod-ro": ref}) == {}


def test_an_EMPTY_secret_is_skipped_rather_than_hashed():
    """sha256("") is a perfectly valid digest that matches nothing on any device. Hashing it would
    turn "Vault holds a blank" into a confident "the community is not on the router".
    """
    provider = FakeVault({_PATH: {"community": ""}})
    assert _fingerprints_blocking(provider, {"prod-ro": SNMP_VAULT_REF}) == {}


def test_ONE_bad_ref_does_not_sink_the_GOOD_ones():
    """A batch is per-community. A single stale ref must not blind the check for the rest."""
    provider = _provider()
    out = _fingerprints_blocking(
        provider, {"prod-ro": SNMP_VAULT_REF, "stale": "network/gone#community", "bogus": "not-a-ref"}
    )
    assert sorted(out) == ["prod-ro"]


def test_a_vault_OUTAGE_yields_nothing_and_never_raises():
    """Every ref fails, and the caller sees an empty map — which it must read as "unverifiable",
    never as "none of these are on the device".
    """
    assert _fingerprints_blocking(_provider(fail=True), {"prod-ro": SNMP_VAULT_REF}) == {}


# ── the async entry point's short-circuits ────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_no_registered_provider_resolves_nothing():
    snmp_verify.register_secrets_provider(None)
    assert await community_fingerprints({"prod-ro": SNMP_VAULT_REF}) == {}


@pytest.mark.anyio
async def test_a_provider_without_read_path_resolves_nothing():
    """The local/env provider (`get(reference)` only) has no mount-explicit read. It must land on
    the same verdict as an outage — unverifiable — not raise AttributeError inside a worker.
    """

    class _LocalOnly:
        def get(self, reference: str) -> str:
            raise AssertionError("the community grain must not be resolved through the env provider")

    snmp_verify.register_secrets_provider(_LocalOnly())
    try:
        assert await community_fingerprints({"prod-ro": SNMP_VAULT_REF}) == {}
    finally:
        snmp_verify.register_secrets_provider(None)


@pytest.mark.anyio
async def test_an_empty_ref_set_never_touches_vault_at_all():
    provider = _provider()
    snmp_verify.register_secrets_provider(provider)
    try:
        assert await community_fingerprints({}) == {}
        assert provider.reads == 0
    finally:
        snmp_verify.register_secrets_provider(None)
