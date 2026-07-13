# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""CR-A17: make an SNMP community's landing and retraction actually verifiable.

The adapter never sees a community string. It pushes a Vault triple and NSO resolves the secret at
commit time, so the write-path intent keys a community by its human-readable LABEL.
network-state-export, reading the far side of that writer, cannot return the string either — it
keys the community by ``sha256(community-string)[:16]``.

Two different namespaces, so the two could never be intersected, and both integrity checks that
exist to catch a silent drop had to abstain on this grain:

* ``_reader_compare_expected`` (post-apply) — is the intended key actually ON the device?
* ``_residue_after_removal`` (post-removal) — is the removed key actually OFF it?

Abstaining was honest (``residue_check="partial"``, the grain listed in ``residue_unverifiable``),
but it left the one scope where a survivor is a live credential as the only scope neither check
covers: a NED that silently drops a community, or a FASTMAP retract that leaves one live on the
router, was caught by nothing.

The adapter CAN close the gap, because it holds the vault_ref and the SNMP-harvest path already
reads secrets out of Vault. Resolve the ref, hash the plaintext with the same
:func:`secret_fingerprint` the export uses, and the comparison becomes plain string equality.

Two rules this module keeps:

* **Fail open, never fabricate.** A label whose secret Vault cannot answer for is simply absent
  from the result. The caller reports THAT grain unverifiable — exactly as before — rather than
  inferring "clean" from a comparison it could not run. A Vault outage must never turn into a
  fabricated clean bill on a credential.
* **Off the event loop** (CR-A13). hvac is blocking ``requests``; called straight from an
  ``async def`` it freezes the single event-loop thread — /health stops answering, the scheduler
  tick stalls. Every read here goes through one ``anyio.to_thread`` hop, and only ever from a
  worker (apply/removal job), never the request path.
"""

from __future__ import annotations

from collections.abc import Mapping

import anyio.to_thread
import structlog

from nso_adapter.secrets.refs import parse_vault_ref, secret_fingerprint

logger = structlog.get_logger(__name__)

# The secrets provider, registered at startup (mirrors importer's NSO/NetBox client registries).
# The workers run outside the request scope, so they cannot reach `request.app.state.secrets`.
_provider = None


def register_secrets_provider(provider) -> None:
    global _provider
    _provider = provider


def get_secrets_provider():
    return _provider


def _resolve_one(provider, ref: str) -> str | None:
    """Plaintext behind one fully-qualified ``mount/path#key`` ref, or None if it can't be read."""
    parsed = parse_vault_ref(ref, require_key=True)
    data = provider.read_path(parsed.mount, parsed.path)
    return data.get(parsed.key)


def _fingerprints_blocking(provider, refs: Mapping[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for label, ref in refs.items():
        try:
            secret = _resolve_one(provider, ref)
        except Exception as exc:  # noqa: BLE001 — one bad ref must not sink the rest
            # The ref is not a secret (it is a path), so it is safe to name. The VALUE never is.
            logger.warning("snmp_verify.vault_read_failed", label=label, vault_ref=ref, error=repr(exc))
            continue
        if secret:
            out[label] = secret_fingerprint(secret)
    return out


async def community_fingerprints(refs: Mapping[str, str]) -> dict[str, str]:
    """``label → sha256(secret)[:16]`` for every ref Vault can actually answer for.

    Never raises and never partially fails: a label that cannot be resolved — no provider, no
    ``read_path`` (the local/env provider), a malformed ref, a Vault outage, a missing key — is
    absent from the result, and the caller must treat it as unverifiable rather than as absent
    from the device.
    """
    if not refs:
        return {}
    provider = get_secrets_provider()
    if provider is None or not hasattr(provider, "read_path"):
        # The local/env provider has no mount-explicit read. Same verdict as a Vault outage:
        # unverifiable, which is where this grain already was.
        logger.info("snmp_verify.no_vault_provider", labels=sorted(refs))
        return {}
    # ONE thread hop for the whole batch: hvac is blocking, and a removal can carry several
    # communities. Hopping per ref would multiply the context switches for no benefit.
    return await anyio.to_thread.run_sync(lambda: _fingerprints_blocking(provider, refs))
