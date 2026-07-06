# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <mazieba@libertyglobal.com>
"""Parsing/validation for fully-qualified Vault KV v2 references.

Canonical forms (always stored fully qualified, plugin and adapter alike):

* ``<mount>/<path...>#<key>`` — one secret field (SNMP communities, derived
  v3 ``#auth``/``#priv`` refs)
* ``<mount>/<path...>`` — a secret path whose fields are fixed by convention
  (SNMP v3 users: fields ``auth``/``priv``)

This dialect is mount-explicit and deliberately distinct from
:meth:`nso_adapter.secrets.base.SecretsProvider.get` references
(``path#field`` inside the provider's *configured* mount) — do not mix them.
The plugin mirrors this parser in ``netbox_nso_plugin/vault_refs.py``; both
test suites share the same golden vectors.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["VaultRef", "VaultRefError", "parse_vault_ref", "secret_fingerprint"]


def secret_fingerprint(value: str) -> str:
    """Return the cross-repo secret fingerprint: first 16 hex chars of SHA-256.

    Mirrors network-state-export's ``_community_hash`` (the read mirror's
    community identity) — computing the same digest over a Vault-held plaintext
    makes vault-vs-device comparison a plain string equality.
    """
    import hashlib

    return hashlib.sha256(value.encode()).hexdigest()[:16]


class VaultRefError(ValueError):
    """Raised for a reference that cannot yield a (mount, path[, key]) triple."""


@dataclass(frozen=True)
class VaultRef:
    mount: str
    path: str
    key: str | None

    def __str__(self) -> str:
        base = f"{self.mount}/{self.path}"
        return f"{base}#{self.key}" if self.key is not None else base


def parse_vault_ref(reference: str, *, require_key: bool | None = None) -> VaultRef:
    """Parse a fully-qualified Vault reference into (mount, path, key).

    ``require_key=True`` rejects refs without ``#key`` (community-style),
    ``require_key=False`` rejects refs with one (v3 path-style), ``None``
    accepts both. Raises :class:`VaultRefError` on any malformed input; the
    message contains only the reference text (refs are non-secret).
    """
    if not isinstance(reference, str) or not reference:
        raise VaultRefError(f"empty vault_ref {reference!r}")
    if any(ch.isspace() for ch in reference):
        raise VaultRefError(f"vault_ref contains whitespace: {reference!r}")
    if reference.count("#") > 1:
        raise VaultRefError(f"vault_ref has more than one '#': {reference!r}")

    locator, sep, key = reference.partition("#")
    if sep and not key:
        raise VaultRefError(f"vault_ref has an empty key after '#': {reference!r}")
    if require_key is True and not sep:
        raise VaultRefError(f"vault_ref must end in '#<key>': {reference!r}")
    if require_key is False and sep:
        raise VaultRefError(f"vault_ref must not carry a '#<key>' here: {reference!r}")

    mount, slash, path = locator.partition("/")
    if not slash or not mount or not path:
        raise VaultRefError(f"vault_ref must be '<mount>/<path...>': {reference!r}")
    if "" in path.split("/"):
        raise VaultRefError(f"vault_ref has an empty path segment: {reference!r}")

    return VaultRef(mount=mount, path=path, key=key if sep else None)
