# SPDX-License-Identifier: Apache-2.0
"""HashiCorp Vault KV v2 secrets provider via AppRole.

References use the format  ``path#field``  where *path* is the KV path within
the configured mount (e.g. ``credentials/svc-netbox-nso#netbox_token``).
"""

from __future__ import annotations

import logging
from typing import Any

import hvac

logger = logging.getLogger(__name__)


class VaultSecretsProvider:
    """Reads secrets from Vault KV v2 using AppRole auth.

    ``get(reference)`` accepts ``"path#field"`` references and caches fetched
    paths so a single Vault read serves multiple fields from the same path.
    Re-authenticates automatically on 403.
    """

    #: Cap every hvac round-trip. hvac defaults to NO timeout, so an unreachable or wedged
    #: Vault would hold the calling thread forever — and these calls are made from request
    #: handlers (via anyio.to_thread), so an unbounded hang leaks the thread-pool slot and
    #: eventually starves every other blocking offload.
    REQUEST_TIMEOUT_S = 10

    def __init__(
        self,
        addr: str,
        role_id: str,
        secret_id: str,
        mount: str,
        namespace: str = "",
        verify_ssl: bool = True,
    ) -> None:
        self._addr = addr
        self._role_id = role_id
        self._secret_id = secret_id
        self._mount = mount
        self._namespace = namespace or None
        self._verify_ssl = verify_ssl
        self._client: hvac.Client | None = None
        # Per-path cache: {path: {field: value}}
        self._cache: dict[str, dict[str, str]] = {}

    def _authenticate(self) -> None:
        kwargs: dict[str, Any] = {
            "url": self._addr,
            "verify": self._verify_ssl,
            "timeout": self.REQUEST_TIMEOUT_S,
        }
        if self._namespace:
            kwargs["namespace"] = self._namespace
        client = hvac.Client(**kwargs)
        resp = client.auth.approle.login(role_id=self._role_id, secret_id=self._secret_id)
        client.token = resp["auth"]["client_token"]
        self._client = client
        self._cache.clear()
        logger.info("Vault AppRole login succeeded")

    def _fetch_path(self, path: str) -> dict[str, str]:
        assert self._client is not None
        secret = self._client.secrets.kv.v2.read_secret_version(
            mount_point=self._mount,
            path=path,
            raise_on_deleted_version=True,
        )
        data: dict[str, str] = secret["data"]["data"]
        self._cache[path] = data
        return data

    def get(self, reference: str) -> str:
        """Resolve a ``path#field`` reference.

        Args:
            reference: Vault KV path and field separated by ``#``,
                       e.g. ``"credentials/svc-netbox-nso#netbox_token"``.

        """
        if "#" not in reference:
            raise ValueError(f"Invalid Vault reference {reference!r} — expected 'path#field' format")
        path, _, field = reference.partition("#")

        # Check per-path cache first
        if path in self._cache:
            if field in self._cache[path]:
                return self._cache[path][field]
            raise KeyError(f"Field {field!r} not found at {self._mount}/{path}")

        if self._client is None:
            self._authenticate()
        try:
            data = self._fetch_path(path)
        except hvac.exceptions.Forbidden:
            logger.warning("Vault token expired, re-authenticating")
            self._authenticate()
            data = self._fetch_path(path)

        if field not in data:
            raise KeyError(f"Field {field!r} not found at {self._mount}/{path}")
        return data[field]

    # ── mount-explicit read/write (SNMP secrets endpoints) ────────────────────
    #
    # These take an explicit mount (the fully-qualified ``mount/path#key``
    # dialect of nso_adapter.secrets.refs) and are deliberately cache-free —
    # unlike ``get()``, whose refs and per-path cache live inside the single
    # configured mount.

    def _with_reauth(self, operation):
        if self._client is None:
            self._authenticate()
        try:
            return operation()
        except hvac.exceptions.Forbidden:
            logger.warning("Vault token expired, re-authenticating")
            self._authenticate()
            return operation()

    def _read_raw_meta(self, mount: str, path: str) -> tuple[dict[str, str], int | None]:
        assert self._client is not None
        try:
            secret = self._client.secrets.kv.v2.read_secret_version(
                mount_point=mount,
                path=path,
                raise_on_deleted_version=True,
            )
        except hvac.exceptions.InvalidPath:
            return {}, None
        version = secret["data"].get("metadata", {}).get("version")
        return dict(secret["data"]["data"]), int(version) if version is not None else None

    def _read_raw(self, mount: str, path: str) -> dict[str, str]:
        return self._read_raw_meta(mount, path)[0]

    def read_path(self, mount: str, path: str) -> dict[str, str]:
        """Read all fields at ``mount/path`` (KV v2); ``{}`` when the path doesn't exist."""
        return self._with_reauth(lambda: self._read_raw(mount, path))

    def read_path_meta(self, mount: str, path: str) -> tuple[dict[str, str], int | None]:
        """Read fields + current KV v2 version at ``mount/path``; ``({}, None)`` when absent."""
        return self._with_reauth(lambda: self._read_raw_meta(mount, path))

    def write_path(self, mount: str, path: str, data: dict[str, str], merge: bool = True) -> int:
        """Write fields at ``mount/path`` and return the new KV v2 version.

        ``merge=True`` preserves existing sibling fields (a raw KV v2 write
        replaces the whole data dict — setting only a v3 auth password must not
        silently delete the priv field). Requires only read+create/update ACLs
        (read-merge-write, no PATCH capability needed).
        """

        def _write() -> int:
            assert self._client is not None
            payload = {**self._read_raw(mount, path), **data} if merge else dict(data)
            resp = self._client.secrets.kv.v2.create_or_update_secret(
                mount_point=mount,
                path=path,
                secret=payload,
            )
            return int(resp["data"]["version"])

        version = self._with_reauth(_write)
        if mount == self._mount:
            self._cache.pop(path, None)  # keep get()'s per-path cache coherent
        return version
