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
        kwargs: dict[str, Any] = {"url": self._addr, "verify": self._verify_ssl}
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
