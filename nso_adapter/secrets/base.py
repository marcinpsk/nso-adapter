# SPDX-License-Identifier: Apache-2.0
"""SecretsProvider protocol.

A reference is a provider-specific string:
- vault provider: ``"path#field"``  (KV path within the configured mount)
- local provider: ``"ENV_VAR_NAME"``  (or the ``<NAME>_FILE`` variant)
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SecretsProvider(Protocol):
    def get(self, reference: str) -> str:
        """Resolve *reference* and return the secret value. Raise KeyError if not found."""
        ...
