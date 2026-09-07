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
        """Resolve *reference* and return the secret value.

        Raise :class:`SecretResolutionError` when it cannot be resolved. The refusal must
        repeat NO part of the reference: a reference names where a secret lives, and every
        caller already holds the one it sent.
        """
        ...


class SecretResolutionError(RuntimeError):
    """A configured secret reference could not be resolved.

    ``reason`` classifies the failure and repeats no part of the reference. Startup resolves
    every configured reference before the app serves, so whatever this carries lands in the
    startup diagnostics. The CALLER stamps ``slot`` — the configuration entry it was loading
    — because only the caller knows which one that was.
    """

    def __init__(self, reason: str, *, slot: str | None = None) -> None:
        super().__init__(f"{slot}: {reason}" if slot is not None else reason)
        self.reason = reason
        self.slot = slot


def resolve_secret(provider: SecretsProvider, reference: str, *, slot: str) -> str:
    """Resolve one CONFIGURED reference, naming the config slot when it fails.

    The provider refuses without the reference, so this is where the failure gets an
    address an operator can act on. Nothing of the provider's own exception is attached:
    hvac repeats the request URL, and the URL carries the path.
    """
    try:
        return provider.get(reference)
    except SecretResolutionError as exc:
        failure = SecretResolutionError(exc.reason, slot=slot)
    except Exception as exc:  # noqa: BLE001 — every provider failure is one classified refusal
        failure = SecretResolutionError(f"the provider failed ({type(exc).__name__})", slot=slot)
    # Raised outside the handler: the provider exception would otherwise ride on __context__.
    raise failure
