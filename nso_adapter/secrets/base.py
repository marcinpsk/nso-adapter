# SPDX-License-Identifier: Apache-2.0
"""SecretsProvider protocol.

A reference is a provider-specific string:
- vault provider: ``"path#field"``  (KV path within the configured mount)
- local provider: ``"ENV_VAR_NAME"``  (or the ``<NAME>_FILE`` variant)
"""

from __future__ import annotations

from collections.abc import Mapping
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
    startup diagnostics. The CALLER stamps ``slot``, the configuration entry it was loading,
    because only the caller knows which one that was.
    """

    def __init__(self, reason: str, *, slot: str | None = None) -> None:
        super().__init__(f"{slot}: {reason}" if slot is not None else reason)
        self.reason = reason
        self.slot = slot


def selected_secret_value(fields: Mapping[str, object], key: str) -> str | None:
    """Return one selected string field, or reject a present value of another type."""
    if key not in fields:
        return None
    value = fields[key]
    if not isinstance(value, str):
        raise SecretResolutionError("the selected secret field is not a string")
    return value


def require_nonblank_secret(value: str, *, slot: str) -> str:
    """Reject a blank secret at the configuration boundary.

    Both providers serve a blank as a SET value on purpose, so nothing below this rejects one.
    A blank shared secret is guessed on the first attempt, so a slot that keys or authenticates
    fails fast here instead of running with it. The value is never trimmed.
    """
    if not value.strip():
        raise SecretResolutionError("the configured secret is blank", slot=slot)
    return value


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
    except Exception as exc:  # noqa: BLE001, every provider failure is one classified refusal
        failure = SecretResolutionError(f"the provider failed ({type(exc).__name__})", slot=slot)
    # Raised outside the handler: the provider exception would otherwise ride on __context__.
    raise failure
