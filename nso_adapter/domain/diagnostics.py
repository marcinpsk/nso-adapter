# SPDX-License-Identifier: Apache-2.0
"""Diagnostic-safe device identity fields."""

from __future__ import annotations

import hmac
import sys

_DEVICE_REF_DOMAIN = b"nso-adapter:device-ref:v1\0"
_DEVICE_REF_HEX_WIDTH = 16
_device_ref_key: bytes | None = None


def register_device_ref_key(key: str) -> None:
    global _device_ref_key
    if not key:
        raise ValueError("diagnostic reference key must not be empty")
    _device_ref_key = key.encode("utf-8")


def _encode_component(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return len(encoded).to_bytes(4, "big") + encoded


def device_ref(nso_instance: str, nso_device_name: str) -> str:
    """Return a stable keyed reference for one adapter device identity."""
    if _device_ref_key is None:
        raise RuntimeError("diagnostic reference key is not configured")
    # Callers pass NSO oper-data and job-payload values straight in. Without this the failure is
    # an AttributeError from .encode() inside the digest, raised from whatever log call built it.
    for label, value in (("nso_instance", nso_instance), ("nso_device_name", nso_device_name)):
        if not isinstance(value, str):
            raise TypeError(f"{label} must be a str")
    identity = _DEVICE_REF_DOMAIN + _encode_component(nso_instance) + _encode_component(nso_device_name)
    return hmac.digest(_device_ref_key, identity, "sha256").hex()[:_DEVICE_REF_HEX_WIDTH]


def device_fields(
    *,
    device_id: int | None = None,
    nso_instance: str | None = None,
    nso_device_name: str | None = None,
) -> dict[str, str | int]:
    """Return the available diagnostic-safe fields for one device."""
    if device_id is not None and (isinstance(device_id, bool) or not isinstance(device_id, int)):
        raise TypeError("device_id must be an int")
    fields: dict[str, str | int] = {}
    if device_id is not None:
        fields["device_id"] = device_id
    if nso_device_name is not None:
        if nso_instance is None:
            raise ValueError("nso_instance is required when nso_device_name is provided")
        fields["device_ref"] = device_ref(nso_instance, nso_device_name)
    return fields


def _main(argv: list[str]) -> int:
    if argv != ["correlate"]:
        print("usage: python -m nso_adapter.domain.diagnostics correlate", file=sys.stderr)
        return 2
    values = sys.stdin.read().splitlines()
    if len(values) != 2:
        print("correlate expects the NSO instance and device name on separate lines", file=sys.stderr)
        return 2

    from nso_adapter.config import get_config, get_env_settings
    from nso_adapter.secrets import make_provider, resolve_secret

    cfg = get_config()
    provider = make_provider(cfg, get_env_settings())
    key = resolve_secret(provider, cfg.diagnostic_key_ref, slot="diagnostic_key_ref")
    register_device_ref_key(key)
    print(device_ref(values[0], values[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
