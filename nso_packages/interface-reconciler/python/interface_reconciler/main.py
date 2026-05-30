# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""interface_reconciler.main
~~~~~~~~~~~~~~~~~~~~~~~~~~
NSO service callback for the interface-reconciler package.

Implements the ``interface-reconciler-sp`` servicepoint.  For each instance
the cb_create writes ``description``, ``shutdown``, IPv4/IPv6 addresses,
and VRF onto the target device interface using the NED-specific Maagic path.

Reconcile-commit semantics — the NSO caller (nso-adapter core/apply.py)
must commit the transaction with the ``reconcile`` option so that FASTMAP
adopts pre-existing brownfield config rather than creating a conflict.  The
service callback itself is NED-config-write only; it does not set the commit
flag — that is the caller's responsibility.

Supported NEDs:
  * cisco-ios-cli  (IOS / IOS-XE, shutdown leaf)
  * cisco-iosxr-cli (IOS-XR, shutdown leaf)
  * cisco-nx-cli  (NX-OS, shutdown leaf)
  * juniper-junos-nc  (Junos classic, disable presence leaf)
  * juniper-junos-evo-nc  (Junos EVO, same YANG structure as junos-nc)

Extension to additional NEDs: add an ``_apply_*`` method and wire it in
``_NED_HANDLERS`` → handler dispatch in cb_create.
"""
from __future__ import annotations

import ipaddress
import re

import ncs
import ncs.application


# Maps NED ID prefix → handler method name on the service class.
_NED_HANDLERS: dict[str, str] = {
    "cisco-ios-cli": "_apply_ios_family",
    "cisco-iosxr-cli": "_apply_ios_family",
    "cisco-nx-cli": "_apply_ios_family",
    "juniper-junos-nc": "_apply_junos",
    "juniper-junos-evo-nc": "_apply_junos",
}


def _prefix_len_to_mask(prefix_len: int) -> str:
    """Convert an IPv4 prefix length (0–32) to a dotted-decimal subnet mask."""
    return str(ipaddress.IPv4Network(f"0.0.0.0/{prefix_len}").netmask)


def _parse_if_name(if_name: str) -> tuple[str, str]:
    """Split an interface name into (type, id).

    Handles both space-separated and joined forms:
      'GigabitEthernet0/0/0/1' → ('GigabitEthernet', '0/0/0/1')
      'GigabitEthernet 0/0/0'  → ('GigabitEthernet', '0/0/0')
      'Port-channel1'          → ('Port-channel', '1')

    Raises ValueError for names that do not match the expected pattern.
    """
    m = re.match(r"^([A-Za-z][A-Za-z0-9\-]*)\s*(\d.*)$", if_name)
    if not m:
        raise ValueError(f"Cannot parse interface name: {if_name!r}")
    return m.group(1), m.group(2)


class InterfaceReconcilerService(ncs.application.Service):
    """Service callback registered at the 'interface-reconciler-sp' servicepoint."""

    @ncs.application.Service.create
    def cb_create(self, tctx, root, service, proplist):
        device_name = str(service.device)
        if_name = str(service.interface_name)

        # Resolve the NSO device and its NED ID.
        dev = root.ncs__devices.ncs__device[device_name]
        ned_id = ""
        try:
            ned_id = str(dev.device_type.cli.ned_id)
        except Exception:
            pass

        handler = None
        for prefix, method_name in _NED_HANDLERS.items():
            if ned_id.startswith(prefix):
                handler = getattr(self, method_name)
                break

        if handler is None:
            raise ValueError(
                f"interface-reconciler: unsupported NED {ned_id!r} for device "
                f"{device_name!r} — add a handler in main.py"
            )

        handler(root, dev, if_name, service)

    # ── NED-specific writers ─────────────────────────────────────────────────

    def _apply_ios_family(self, root, dev, if_name: str, service) -> None:  # noqa: ARG002
        """Apply config for cisco-ios-cli / cisco-iosxr-cli / cisco-nx-cli.

        All three NED families use the same interface container shape and the
        ``shutdown`` leaf to represent the enabled/disabled state.
        """
        if_type, if_id = _parse_if_name(if_name)

        # Navigate: device.config.interface.<IfType>[<id>]
        # The Maagic API exposes the NED namespace transparently via the
        # generated _namespaces module; we traverse it by Python attribute name.
        ifc_container = dev.config.interface
        # getattr handles the case where the interface type subtree does not
        # yet exist; create() on the list node will create it.
        if_list = getattr(ifc_container, if_type, None)
        if if_list is None:
            raise ValueError(
                f"interface-reconciler: interface type {if_type!r} not found "
                f"in NED config for device {str(dev.name)!r}"
            )
        if_obj = if_list.create(if_id)

        # Write description if the leaf is present in the service instance.
        desc = service.description
        if desc is not None:
            if_obj.description = str(desc)

        # Write enabled/shutdown if the leaf is present in the service instance.
        enabled = service.enabled
        if enabled is not None:
            enabled_val = bool(enabled)
            if enabled_val:
                # enabled=True → remove shutdown leaf (no shutdown)
                try:
                    if_obj.shutdown.delete()
                except Exception:
                    pass  # leaf already absent — that is correct
            else:
                # enabled=False → set shutdown leaf
                if_obj.shutdown.create()

        # Write VRF if the leaf is present.
        vrf = service.vrf
        if vrf is not None:
            if_obj.vrf.forwarding = str(vrf)

        # Write IPv4 addresses.  Write primary before secondaries so IOS
        # accepts the secondary (requires a primary to already exist).
        for ip_entry in service.ipv4_address:
            addr = str(ip_entry.address)
            mask = _prefix_len_to_mask(int(ip_entry.prefix_length))
            if not ip_entry.secondary:
                if_obj.ip.address.primary.address = addr
                if_obj.ip.address.primary.mask = mask

        for ip_entry in service.ipv4_address:
            addr = str(ip_entry.address)
            mask = _prefix_len_to_mask(int(ip_entry.prefix_length))
            if ip_entry.secondary:
                sec = if_obj.ip.address.secondary.create(addr)
                sec.mask = mask

        # Write IPv6 addresses.  Prefix list key is the full "addr/len" string.
        for ipv6_entry in service.ipv6_address:
            addr = str(ipv6_entry.address)
            prefix_len = int(ipv6_entry.prefix_length)
            if_obj.ipv6.address.prefix_list.create(f"{addr}/{prefix_len}")

    def _apply_junos(self, root, dev, if_name: str, service) -> None:  # noqa: ARG002
        """Apply config for juniper-junos-nc / juniper-junos-evo-nc.

        Junos models interface state via a ``disable`` presence leaf (type empty)
        inside a ``choice enable-disable`` at the physical interface level.
        Enabling the interface removes the leaf; disabling creates it.

        Both juniper-junos-nc and juniper-junos-evo-nc expose the same
        ``jc__configuration`` attribute (module junos-conf-root, prefix jc,
        namespace ``http://yang.juniper.net/junos/conf/root``).

        Maagic path:
          dev.config
            .jc__configuration
            .jc_interfaces__interfaces
            .interface[if_name]
        """
        conf = dev.config.jc__configuration
        if_obj = conf.jc_interfaces__interfaces.interface.create(if_name)

        desc = service.description
        if desc is not None:
            if_obj.description = str(desc)

        enabled = service.enabled
        if enabled is not None:
            enabled_val = bool(enabled)
            if enabled_val:
                # enabled=True → remove disable leaf (interface up)
                try:
                    if_obj.disable.delete()
                except Exception:
                    pass  # leaf already absent — correct
            else:
                # enabled=False → create disable leaf (interface down)
                if_obj.disable.create()


class InterfaceReconcilerApp(ncs.application.Application):
    """NSO application entry point.  Registers the service callback."""

    def setup(self):
        self.log.info("interface-reconciler: registering service callback")
        self.register_service("interface-reconciler-sp", InterfaceReconcilerService)

    def teardown(self):
        self.log.info("interface-reconciler: teardown")
