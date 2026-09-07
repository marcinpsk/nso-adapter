# SPDX-License-Identifier: Apache-2.0
"""Shared projection fixture builders."""


async def freeze_snapshot(db, device_id, stream):
    from nso_adapter.core.projection import freeze_fragment, snapshot_stream
    from nso_adapter.store.models import Device

    return await freeze_fragment(
        db, await db.get(Device, device_id), stream, await snapshot_stream(db, device_id, stream)
    )
