# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Shared projection fixture builders."""


async def freeze_tables(db, device_id, stream, tables):
    """Freeze *tables* into a document fragment the way authorization does."""
    from nso_adapter.core.projection import freeze_fragment
    from nso_adapter.store.models import Device

    return await freeze_fragment(db, await db.get(Device, device_id), stream, tables)


async def freeze_snapshot(db, device_id, stream):
    from nso_adapter.core.projection import snapshot_stream

    return await freeze_tables(db, device_id, stream, await snapshot_stream(db, device_id, stream))


def encode_hydrated_section(section, rows, context):
    """Render real hydrated rows through the section's registered encoder."""
    from nso_adapter.core.community_dialect import community_dialect_by_name
    from nso_adapter.core.projection import section_registry
    from nso_adapter.nso.apply import SectionExecution

    entry = section_registry()[section]
    tables = {spec.model.__tablename__: rows.get(spec.model, []) for spec in entry.tables}
    execution = SectionExecution(context["ned_id"], community_dialect_by_name(context["dialect"]))
    return entry.encode(tables, execution)
