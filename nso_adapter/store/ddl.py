# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Database objects the ORM metadata cannot express, defined ONCE (#1522 §G1).

The generation-immutability trigger has to exist on both schema paths — ``create_all`` (the
startup convenience) and alembic (the production run) — and the schema-parity test compares
tables, not triggers, so a second hand-written copy in the migration could drift unnoticed.
Both paths render the SQL from here instead. This module imports nothing from the app, so a
migration importing it cannot be broken by model drift.
"""

from __future__ import annotations

#: A deployment generation's identity: what a retry re-sends and what settlement stamps.
#: Everything else on the row (status, job_id, attempts, last_error, updated_at, settled_at)
#: is lifecycle and stays writable.
GENERATION_IMMUTABLE_COLUMNS: tuple[str, ...] = (
    "device_id",
    "seq",
    "mode",
    "document",
    "digest",
    "allowed_removal_keys",
    "source_push_seq",
    "stream_revisions",
    "removal_context",
    "created_at",
)

#: Immutable columns with a native equality operator. Every other column is compared as
#: text. PostgreSQL defines no equality operator for ``json``, so this safe default prevents
#: a newly guarded JSON column from breaking every lifecycle update at runtime.
_COMPARABLE_COLUMNS = frozenset({"device_id", "seq", "mode", "digest", "created_at"})

GENERATION_IMMUTABLE_TRIGGER = "deployment_generation_immutable"
_FUNCTION = "deployment_generation_reject_rewrite"


def _compare(col: str) -> str:
    cast = "" if col in _COMPARABLE_COLUMNS else "::text"
    return f"NEW.{col}{cast} IS DISTINCT FROM OLD.{col}{cast}"


def generation_immutability_ddl() -> tuple[str, ...]:
    """Return the statements that install the trigger, in order."""
    predicate = "\n        OR ".join(_compare(col) for col in GENERATION_IMMUTABLE_COLUMNS)
    columns = ", ".join(GENERATION_IMMUTABLE_COLUMNS)
    return (
        # No printf placeholders in the message: SQLAlchemy's DDL construct percent-formats
        # its statement, so a literal % here would break the create_all path.
        f"""
CREATE OR REPLACE FUNCTION {_FUNCTION}() RETURNS trigger AS $$
BEGIN
    IF {predicate} THEN
        RAISE EXCEPTION USING
            ERRCODE = 'integrity_constraint_violation',
            MESSAGE = 'deployment_generation ' || OLD.id || ' is immutable: {columns} may not be updated';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
""".strip(),
        f"DROP TRIGGER IF EXISTS {GENERATION_IMMUTABLE_TRIGGER} ON deployment_generation",
        f"CREATE TRIGGER {GENERATION_IMMUTABLE_TRIGGER} BEFORE UPDATE ON deployment_generation "
        f"FOR EACH ROW EXECUTE FUNCTION {_FUNCTION}()",
    )


def generation_immutability_drop_ddl() -> tuple[str, ...]:
    """Return the statements that remove the trigger and its function."""
    return (
        f"DROP TRIGGER IF EXISTS {GENERATION_IMMUTABLE_TRIGGER} ON deployment_generation",
        f"DROP FUNCTION IF EXISTS {_FUNCTION}()",
    )
