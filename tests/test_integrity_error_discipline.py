# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Run the IntegrityError guard and pin its source-level rule."""

from __future__ import annotations

import pytest

from tests.integrity_error_discipline import scan_source, scan_tree


def test_repository_integrity_error_handlers_classify_or_reraise() -> None:
    violations = scan_tree()
    assert not violations, "\n".join(str(violation) for violation in violations)


@pytest.mark.parametrize(
    "source",
    [
        """
from sqlalchemy.exc import IntegrityError
from nso_adapter.store.db import _violated_constraint
try:
    write()
except IntegrityError as exc:
    if _violated_constraint(exc) != \"expected\":
        raise
    convert()
""",
        """
import sqlalchemy as sa
from nso_adapter.store import db as store_db
try:
    write()
except sa.exc.IntegrityError as error:
    if store_db._violated_constraint(error) == \"expected\":
        convert()
    else:
        raise
""",
        """
from sqlalchemy.exc import IntegrityError
try:
    write()
except IntegrityError:
    record_failure()
    raise
""",
    ],
)
def test_accepts_constraint_classification_or_an_unconditional_reraise(source: str) -> None:
    assert scan_source(source) == []


@pytest.mark.parametrize(
    "source",
    [
        """
from sqlalchemy.exc import IntegrityError
try:
    write()
except IntegrityError:
    convert()
""",
        """
from sqlalchemy.exc import IntegrityError
from nso_adapter.store.db import _violated_constraint
try:
    write()
except IntegrityError as exc:
    if should_classify:
        _violated_constraint(exc)
    convert()
""",
        """
from sqlalchemy.exc import IntegrityError
try:
    write()
except IntegrityError:
    if debug:
        raise
    convert()
""",
    ],
)
def test_rejects_an_unclassified_domain_conversion(source: str) -> None:
    violations = scan_source(source, "sample.py")
    assert len(violations) == 1
    assert "must bind IntegrityError and call" in str(violations[0])
    assert "or re-raise it with a bare raise" in str(violations[0])


def test_a_helper_call_in_a_deferred_scope_does_not_satisfy_the_handler() -> None:
    source = """
from sqlalchemy.exc import IntegrityError
from nso_adapter.store.db import _violated_constraint
try:
    write()
except IntegrityError as exc:
    def classify_later():
        return _violated_constraint(exc)
    convert()
"""

    assert len(scan_source(source)) == 1
