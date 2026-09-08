# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""What ``tests/_secret_discipline.assert_chain_free_of`` must reach.

The helper is the runtime half of the suppression discipline: a sanitized refusal is only
sanitized if NOTHING reachable from it repeats the material. "Reachable" is what a formatted
traceback prints, and that is more than ``__cause__`` and ``__context__``:

* a note attached with ``BaseException.add_note`` is printed under the exception it is on;
* the members of an exception group are printed under the group, and a group is what any
  ``anyio``/``asyncio`` task group raises.

Every exception here is really raised, so the chain under test is the one the interpreter
builds rather than one the test assembled by hand.
"""

from __future__ import annotations

import pytest

from tests._secret_discipline import assert_chain_free_of, exception_chain

_SECRET = "placeholder-vault-secret"


def _raised_with_context(inner: BaseException, outer: BaseException) -> BaseException:
    """Raise *outer* while *inner* is handled, so the interpreter attaches inner to it."""
    try:
        raise inner
    except BaseException:
        try:
            raise outer
        except BaseException as caught:
            return caught


def test_a_note_on_the_raised_exception_is_caught() -> None:
    """A note travels with the exception and a traceback prints it, message or not."""
    refusal = ValueError("The Vault operation failed (RuntimeError)")
    refusal.add_note(f"while reading {_SECRET}")

    assert _SECRET not in f"{refusal!r} {refusal}", "the message itself must stay clean for this test"
    with pytest.raises(AssertionError, match="repeats secret material"):
        assert_chain_free_of(refusal, [_SECRET])


def test_a_note_DEEPER_in_the_chain_is_caught() -> None:
    """The provider's own exception stays on __context__, and so do its notes."""
    provider_failure = RuntimeError("vault: read failed")
    provider_failure.add_note(f"holding {_SECRET}")
    caught = _raised_with_context(provider_failure, ValueError("The Vault operation failed (RuntimeError)"))

    assert caught.__context__ is provider_failure
    with pytest.raises(AssertionError, match="repeats secret material"):
        assert_chain_free_of(caught, [_SECRET])


def test_a_GROUP_MEMBER_is_caught() -> None:
    """A task group raises a group: the members hang off it, not off cause/context."""
    member = RuntimeError("vault: read failed")
    member.add_note(f"holding {_SECRET}")
    try:
        raise ExceptionGroup("provisioning failed", [member])
    except ExceptionGroup as group:
        caught = group

    assert caught.__cause__ is None and caught.__context__ is None, "the member is reachable only as a member"
    assert _SECRET not in f"{caught!r} {caught}", "only walking INTO the member can find this one"
    assert len(exception_chain(caught)) == 2, "the walker must reach the member"
    with pytest.raises(AssertionError, match="repeats secret material"):
        assert_chain_free_of(caught, [_SECRET])


def test_a_CLEAN_chain_passes() -> None:
    """The assertions above are not vacuous: a sanitized refusal with a clean note passes."""
    provider_failure = RuntimeError("vault: read failed")
    refusal = ValueError("The Vault operation failed (RuntimeError)")
    refusal.add_note("secrets.set operation_id=0123456789ab")
    caught = _raised_with_context(provider_failure, refusal)

    assert_chain_free_of(caught, [_SECRET])
