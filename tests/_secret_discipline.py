# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Chain-walking assertions for the secret-discipline tests.

``raise ... from None`` only sets ``__suppress_context__``. The suppressed exception stays
reachable as ``__context__`` and its repr can still hold the secret, so an assertion that
reads those two flags passes while the material is one attribute away. Every check here
walks both chains to the end and reads the nodes instead.
"""

from __future__ import annotations


def exception_chain(exc: BaseException) -> list[BaseException]:
    """Every exception reachable from *exc* through ``__cause__`` AND ``__context__``."""
    seen: set[int] = set()
    pending: list[BaseException | None] = [exc]
    chain: list[BaseException] = []
    while pending:
        node = pending.pop()
        if node is None or id(node) in seen:
            continue
        seen.add(id(node))
        chain.append(node)
        pending += [node.__cause__, node.__context__]
    return chain


def assert_chain_free_of(exc: BaseException, secrets) -> None:
    """Fail when any node of *exc*'s cause/context chain repeats one of *secrets*."""
    for node in exception_chain(exc):
        rendered = f"{node!r} {node}"
        for secret in secrets:
            assert secret not in rendered, f"{type(node).__name__} in the chain repeats secret material"
