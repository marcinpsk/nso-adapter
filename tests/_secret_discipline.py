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


class EchoingVault:
    """A Vault provider whose failure repeats the reference and the plaintext.

    A real client fails with the request URL in the message, and a decode failure can repeat the
    payload, so a sink that logs the exception repeats whatever the provider said. A real object,
    never a Mock: a Mock answers ``read_path`` with another Mock and the sink never sees a failure.
    """

    def __init__(self, ref: str, secret: str):
        self.ref = ref
        self.secret = secret
        self.reads = 0

    def read_path(self, mount: str, path: str) -> dict[str, str]:
        self.reads += 1
        raise RuntimeError(f"vault: read of {mount}/{path} failed for {self.ref} holding {self.secret}")


def assert_records_free_of(records, secrets) -> None:
    """Fail when any captured structlog record repeats one of *secrets*."""
    rendered = repr([dict(record) for record in records])
    for index, secret in enumerate(secrets):
        # The index, never the value: a failure prints this into pytest output and CI logs.
        assert secret not in rendered, f"a log record repeats secret material (secrets[{index}])"
