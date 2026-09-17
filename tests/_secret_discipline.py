# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Chain-walking assertions for the secret-discipline tests.

``raise ... from None`` only sets ``__suppress_context__``. The suppressed exception stays
reachable as ``__context__`` and its repr can still hold the secret, so an assertion that
reads those two flags passes while the material is one attribute away. Every check here
walks the chains to the end and reads the nodes instead.

"Reachable" is what a formatted traceback prints, which is more than the two chains: a note
added with ``BaseException.add_note`` prints under the exception it is on, and an exception
group prints its members, which hang off neither ``__cause__`` nor ``__context__``. Any task
group raises one, so both are walked here.
"""

from __future__ import annotations


def exception_chain(exc: BaseException) -> list[BaseException]:
    """Every exception reachable from *exc*: ``__cause__``, ``__context__``, group members."""
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
        if isinstance(node, BaseExceptionGroup):
            pending += list(node.exceptions)
    return chain


def assert_chain_free_of(exc: BaseException, secrets) -> None:
    """Fail when any node of *exc*'s chain, its notes included, repeats one of *secrets*."""
    for node in exception_chain(exc):
        rendered = " ".join((repr(node), str(node), *getattr(node, "__notes__", ())))
        for index, secret in enumerate(secrets):
            if secret in rendered:
                raise AssertionError(f"exception chain repeats secret material (secrets[{index}])")


def assert_text_free_of(value, secrets) -> None:
    """Fail without copying protected material or the inspected value into diagnostics."""
    rendered = str(value)
    for index, secret in enumerate(secrets):
        if secret in rendered:
            raise AssertionError(f"text repeats secret material (secrets[{index}])")


def assert_text_omits(value, fragments) -> None:
    """Fail without copying the inspected value into diagnostics.

    The sibling of :func:`assert_text_free_of` for an absence check over text that carries no
    protected material — a URL query parameter, a tool's own stderr. The property is the same:
    pytest must not rewrite the assertion and print the whole surface. The fragment IS named,
    because naming it discloses nothing and the test is unreadable without it.
    """
    rendered = str(value)
    for fragment in fragments:
        if fragment in rendered:
            raise AssertionError(f"text contains {fragment!r}")


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
        if secret in rendered:
            raise AssertionError(f"a log record repeats secret material (secrets[{index}])")
