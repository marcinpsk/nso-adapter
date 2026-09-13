# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Run the credential guard in the suite and test the scanner with real source."""

from __future__ import annotations

import pytest

from tests.credential_discipline import (
    _counts_by_site,
    load_baseline,
    save_baseline,
    scan_source,
    scan_tree,
    unapproved,
)


def test_no_unapproved_credentials_beyond_baseline():
    bad = unapproved()
    assert not bad, (
        "Unapproved credential literal(s):\n"
        + "\n".join(f"  {v}" for v in bad)
        + "\nUse neutral placeholders or an inline '# credential-ok: <reason>' comment."
    )


@pytest.mark.parametrize(
    "statement",
    [
        'username = "admin"',
        'NSO_PASSWORD = "ADMIN"',
        'client.secret = "AdMiN"',
        'token: str = "admin"',
        'username, password = "admin", "placeholder"',
        'config["password"] = "admin"',
        'config = {"username": "admin"}',
        'client(username="admin")',
        'client(api_token="admin")',
        'NsoClient(config, "admin", "placeholder")',
        'subscriber(config, ("admin", "placeholder"))',
        'client(auth=("admin", "placeholder"))',
        'monkeypatch.setenv("NSO_USERNAME", "admin")',
        'def connect(password="admin"): pass',
        'def connect(*, token="admin"): pass',
    ],
)
def test_flags_new_credential_literals(statement):
    hits = scan_source(statement, "t.py")
    assert len(hits) == 1
    assert hits[0].path == "t.py"
    assert hits[0].lineno == 1


@pytest.mark.parametrize(
    "statement",
    [
        'lambda password="admin": None',
        'lambda *, token="admin": None',
    ],
)
def test_flags_credential_literals_in_lambda_defaults(statement):
    assert len(scan_source(statement, "t.py")) == 1


@pytest.mark.parametrize(
    "statement",
    [
        'password = f"admin"',
        'password = "ad" + "min"',
    ],
)
def test_flags_constant_credential_string_expressions(statement):
    assert len(scan_source(statement, "t.py")) == 1


@pytest.mark.parametrize(
    "source",
    [
        'role = "admin"',
        'client(role="admin")',
        'config = {"role": "admin"}',
        'assert by_name["admin"].enabled',
        'assert username == "admin"',
        'username = "placeholder-user"',
        'username_ref = "admin"',
        'config = {"password_ref": "admin"}',
        '"""username = admin"""',
        '# username = "admin"',
    ],
)
def test_accepts_legitimate_noncredential_uses(source):
    assert scan_source(source) == []


@pytest.mark.parametrize(
    "source",
    [
        'username = "admin"  # credential-ok: tests a rejected account\n',
        '# credential-ok: tests a rejected account\n# deliberate input\nusername = "admin"\n',
        'client(  # credential-ok: tests a rejected account\n    username="admin",\n)\n',
    ],
)
def test_accepts_comment_marker(source):
    assert scan_source(source) == []


@pytest.mark.parametrize(
    "source",
    [
        'label = "credential-ok: reason"\nusername = "admin"\n',
        '# credential-ok: reason\n\nusername = "admin"\n',
        'username = "admin"  # credential-ok:\n',
        'role = "admin"  # credential-ok: role\nusername = "admin"\n',
    ],
)
def test_marker_requires_a_reason_and_belongs_to_its_statement(source):
    assert len(scan_source(source)) == 1


def test_counts_by_lexical_scope():
    source = (
        'username = "admin"\n'
        "class Example:\n"
        "    async def connect(self):\n"
        '        client("admin", "ADMIN")\n'
        "        def nested():\n"
        '            password = "admin"\n'
    )
    assert _counts_by_site(scan_source(source, "t.py")) == {
        "t.py::<module>": 1,
        "t.py::Example.connect": 2,
        "t.py::Example.connect.nested": 1,
    }


def test_baseline_allows_existing_but_flags_excess_and_new_scope(tmp_path):
    path = tmp_path / "test_example.py"
    path.write_text('def existing():\n    client("admin", "admin")\n', encoding="utf-8")
    baseline = {"test_example.py::existing": 2}
    assert unapproved(tmp_path, baseline) == []
    path.write_text(
        'def existing():\n    client("admin", "admin")\n    token = "admin"\ndef new():\n    username = "admin"\n',
        encoding="utf-8",
    )
    hits = unapproved(tmp_path, baseline)
    assert [(v.lineno, v.qualname) for v in hits] == [(3, "existing"), (5, "new")]


def test_baseline_round_trip(tmp_path):
    path = tmp_path / "baseline.txt"
    assert load_baseline(path) == {}
    counts = {"t.py::second": 2, "t.py::first": 1}
    save_baseline(counts, path)
    assert load_baseline(path) == counts
    assert path.read_text().index("t.py::first") < path.read_text().index("t.py::second")


def test_scan_tree_skips_only_its_own_files(tmp_path):
    for name in ("credential_discipline.py", "test_credential_discipline.py", "test_example.py"):
        (tmp_path / name).write_text('username = "admin"\n', encoding="utf-8")
    nested = tmp_path / "nested"
    nested.mkdir()
    for name in ("credential_discipline.py", "test_credential_discipline.py"):
        (nested / name).write_text('username = "admin"\n', encoding="utf-8")
    assert [v.path for v in scan_tree(tmp_path)] == [
        "nested/credential_discipline.py",
        "nested/test_credential_discipline.py",
        "test_example.py",
    ]


def test_importer_regressions_have_no_baseline_allowance():
    baseline = load_baseline()
    assert not any(site.startswith("core/test_importer.py::") for site in baseline)
