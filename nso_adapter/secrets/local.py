# SPDX-License-Identifier: Apache-2.0
"""Local secrets provider — reads from env vars or Docker secret files.

The *reference* passed to ``get()`` is the environment-variable name
(or the ``<REFERENCE>_FILE`` path variant).  Config files should use
UPPER_SNAKE_CASE references that match env var names directly.
"""

from __future__ import annotations

import os
from pathlib import Path


class LocalSecretsProvider:
    """Resolves secret references as environment-variable names.

    For a reference ``"MY_TOKEN"``:
    1. Returns ``os.environ["MY_TOKEN"]`` if set.
    2. Returns the content of ``os.environ["MY_TOKEN_FILE"]`` if set and the file exists.
    3. Raises ``KeyError`` otherwise.
    """

    def get(self, reference: str) -> str:
        # `is not None`, not truthiness: an intentionally empty secret ("") is a set value,
        # not "unset" — returning it here avoids a confusing fall-through to a KeyError.
        val = os.environ.get(reference)
        if val is not None:
            return val
        file_path = os.environ.get(f"{reference}_FILE")
        if file_path and Path(file_path).exists():
            return Path(file_path).read_text().strip()
        raise KeyError(f"Secret reference {reference!r} not found in environment")
