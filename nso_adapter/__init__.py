# SPDX-License-Identifier: Apache-2.0
"""NSO Adapter package.

Single source for the version string — FastAPI(version=...) and the /healthz
payload both import it; a test pins it against pyproject.toml.
"""

__version__ = "0.4.0"
