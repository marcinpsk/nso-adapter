#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Container entrypoint: apply DB migrations to head, then run the app.
# `alembic upgrade head` is a no-op when the DB is already current (e.g. stamped
# dev DB), and builds the schema from the baseline on a fresh DB. DATABASE_URL is
# read from the environment by alembic/env.py.
set -e

python -m nso_adapter.db_migrate

exec "$@"
