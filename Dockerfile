FROM python:3.12-slim AS builder
WORKDIR /build

RUN pip install uv --no-cache-dir

# uv.lock + --frozen: the image ships the exact dependency set the suite tested,
# instead of re-resolving at build time.
COPY pyproject.toml uv.lock ./
COPY nso_adapter/ nso_adapter/

RUN uv sync --no-dev --no-cache --frozen

FROM python:3.12-slim
WORKDIR /app

# Add NSO/Vault hosts to NO_PROXY at runtime via compose/.env, not baked into the image.
ENV PYTHONUNBUFFERED=1 \
    NO_PROXY="localhost,127.0.0.1" \
    no_proxy="localhost,127.0.0.1"

COPY --from=builder /build/.venv /app/.venv
COPY --from=builder /build/nso_adapter /app/nso_adapter

# Migrations run at container start (entrypoint) — ship the alembic tree + ini.
COPY alembic/ /app/alembic/
COPY alembic.ini /app/alembic.ini
COPY scripts/docker-entrypoint.sh /app/scripts/docker-entrypoint.sh
RUN chmod +x /app/scripts/docker-entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000
ENTRYPOINT ["/app/scripts/docker-entrypoint.sh"]
CMD ["uvicorn", "nso_adapter.main:app", "--host", "0.0.0.0", "--port", "8000"]
