FROM python:3.12-slim AS builder
WORKDIR /build

RUN pip install uv --no-cache-dir

COPY pyproject.toml .
COPY nso_adapter/ nso_adapter/

RUN uv sync --no-dev --no-cache

FROM python:3.12-slim
WORKDIR /app

# Add NSO/Vault hosts to NO_PROXY at runtime via compose/.env, not baked into the image.
ENV PYTHONUNBUFFERED=1 \
    NO_PROXY="localhost,127.0.0.1" \
    no_proxy="localhost,127.0.0.1"

COPY --from=builder /build/.venv /app/.venv
COPY --from=builder /build/nso_adapter /app/nso_adapter

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000
CMD ["uvicorn", "nso_adapter.main:app", "--host", "0.0.0.0", "--port", "8000"]
