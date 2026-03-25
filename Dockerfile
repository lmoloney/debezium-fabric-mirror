FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first (layer caching)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy source
COPY README.md ./
COPY src/ src/
COPY table_config.json ./
RUN uv sync --frozen --no-dev

# Run as non-root
RUN useradd --create-home --uid 1000 omd
USER omd

CMD ["uv", "run", "omd-consumer"]
