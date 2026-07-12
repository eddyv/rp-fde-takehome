# Built from the repo root (workspace root): context is ".", dockerfile is service/Dockerfile.
FROM python:3.13-slim

RUN pip install --no-cache-dir uv==0.11.16

WORKDIR /app

# Third-party deps first (cached layer): lockfile + all workspace pyprojects,
# --no-install-workspace defers installing our own packages until code is copied.
COPY pyproject.toml uv.lock ./
COPY service/pyproject.toml service/pyproject.toml
RUN uv sync --frozen --no-dev --no-install-workspace --package reasoning-service

COPY service/app service/app
RUN uv sync --frozen --no-dev --package reasoning-service

ENV PATH="/app/.venv/bin:$PATH"
CMD ["python", "-m", "app.worker"]
