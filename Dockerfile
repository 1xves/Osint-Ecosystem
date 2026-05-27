# ─── OSINT System — Multi-stage Dockerfile ────────────────────────────────────
#
# Builds a single image used by both the API service and the ARQ worker.
# Service selection is via CMD override in docker-compose.yml:
#   API:    uvicorn osint.api.app:app --host 0.0.0.0 --port 8000
#   Worker: python -m arq osint.workers.worker.WorkerSettings
#
# Build: docker build -t osint:latest .
# ──────────────────────────────────────────────────────────────────────────────

# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# System packages needed to compile native extensions (psycopg2, lxml, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps into a prefix so we can copy them cleanly to the runtime stage
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# Runtime system libs — no build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    libxml2 \
    libxslt1.1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY . .

# Non-root user for security
RUN useradd --system --no-create-home --shell /bin/false osint \
    && chown -R osint:osint /app
USER osint

# Expose API port (worker doesn't need it but harmless)
EXPOSE 8000

# Health check — API mode only; worker ignores this
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default: API server. Override in docker-compose.yml for worker.
CMD ["uvicorn", "osint.api.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
