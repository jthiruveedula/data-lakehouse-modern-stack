FROM python:3.11-slim AS base

# System deps for PySpark / JVM
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app

# Install Python dependencies in a layer-cached step
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Development stage ──────────────────────────────────────────────────────
FROM base AS dev
COPY . .
ENV LAKEHOUSE_ENV=development
CMD ["bash"]

# ── Production API stage ───────────────────────────────────────────────────
FROM base AS api
COPY src/ src/
COPY pyproject.toml .

RUN pip install --no-cache-dir -e .

ENV LAKEHOUSE_ENV=production
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["uvicorn", "src.semantic_search.api:app", \
     "--host", "0.0.0.0", "--port", "8080", \
     "--workers", "2", "--access-log"]
