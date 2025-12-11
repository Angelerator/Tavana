# Tavana Multi-stage Dockerfile
# Builds all services from a single Dockerfile

# =============================================================================
# Stage 1: Build
# =============================================================================
FROM rust:1.89-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    cmake \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY proto/ proto/
COPY crates/ crates/

# Build all binaries in release mode
RUN cargo build --release

# =============================================================================
# Stage 2: Runtime base
# =============================================================================
FROM debian:bookworm-slim AS runtime-base

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 -s /bin/bash tavana

# =============================================================================
# Stage 3: Gateway
# =============================================================================
FROM runtime-base AS gateway

COPY --from=builder /app/target/release/tavana-gateway /usr/local/bin/

USER tavana
WORKDIR /home/tavana

EXPOSE 5432 8815 8080

ENTRYPOINT ["tavana-gateway"]

# =============================================================================
# Stage 4: Operator
# =============================================================================
FROM runtime-base AS operator

COPY --from=builder /app/target/release/tavana-operator /usr/local/bin/

USER tavana
WORKDIR /home/tavana

EXPOSE 8080

ENTRYPOINT ["tavana-operator"]

# =============================================================================
# Stage 5: Worker
# =============================================================================
FROM runtime-base AS worker

COPY --from=builder /app/target/release/tavana-worker /usr/local/bin/

USER tavana
WORKDIR /home/tavana

EXPOSE 50053

ENTRYPOINT ["tavana-worker"]

# =============================================================================
# Stage 6: Catalog
# =============================================================================
FROM runtime-base AS catalog

COPY --from=builder /app/target/release/tavana-catalog /usr/local/bin/

USER tavana
WORKDIR /home/tavana

EXPOSE 50052 8080

ENTRYPOINT ["tavana-catalog"]

# =============================================================================
# Stage 7: Metering
# =============================================================================
FROM runtime-base AS metering

COPY --from=builder /app/target/release/tavana-metering /usr/local/bin/

USER tavana
WORKDIR /home/tavana

EXPOSE 50054 9090

ENTRYPOINT ["tavana-metering"]

