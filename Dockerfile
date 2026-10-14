# syntax=docker/dockerfile:1

# ---- Build stage ------------------------------------------------------------
# Compile a release binary. Only the final binary is copied into the runtime
# image, so the Rust toolchain and sources never ship.
FROM rust:1.98-bookworm AS build
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Cache mounts keep downloaded crates and compiled dependencies between builds,
# so a source change does not rebuild every dependency. target/ only exists
# inside the cache mount, so copy the binary out in the same step.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked \
 && cp target/release/dicom-router /usr/local/bin/dicom-router

# ---- Runtime stage ----------------------------------------------------------
FROM debian:bookworm-slim

# Run as an unprivileged user. /var/lib/dicom-router holds the spool queue and
# dead letters and must be writable by that user.
RUN useradd --system --uid 10001 --no-create-home dicom \
 && mkdir -p /var/lib/dicom-router \
 && chown dicom /var/lib/dicom-router

COPY --from=build /usr/local/bin/dicom-router /usr/local/bin/dicom-router

USER dicom

# Documents the default DICOM TLS port (listen_addr in config.example.yaml).
# EXPOSE does not publish anything; use `docker run -p` or a Kubernetes
# Service. The router listens on whatever listen_addr is set to. Ports below
# 1024 (e.g. 104) work under Docker's defaults but may need
# NET_BIND_SERVICE elsewhere, because the router does not run as root.
EXPOSE 2762

# Queued images live here. Mount persistent storage, otherwise anything still
# queued is lost when the container is replaced.
VOLUME ["/var/lib/dicom-router"]

# Mount the YAML config at this path (e.g. a Kubernetes ConfigMap). TLS
# certificates and keys are read from the paths given in the config.
ENTRYPOINT ["dicom-router", "--config", "/etc/dicom-router/config.yaml"]
