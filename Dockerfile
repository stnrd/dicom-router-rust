FROM rust:1-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd -r -u 10001 dicom && mkdir -p /var/lib/dicom-router && chown dicom /var/lib/dicom-router
COPY --from=build /app/target/release/dicom-router /usr/local/bin/dicom-router
USER dicom
EXPOSE 2762
VOLUME ["/var/lib/dicom-router"]
ENTRYPOINT ["dicom-router", "--config", "/etc/dicom-router/config.yaml"]
