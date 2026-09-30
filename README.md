# DICOM Router (Rust)

Production-grade DICOM C-STORE router. Durable spool-and-forward with TLS on by default; cleartext DICOM can be enabled per direction for lab or legacy PACS (e.g. Orthanc on port 4242).

## Architecture

```
Modality (TLS) → Router SCP → spool queue → Dispatcher → Router SCU (TLS) → PACS
```

- **Inbound:** TLS SCP by default (port 2762). Set `tls.enabled: false` for cleartext (port 104). Optional inbound mTLS via `tls.client_ca`.
- **Outbound:** TLS with CA verification by default (`destinations[].tls: true`). Set `tls: false` for cleartext destinations. Optional client cert per TLS destination.
- **Durability:** C-STORE-RSP success is sent only after atomic spool to disk.
- **Routing:** Fan-out to all destinations; optional `source_ae_titles` filter per destination.
- **Atomic fan-out:** C-STORE-RSP success is sent only after the object is durably spooled to *all* matching destination queues; a failure on any destination rolls back the others.

## Configuration

YAML config (Kubernetes ConfigMap friendly). See [`config.example.yaml`](config.example.yaml).

Configuration is validated on every startup (`Config::load` → `validate()`). Invalid YAML or rule violations print `configuration error: …` and exit code 2 before the router binds or loads TLS.

```bash
dicom-router --config /path/to/config.yaml
```

## Compression

Per destination, `compression` controls the transfer syntax used on the wire:

| Value | Behaviour |
|---|---|
| `none` (default) | Forward objects exactly as received. |
| `jpeg-xl-lossless` | Re-encode uncompressed little-endian images (8/16-bit, MONOCHROME1/2, PALETTE COLOR, interleaved RGB) as JPEG XL Lossless. Everything else is forwarded unchanged. |
| `explicit-le` | Decode compressed pixel data to Explicit VR Little Endian. Use on a router in front of a PACS that should not receive JPEG XL. |

The router proposes both the target and the original transfer syntax. It transcodes only when the destination accepts the target, and sends the original if transcoding fails. Every JPEG XL encode is decoded again and compared byte-for-byte with the original pixel data before it is sent.

Typical setup with a router on each side of the link:

```
Modality → router A (compression: jpeg-xl-lossless) → router B (compression: explicit-le) → PACS
```

`object forwarded` log lines include `transfer_syntax` and `bytes` (dataset size on the wire). To measure compression on your own data:

```bash
cargo run --release --example compression_bench -- path/to/*.dcm
```

## Logging

JSON lines to stdout, Go `slog`-style fields: `msg`, `level` (`WARN`, `ERROR`, …), `ts` (RFC3339 UTC), plus key-values.

## Local development

```bash
./scripts/gen-dev-certs.sh dev-certs
# edit config to point at dev-certs/*.crt and *.key
cargo run -- --config config.example.yaml
```

## Tests

```bash
task test
# or: cargo test
```

Integration coverage is in-process: `tests/loopback.rs` runs the router (SCP + dispatcher + SCU) in the test process and uses an in-process TLS destination SCP as a stand-in for a PACS.

A future `docker-tests` feature could start external destinations (e.g. Orthanc) via testcontainers; stock Orthanc speaks cleartext DICOM by default while this router always forwards over TLS, so that needs extra certificate wiring first.

## CI

GitHub Actions runs `task lint` and `task test` on pull requests and pushes to all branches.

Version tags (`v1.2.3`, `v1.2.3-rc1`) publish a Docker image to [GHCR](https://github.com/features/packages) (`ghcr.io/<owner>/dicom-router-rust`).

See [`.github/workflows/ci.yml`](.github/workflows/ci.yml) and [`.github/workflows/release.yml`](.github/workflows/release.yml).

## Operations

- **Queue depth:** count files in `<queue_dir>/<destination>/`.
- **Dead letters:** objects in `dead_letter_dir` after `retry.max_attempts` exhausted.
- **Reprocess:** move `.dcm` + `.yaml` from dead-letter back into the destination queue dir.

## License

Internal / project-specific.
