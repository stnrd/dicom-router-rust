# DICOM Router (Rust)

Production-grade, TLS-only DICOM C-STORE router. Receives DICOM over TLS, durably spools to disk, and forwards to one or more destinations over verified TLS.

## Architecture

```
Modality (TLS) → Router SCP → spool queue → Dispatcher → Router SCU (TLS) → PACS
```

- **Inbound:** TLS SCP (port 2762 by default). Optional inbound mTLS via `tls.client_ca`.
- **Outbound:** Always TLS with CA verification. Optional client cert per destination.
- **Durability:** C-STORE-RSP success is sent only after atomic spool to disk.
- **Routing:** Fan-out to all destinations; optional `source_ae_titles` filter per destination.
- **Atomic fan-out:** C-STORE-RSP success is sent only after the object is durably spooled to *all* matching destination queues; a failure on any destination rolls back the others.

## Configuration

YAML config (Kubernetes ConfigMap friendly). See [`config.example.yaml`](config.example.yaml).

```bash
dicom-router --config /path/to/config.yaml --check
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

GitLab CI runs `task lint` on merge requests and tagged releases. Image publishing uses Kaniko on version tags (see [`.gitlab-ci.yml`](.gitlab-ci.yml)).

## Operations

- **Queue depth:** count files in `<queue_dir>/<destination>/`.
- **Dead letters:** objects in `dead_letter_dir` after `retry.max_attempts` exhausted.
- **Reprocess:** move `.dcm` + `.yaml` from dead-letter back into the destination queue dir.

## License

Internal / project-specific.
