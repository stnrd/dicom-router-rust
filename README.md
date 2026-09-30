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
- **Query/Retrieve proxy (optional):** when `query_retrieve` is set, inbound C-FIND and C-GET (Patient Root and Study Root) from the listed calling AE titles are forwarded to that destination. The router does not interpret `QueryRetrieveLevel`. C-GET sub-operations (C-STORE) come back on the router's outbound association and are relayed to the requestor. C-MOVE is not supported (it would require the destination to dial the hospital).
  - **Access:** only calling AE titles in `allowed_ae_titles` may query; others get status `0124` (not authorized). AE titles are not authentication, so combine this with inbound mTLS (`tls.client_ca`) when the network is not trusted.
  - **Transfer syntaxes:** identifiers are re-encoded when the requestor and destination negotiated different native encodings (e.g. implicit vs explicit VR). For C-GET the router offers the destination exactly the storage transfer syntaxes the requestor accepted, so images are relayed unchanged.
  - **Messages, not PDUs:** commands and datasets are reassembled and re-fragmented for each side's max PDU length; C-CANCEL from the requestor is forwarded.

## Configuration

YAML config (Kubernetes ConfigMap friendly). See [`config.example.yaml`](config.example.yaml).

Configuration is validated on every startup (`Config::load` → `validate()`). Invalid YAML or rule violations print `configuration error: …` and exit code 2 before the router binds or loads TLS.

```yaml
query_retrieve:
  destination: pacs-main          # must match destinations[].name
  allowed_ae_titles: ["VIEWER"]   # required; calling AE titles allowed to query/retrieve
```

```bash
dicom-router --config /path/to/config.yaml
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
