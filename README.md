# DICOM Router (Rust)

DICOM router supporting reliable image forwarding to one or more PACS, query/retrieve for viewers, and secure transport over DICOM TLS.

```
                         ┌──────────────── dicom-router ────────────────┐
 Modality ──C-STORE────► │ SCP ──► disk queue (per destination) ──► SCU │ ──► PACS A
                         │                                         └──► │ ──► PACS B
 Viewer ──C-FIND/C-GET─► │ Q/R proxy ─────────────────────────────────► │ ──► PACS A
                         └──────────────────────────────────────────────┘
```

## Features

| | |
|---|---|
| **Store and forward** | Receives C-STORE and forwards to every configured destination (fan-out), or only to destinations that match the sender's AE title. |
| **No lost images** | The modality gets "success" only after the image is written and fsynced to the queue of *every* matching destination. If one write fails, all are rolled back and the modality gets an error, so it can resend. |
| **Retries and dead letters** | Failed sends retry with exponential backoff. After `max_attempts` the object moves to a dead-letter directory for inspection. |
| **Survives restarts** | The queue lives on disk. After a crash or restart, pending objects are sent. |
| **TLS by default** | DICOM TLS on both sides, with optional mutual TLS. Cleartext can be enabled per direction for labs or legacy systems (e.g. Orthanc on 4242). |
| **Query/Retrieve proxy** | Optional. Viewers can C-FIND and C-GET (Patient Root and Study Root) from one destination through the router. Only listed AE titles are allowed. |
| **C-ECHO** | Answers verification requests. |
| **Ops-friendly** | One static binary or container, YAML config (works as a Kubernetes ConfigMap), JSON logs, graceful shutdown on SIGTERM, refuses new images when the disk is nearly full. |

Not supported: C-MOVE (it requires the PACS to connect back to the caller), worklist, and storage commitment.

## Install

**Container image** (Linux `amd64` and `arm64`), from the GitHub Container Registry:

```bash
docker pull ghcr.io/stnrd/dicom-router-rust:latest
```

Use a version tag such as `0.2.0` to pin a release. Mount your config at `/etc/dicom-router/config.yaml` and publish the port you set in `listen_addr`:

```bash
docker run -d --name dicom-router -p 2762:2762 \
  -v "$PWD/config.yaml:/etc/dicom-router/config.yaml:ro" \
  -v dicom-router-data:/var/lib/dicom-router \
  ghcr.io/stnrd/dicom-router-rust:latest
```

**Binary**: static Linux builds (`x86_64` and `aarch64`, no dependencies) are attached to each [GitHub release](https://github.com/stnrd/dicom-router-rust/releases/latest), together with `SHA256SUMS`:

```bash
tar xzf dicom-router-<version>-x86_64-unknown-linux-musl.tar.gz
cd dicom-router-<version>-x86_64-unknown-linux-musl
./dicom-router --config config.example.yaml   # edit the config and its certificate paths first
```

The archive contains the `dicom-router` binary, `config.example.yaml`, `README.md` and `LICENSE`.

**From source**: see [Development](#development).

## Quick start

Try it locally against [Orthanc](https://www.orthanc-server.com/), all in cleartext. You need Docker and the [DCMTK](https://dicom.offis.de/dcmtk) tools (`storescu`, `findscu`, `getscu`).

**1. Start Orthanc and the router**

```bash
docker network create dicom
docker run -d --name orthanc --network dicom -p 8042:8042 \
  -e ORTHANC__AUTHENTICATION_ENABLED=false \
  -e ORTHANC__DICOM_MODALITIES='{"router": ["ROUTER", "router", 11112]}' \
  orthancteam/orthanc

```

The two Orthanc settings are for local testing only. They turn off the web UI login and register the router, so Orthanc accepts its queries and retrievals.

Save this as `quickstart.yaml`:

```yaml
listen_addr: "0.0.0.0:11112"
ae_title: "ROUTER"
queue_dir: "/var/lib/dicom-router/queue"
dead_letter_dir: "/var/lib/dicom-router/dead-letter"

tls:
  enabled: false            # cleartext inbound

destinations:
  - name: orthanc
    ae_title: "ORTHANC"
    host: "orthanc"
    port: 4242
    tls: false              # cleartext outbound

query_retrieve:
  destination: orthanc
  allowed_ae_titles: ["VIEWER"]
```

```bash
docker run -d --name router --network dicom -p 11112:11112 \
  -v "$PWD/quickstart.yaml:/etc/dicom-router/config.yaml:ro" \
  ghcr.io/stnrd/dicom-router-rust:latest
```

**2. Send, query, retrieve**

```bash
echoscu  -aet VIEWER -aec ROUTER localhost 11112                     # C-ECHO
storescu -aet CT1    -aec ROUTER localhost 11112 path/to/*.dcm       # C-STORE → Orthanc
findscu  -aet VIEWER -aec ROUTER -S -k QueryRetrieveLevel=STUDY \
         -k PatientName -k StudyInstanceUID localhost 11112          # C-FIND via router
getscu   -aet VIEWER -aec ROUTER -S -k QueryRetrieveLevel=STUDY \
         -k StudyInstanceUID=<uid> -od out/ localhost 11112          # C-GET via router
```

Open <http://localhost:8042> to see the images in Orthanc, and `docker logs -f router` to follow the router.

## Configuration

The router reads one YAML file, by default `/etc/dicom-router/config.yaml`:

```bash
dicom-router --config /path/to/config.yaml
```

The config is checked at startup. If it is invalid, the router prints `configuration error: …` and exits with code 2 before it opens any ports. [`config.example.yaml`](config.example.yaml) shows a production-style setup with TLS.

### General

| Key | Default | Description |
|---|---|---|
| `listen_addr` | *required* | Address to listen on, e.g. `0.0.0.0:2762` (TLS) or `0.0.0.0:104` (cleartext). |
| `ae_title` | *required* | The router's AE title, 1–16 characters. Also used as calling AE when forwarding. |
| `queue_dir` | *required* | Spool directory. Each destination gets a subdirectory. |
| `dead_letter_dir` | `/var/lib/dicom-router/dead-letter` | Where objects go after all retries fail. |
| `max_pdu_length` | `131072` | Maximum PDU size, at least 1018. |
| `max_concurrent_associations` | `32` | Inbound associations at once; extra connections are dropped. |
| `max_concurrent_sends` | `4` | Outbound sends at once, per destination. |
| `min_free_bytes` | `536870912` (512 MiB) | Refuse new images (status `A700`) when the queue disk has less free space than this. |
| `promiscuous` | `false` | Accept any storage SOP class. When `false`, accepts CT, MR, CR, DX, US, NM, PET and Secondary Capture. |
| `log_level` | `info` | `critical`, `error`, `warn`, `info`, `debug` or `trace`. |

### `retry`

| Key | Default | Description |
|---|---|---|
| `initial_delay_ms` | `1000` | Delay before the first retry. |
| `multiplier` | `2.0` | Each retry waits this much longer than the previous one. |
| `max_delay_ms` | `60000` | Upper limit for the delay. |
| `max_attempts` | `10` | After this many failed attempts the object is dead-lettered. |

### `tls` (inbound)

| Key | Default | Description |
|---|---|---|
| `enabled` | `true` | `false` accepts cleartext DICOM. The router logs a warning at startup. |
| `server_cert`, `server_key` | — | PEM certificate chain and key. Required when `enabled`. |
| `client_ca` | — | When set, clients must present a certificate signed by this CA (mutual TLS). |

### `destinations[]`

| Key | Default | Description |
|---|---|---|
| `name` | *required* | Unique name, used for the queue directory and in logs. |
| `ae_title` | *required* | Called AE title of the destination. |
| `host`, `port` | *required* | Address of the destination. |
| `tls` | `true` | `false` sends cleartext DICOM. |
| `ca_cert` | — | CA used to verify the destination. Required when `tls` is `true`. |
| `server_name` | `host` | Name to verify in the destination's certificate (SNI). |
| `client_cert`, `client_key` | — | Client certificate for destinations that require mutual TLS. Set both or neither. |
| `source_ae_titles` | `[]` (all) | Only forward images from these calling AE titles. Empty means everything. |

### `query_retrieve` (optional)

Leave this block out and the router only handles C-STORE and C-ECHO.

| Key | Description |
|---|---|
| `destination` | `destinations[].name` that C-FIND and C-GET are sent to. Uses that destination's TLS settings. |
| `allowed_ae_titles` | Calling AE titles that may query and retrieve. Required. Others get status `0124` (not authorized). |

AE titles are not authentication. If the network is not trusted, combine `allowed_ae_titles` with inbound mutual TLS (`tls.client_ca`).

## How it works

**Storing.** When an image arrives, the router finds the destinations that should get it (all, or those whose `source_ae_titles` match), writes a copy into each destination's queue directory, fsyncs it, and only then replies success. Every object is a `.dcm` file plus a `.yaml` sidecar that tracks attempts and the last error.

**Forwarding.** Each destination has its own dispatcher. It watches its queue directory (and rescans every 5 seconds), sends objects over a reused association, and deletes them once the PACS confirms. A send that fails is retried with backoff; after `max_attempts` it moves to `dead_letter_dir`. Destinations are independent: a PACS that is down does not hold up the others.

**Query/Retrieve.** For each C-FIND or C-GET the router opens an association to the Q/R destination and relays the messages in both directions. It does not cache or change the query. C-GET images come back over the same association and are passed straight to the viewer, in a transfer syntax the viewer accepted. C-CANCEL is forwarded. Different PDU sizes and implicit/explicit VR on each side are handled.

**Shutdown.** On SIGTERM or Ctrl-C the router stops accepting connections, finishes work in progress and exits. Anything still in the queue is sent on the next start.

## Operations

- **Logs** are JSON lines on stdout with `msg`, `level`, `ts` (RFC 3339, UTC) and extra fields such as `destination`, `calling_ae` and `sop_instance_uid`.
- **Queue depth**: count `.dcm` files in `<queue_dir>/<destination>/`.
- **Dead letters**: look in `dead_letter_dir`. The `.yaml` sidecar has the last error.
- **Resend a dead letter**: move both the `.dcm` and its `.yaml` back into `<queue_dir>/<destination>/`. Reset `attempts` in the sidecar to get a full set of retries again.
- **Container**: runs as UID 10001, exposes 2762, and keeps its data in the `/var/lib/dicom-router` volume. Mount that volume on persistent storage, or queued images are lost when the container is replaced.

## Development

You need Rust 1.98 (see [`rust-toolchain.toml`](rust-toolchain.toml)) and optionally [Task](https://taskfile.dev).

```bash
task build          # cargo build
task test           # cargo test
task lint           # nightly rustfmt + clippy -D warnings
```

> [`.cargo/config.toml`](.cargo/config.toml) links with `clang` and `mold`. Install them (`apt install clang mold`), or build with `docker build .` instead.

For TLS testing, `scripts/gen-dev-certs.sh dev-certs` creates a throwaway CA plus server and client certificates (`ca.crt`, `server.crt/.key`, `client.crt/.key`). Point `config.example.yaml` at them and run `cargo run -- --config config.example.yaml`.

The integration tests in [`tests/`](tests) run the whole router in-process against in-process destination SCPs. They cover TLS and cleartext, fan-out, multi-image series, and the Q/R proxy.

**CI**: GitHub Actions runs `task lint` and `task test` on every branch push and pull request. See [`.github/workflows`](.github/workflows).

`scripts/smoke-test.sh <image>` runs the router image against Orthanc and checks C-ECHO, C-STORE, C-FIND and C-GET end to end. It needs Docker and DCMTK.

## License

[MIT](LICENSE)
