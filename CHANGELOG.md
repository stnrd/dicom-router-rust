# Changelog

## [0.2.0](https://github.com/stnrd/dicom-router-rust/compare/v0.1.0...v0.2.0) (2026-10-15)


### Features

* DIMSE command builders/parsers (C-STORE, C-ECHO) ([f9a7684](https://github.com/stnrd/dicom-router-rust/commit/f9a76843dc945775e044d221b1ed4d82ef6a33e3))
* durable spool queue and exponential backoff retry ([2319c3a](https://github.com/stnrd/dicom-router-rust/commit/2319c3a6c70719f76d784a0b10400b5d6201db67))
* initial DICOM router implementation ([6d0b86d](https://github.com/stnrd/dicom-router-rust/commit/6d0b86d7f2f4586ce899178d90a350f2f40d17db))
* optional cleartext DICOM on inbound and outbound ([5152bc1](https://github.com/stnrd/dicom-router-rust/commit/5152bc1b5934c0655065932c71095d70cfa34a26))
* proxy C-FIND/C-GET to a configured destination ([83a74b9](https://github.com/stnrd/dicom-router-rust/commit/83a74b93a7b85f73100cc7671098dcd3bddbbe06))
* reuse outbound DICOM associations per destination ([d707593](https://github.com/stnrd/dicom-router-rust/commit/d707593e9cc468ba1c062352c2e9b6ad1cc1cc4b))
* rustls server/client config builders with optional mTLS ([b640810](https://github.com/stnrd/dicom-router-rust/commit/b640810c0897e75a28eaac01ecc63cb74a657c7b))
* slog-style JSON logging with msg field and dep log bridges ([0518ab9](https://github.com/stnrd/dicom-router-rust/commit/0518ab9a84bfde7cc2d4be1ea981d72f024d934f))
* TLS SCP/SCU router pipeline with dispatcher and E2E test ([8335490](https://github.com/stnrd/dicom-router-rust/commit/8335490fbf4aeff5879f80dde6c9c3ecc6c75d6b))
* validate config on every startup and drop --check flag ([03c6f44](https://github.com/stnrd/dicom-router-rust/commit/03c6f44e7ec272a3fcb7e47282f2b780b66d2f34))
* YAML config with N destinations and validation ([84e7750](https://github.com/stnrd/dicom-router-rust/commit/84e77504c35abdc6e91fb2b1b558a9d36b8bd528))


### Bug Fixes

* atomic multi-destination fan-out spool with rollback ([2320b2d](https://github.com/stnrd/dicom-router-rust/commit/2320b2d9affcec1083d7fd08bd8f254c5ea77b1e))
* logging review — guard semantics, full level names, idempotent bridge, drain resilience ([5c216da](https://github.com/stnrd/dicom-router-rust/commit/5c216dad7e4a20c487007ea54ada3b7b2d5caf9f))
* mark delivered in sidecar before acknowledge to prevent duplicate forwards ([59798ff](https://github.com/stnrd/dicom-router-rust/commit/59798ff337f7c6472397d49e40d671cd412429a7))
* offload spool queue I/O to blocking thread pool ([680a4f9](https://github.com/stnrd/dicom-router-rust/commit/680a4f90b2e8aba3ad80a0f4f23ea7cf576167a7))
* prevent queue scan from racing with in-flight enqueues ([de185f6](https://github.com/stnrd/dicom-router-rust/commit/de185f6c8da59fe04ce51248eb8940beef839e5b))
* replace tracing-slog with tracing log feature (correct bridge direction) ([1186df2](https://github.com/stnrd/dicom-router-rust/commit/1186df253ac4ed56b5025917e8fb03b58a795d87))
