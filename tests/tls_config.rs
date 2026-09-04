//! Integration tests for `dicom_router::tls`: building rustls configs from PEM files on disk.

mod common;

use dicom_router::tls::{build_client_config, build_server_config};

#[test]
fn builds_server_and_client_configs_from_pem_files_with_mtls() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pki = common::write_pki(dir.path());

    // Server requires mTLS: client_ca is set to the shared CA bundle.
    let _server_config = build_server_config(&pki.server_cert, &pki.server_key, Some(&pki.ca))
        .expect("server config with mTLS should build from valid PEM files");

    // Server without mTLS (no client_ca) also builds.
    let _server_config_no_mtls = build_server_config(&pki.server_cert, &pki.server_key, None)
        .expect("server config without mTLS should build from valid PEM files");

    // Client trusts the CA and presents its own certificate for mTLS.
    let client_config = build_client_config(&pki.ca, Some(&pki.client_cert), Some(&pki.client_key))
        .expect("client config should build from valid PEM files");
    assert!(client_config.client_auth_cert_resolver.has_certs());

    // Client without mTLS material still builds (server-only verification).
    let client_config_no_mtls =
        build_client_config(&pki.ca, None, None).expect("client config without mTLS material");
    assert!(!client_config_no_mtls.client_auth_cert_resolver.has_certs());
}

#[test]
fn rejects_garbage_private_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pki = common::write_pki(dir.path());

    let garbage_key = dir.path().join("garbage.key");
    std::fs::write(
        &garbage_key,
        "-----BEGIN PRIVATE KEY-----\nbm90IGEgcmVhbCBrZXk=\n-----END PRIVATE KEY-----\n",
    )
    .expect("write garbage key");

    let result = build_server_config(&pki.server_cert, &garbage_key, None);
    assert!(result.is_err(), "garbage key must be rejected");
}
