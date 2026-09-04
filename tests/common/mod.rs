//! Shared test-only PKI fixtures: a self-signed CA plus a server and client leaf certificate,
//! generated in-memory with `rcgen` and optionally written to PEM files on disk for tests that
//! need file paths (e.g. `tls::build_server_config`).

#![allow(dead_code)]

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose,
};
use std::path::{Path, PathBuf};

/// An in-memory CA plus one server and one client leaf certificate, all PEM-encoded.
pub struct Pki {
    pub ca_cert_pem: String,
    pub server_cert_pem: String,
    pub server_key_pem: String,
    pub client_cert_pem: String,
    pub client_key_pem: String,
}

/// Paths to the PEM files written by [`write_pki`].
pub struct PkiPaths {
    pub ca: PathBuf,
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
}

fn make_ca() -> (Certificate, KeyPair) {
    let key = KeyPair::generate().expect("generate CA key");
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
    params
        .distinguished_name
        .push(DnType::CommonName, "dicom-router test CA");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let cert = params.self_signed(&key).expect("self-sign CA");
    (cert, key)
}

fn make_leaf(
    common_name: &str,
    san: &[&str],
    ca_cert: &Certificate,
    ca_key: &KeyPair,
) -> (Certificate, KeyPair) {
    let key = KeyPair::generate().expect("generate leaf key");
    let mut params = CertificateParams::new(san.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .expect("leaf params");
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    let cert = params
        .signed_by(&key, ca_cert, ca_key)
        .expect("sign leaf cert");
    (cert, key)
}

/// Generate a fresh CA + server + client PKI in memory.
pub fn generate_pki() -> Pki {
    let (ca_cert, ca_key) = make_ca();
    let (server_cert, server_key) = make_leaf(
        "dicom-router test server",
        &["localhost"],
        &ca_cert,
        &ca_key,
    );
    let (client_cert, client_key) = make_leaf("dicom-router test client", &[], &ca_cert, &ca_key);

    Pki {
        ca_cert_pem: ca_cert.pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
    }
}

/// Generate a fresh PKI and write it as PEM files under `dir`. `dir` must already exist.
pub fn write_pki(dir: &Path) -> PkiPaths {
    let pki = generate_pki();

    let ca = dir.join("ca.crt");
    let server_cert = dir.join("server.crt");
    let server_key = dir.join("server.key");
    let client_cert = dir.join("client.crt");
    let client_key = dir.join("client.key");

    std::fs::write(&ca, &pki.ca_cert_pem).expect("write ca.crt");
    std::fs::write(&server_cert, &pki.server_cert_pem).expect("write server.crt");
    std::fs::write(&server_key, &pki.server_key_pem).expect("write server.key");
    std::fs::write(&client_cert, &pki.client_cert_pem).expect("write client.crt");
    std::fs::write(&client_key, &pki.client_key_pem).expect("write client.key");

    PkiPaths {
        ca,
        server_cert,
        server_key,
        client_cert,
        client_key,
    }
}
