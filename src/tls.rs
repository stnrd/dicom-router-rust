//! rustls configuration builders (rustls 0.23 API, matching dicom-ul 0.10).
//!
//! Inbound (server) TLS optionally requires mutual TLS: when a client CA bundle is supplied,
//! [`build_server_config`] wires up a [`WebPkiClientVerifier`] so unauthenticated clients are
//! rejected at the handshake. Outbound (client) TLS always verifies the destination's server
//! certificate against a CA bundle and optionally presents a client certificate/key for
//! destinations that require mTLS.

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{VerifierBuilderError, WebPkiClientVerifier};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use snafu::{ResultExt, Snafu};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum TlsError {
    #[snafu(display("could not open {}: {source}", path.display()))]
    OpenFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("could not read certificate(s) from {}: {source}", path.display()))]
    ReadCert {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("no certificates found in {}", path.display()))]
    NoCertificates { path: PathBuf },
    #[snafu(display("could not read private key from {}: {source}", path.display()))]
    ReadKey {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("no private key found in {}", path.display()))]
    NoPrivateKey { path: PathBuf },
    #[snafu(display("invalid certificate or key material: {source}"))]
    Rustls { source: rustls::Error },
    #[snafu(display("could not build client certificate verifier: {source}"))]
    ClientVerifier { source: VerifierBuilderError },
}

/// Load a PEM certificate chain from `path`.
fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let file = File::open(path).context(OpenFileSnafu { path })?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .context(ReadCertSnafu { path })?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificates {
            path: path.to_path_buf(),
        });
    }
    Ok(certs)
}

/// Load a single PEM private key (PKCS#1, PKCS#8, or SEC1) from `path`.
fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let file = File::open(path).context(OpenFileSnafu { path })?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .context(ReadKeySnafu { path })?
        .ok_or_else(|| TlsError::NoPrivateKey {
            path: path.to_path_buf(),
        })
}

/// Build a [`RootCertStore`] from a PEM CA bundle.
fn load_root_store(ca_cert: &Path) -> Result<RootCertStore, TlsError> {
    let certs = load_certs(ca_cert)?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).context(RustlsSnafu)?;
    }
    Ok(roots)
}

/// Build the inbound TLS server configuration.
///
/// `cert_path`/`key_path` are this router's own certificate chain and private key, presented
/// to every connecting client regardless of SNI. When `client_ca` is `Some`, mutual TLS is
/// enforced: connecting clients must present a certificate signed by one of the CAs in that
/// bundle, or the handshake fails. When `client_ca` is `None`, any client may connect without
/// presenting a certificate.
pub fn build_server_config(
    cert_path: &Path,
    key_path: &Path,
    client_ca: Option<&Path>,
) -> Result<Arc<ServerConfig>, TlsError> {
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    let builder = ServerConfig::builder();
    let builder = match client_ca {
        Some(ca_path) => {
            let roots = Arc::new(load_root_store(ca_path)?);
            let verifier = WebPkiClientVerifier::builder(roots)
                .build()
                .context(ClientVerifierSnafu)?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };

    let config = builder.with_single_cert(certs, key).context(RustlsSnafu)?;
    Ok(Arc::new(config))
}

/// Build the outbound TLS client configuration used to connect to a forwarding destination.
///
/// `ca_cert` is the CA bundle used to verify the destination's server certificate; this is
/// always required — outbound connections are never made without verification. When
/// `client_cert`/`client_key` are both supplied, they are presented to destinations that
/// require mutual TLS; otherwise no client certificate is offered.
pub fn build_client_config(
    ca_cert: &Path,
    client_cert: Option<&Path>,
    client_key: Option<&Path>,
) -> Result<Arc<ClientConfig>, TlsError> {
    let roots = load_root_store(ca_cert)?;
    let builder = ClientConfig::builder().with_root_certificates(roots);

    let config = match (client_cert, client_key) {
        (Some(cert_path), Some(key_path)) => {
            let certs = load_certs(cert_path)?;
            let key = load_key(key_path)?;
            builder
                .with_client_auth_cert(certs, key)
                .context(RustlsSnafu)?
        }
        _ => builder.with_no_client_auth(),
    };

    Ok(Arc::new(config))
}
