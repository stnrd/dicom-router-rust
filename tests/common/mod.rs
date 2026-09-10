//! Shared test-only PKI fixtures: a self-signed CA plus a server and client
//! leaf certificate, generated in-memory with `rcgen` and optionally written to
//! PEM files on disk for tests that need file paths (e.g.
//! `tls::build_server_config`).

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use dicom_core::{dicom_value, DataElement, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_object::{FileMetaTableBuilder, InMemDicomObject};
use rcgen::{BasicConstraints, Certificate, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose};

/// Build a minimal CT image for queue/forward tests.
pub fn test_object(sop_instance_uid: &str) -> dicom_object::FileDicomObject<InMemDicomObject> {
  let mut obj = InMemDicomObject::new_empty();
  obj.put(DataElement::new(
    tags::SOP_CLASS_UID,
    VR::UI,
    dicom_value!(Str, uids::CT_IMAGE_STORAGE),
  ));
  obj.put(DataElement::new(
    tags::SOP_INSTANCE_UID,
    VR::UI,
    dicom_value!(Str, sop_instance_uid),
  ));
  obj.put(DataElement::new(
    tags::PATIENT_NAME,
    VR::PN,
    dicom_value!(Str, "TEST^PATIENT"),
  ));
  let meta = FileMetaTableBuilder::new()
    .media_storage_sop_class_uid(uids::CT_IMAGE_STORAGE)
    .media_storage_sop_instance_uid(sop_instance_uid)
    .transfer_syntax(uids::EXPLICIT_VR_LITTLE_ENDIAN)
    .build()
    .unwrap();
  obj.with_exact_meta(meta)
}

/// In-process TLS C-STORE SCP that records received SOP Instance UIDs.
pub struct TestScp {
  pub port:              u16,
  pub received:          Arc<Mutex<Vec<String>>>,
  pub association_count: Arc<std::sync::atomic::AtomicU32>,
  _handle:               tokio::task::JoinHandle<()>,
}

pub async fn start_test_scp(server_cert: &Path, server_key: &Path) -> TestScp {
  use dicom_ul::association::server::ServerAssociationOptions;
  use dicom_ul::pdu::{PDataValue, PDataValueType};
  use dicom_ul::Pdu;

  let tls_cfg = dicom_router::tls::build_server_config(server_cert, server_key, None).expect("test SCP TLS config");
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
    .await
    .expect("bind test SCP");
  let port = listener.local_addr().unwrap().port();
  let received = Arc::new(Mutex::new(Vec::new()));
  let received2 = received.clone();
  let association_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
  let association_count2 = association_count.clone();

  let handle = tokio::spawn(async move {
    loop {
      let (stream, _) = listener.accept().await.expect("accept");
      let tls_cfg = tls_cfg.clone();
      let received = received2.clone();
      let association_count = association_count2.clone();
      tokio::spawn(async move {
        let options = ServerAssociationOptions::new()
          .accept_any()
          .ae_title("TEST-DEST")
          .promiscuous(true)
          .tls_config(tls_cfg);
        let mut assoc = options.establish_tls_async(stream).await.expect("assoc");
        association_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut buf: Vec<u8> = Vec::new();
        let mut msgid = 1u16;
        let mut class = String::new();
        let mut inst = String::new();
        loop {
          match assoc.receive().await {
            Ok(Pdu::PData { mut data }) =>
              for dv in &mut data {
                if dv.value_type == PDataValueType::Command && dv.is_last {
                  let cmd = dicom_router::dimse::decode_command(&dv.data).unwrap();
                  if dicom_router::dimse::command_field(&cmd).unwrap() == dicom_router::dimse::C_STORE_RQ {
                    msgid = dicom_router::dimse::uint16(&cmd, dicom_router::dimse::TAG_MESSAGE_ID).unwrap();
                    class = dicom_router::dimse::string(&cmd, dicom_router::dimse::TAG_AFFECTED_SOP_CLASS_UID)
                      .unwrap()
                      .to_string();
                    inst = dicom_router::dimse::string(&cmd, dicom_router::dimse::TAG_AFFECTED_SOP_INSTANCE_UID)
                      .unwrap()
                      .to_string();
                    buf.clear();
                  }
                } else if dv.value_type == PDataValueType::Data {
                  buf.append(&mut dv.data);
                  if dv.is_last {
                    received.lock().unwrap().push(inst.clone());
                    let rsp =
                      dicom_router::dimse::create_cstore_rsp(msgid, &class, &inst, dicom_router::dimse::STATUS_SUCCESS);
                    let data = dicom_router::dimse::encode_command(&rsp);
                    assoc
                      .send(&Pdu::PData {
                        data: vec![PDataValue {
                          presentation_context_id: dv.presentation_context_id,
                          value_type: PDataValueType::Command,
                          is_last: true,
                          data,
                        }],
                      })
                      .await
                      .unwrap();
                  }
                }
              },
            Ok(Pdu::ReleaseRQ) => {
              let _ = assoc.send(&Pdu::ReleaseRP).await;
              break;
            }
            _ => break,
          }
        }
      });
    }
  });

  TestScp {
    port,
    received,
    association_count,
    _handle: handle,
  }
}

/// An in-memory CA plus one server and one client leaf certificate, all
/// PEM-encoded.
pub struct Pki {
  pub ca_cert_pem:     String,
  pub server_cert_pem: String,
  pub server_key_pem:  String,
  pub client_cert_pem: String,
  pub client_key_pem:  String,
}

/// Paths to the PEM files written by [`write_pki`].
pub struct PkiPaths {
  pub ca:          PathBuf,
  pub server_cert: PathBuf,
  pub server_key:  PathBuf,
  pub client_cert: PathBuf,
  pub client_key:  PathBuf,
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

fn make_leaf(common_name: &str, san: &[&str], ca_cert: &Certificate, ca_key: &KeyPair) -> (Certificate, KeyPair) {
  let key = KeyPair::generate().expect("generate leaf key");
  let mut params = CertificateParams::new(san.iter().map(|s| s.to_string()).collect::<Vec<_>>()).expect("leaf params");
  params.distinguished_name.push(DnType::CommonName, common_name);
  params.key_usages = vec![KeyUsagePurpose::DigitalSignature, KeyUsagePurpose::KeyEncipherment];
  let cert = params.signed_by(&key, ca_cert, ca_key).expect("sign leaf cert");
  (cert, key)
}

/// Generate a fresh CA + server + client PKI in memory.
pub fn generate_pki() -> Pki {
  let (ca_cert, ca_key) = make_ca();
  let (server_cert, server_key) = make_leaf("dicom-router test server", &["localhost"], &ca_cert, &ca_key);
  let (client_cert, client_key) = make_leaf("dicom-router test client", &[], &ca_cert, &ca_key);

  Pki {
    ca_cert_pem:     ca_cert.pem(),
    server_cert_pem: server_cert.pem(),
    server_key_pem:  server_key.serialize_pem(),
    client_cert_pem: client_cert.pem(),
    client_key_pem:  client_key.serialize_pem(),
  }
}

/// Generate a fresh PKI and write it as PEM files under `dir`. `dir` must
/// already exist.
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
