//! Shared test-only PKI fixtures: a self-signed CA plus a server and client
//! leaf certificate, generated in-memory with `rcgen` and optionally written to
//! PEM files on disk for tests that need file paths (e.g.
//! `tls::build_server_config`).

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use dicom_core::{dicom_value, DataElement, VR};
use dicom_dictionary_std::{tags, uids};
use dicom_encoding::TransferSyntaxIndex;
use dicom_object::{FileMetaTableBuilder, InMemDicomObject};
use dicom_transfer_syntax_registry::TransferSyntaxRegistry;
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

pub fn encode_dataset(obj: &InMemDicomObject, ts_uid: &str) -> Vec<u8> {
  let ts = TransferSyntaxRegistry.get(ts_uid).expect("known transfer syntax");
  let mut buf = Vec::new();
  obj.write_dataset_with_ts(&mut buf, ts).expect("encode dataset");
  buf
}

pub fn decode_dataset(bytes: &[u8], ts_uid: &str) -> InMemDicomObject {
  let ts = TransferSyntaxRegistry.get(ts_uid).expect("known transfer syntax");
  InMemDicomObject::read_dataset_with_ts(bytes, ts).expect("decode dataset")
}

pub fn element_str(obj: &InMemDicomObject, tag: dicom_core::Tag) -> String {
  obj
    .element(tag)
    .unwrap()
    .to_str()
    .unwrap()
    .trim_end_matches(['\0', ' '])
    .to_string()
}

pub fn find_identifier(patient_id: &str) -> InMemDicomObject {
  let mut obj = InMemDicomObject::new_empty();
  obj.put(DataElement::new(
    tags::QUERY_RETRIEVE_LEVEL,
    VR::CS,
    dicom_value!(Str, "STUDY"),
  ));
  obj.put(DataElement::new(
    tags::PATIENT_ID,
    VR::LO,
    dicom_value!(Str, patient_id),
  ));
  obj.put(DataElement::new(
    tags::STUDY_INSTANCE_UID,
    VR::UI,
    dicom_value!(Str, ""),
  ));
  obj
}

/// 512x512 16-bit CT image (512 KiB of pixel data): larger than any default
/// max PDU, so it always travels in several P-DATA fragments.
pub fn large_ct_image(sop_instance_uid: &str) -> InMemDicomObject {
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
  obj.put(DataElement::new(tags::ROWS, VR::US, dicom_value!(U16, [512])));
  obj.put(DataElement::new(tags::COLUMNS, VR::US, dicom_value!(U16, [512])));
  obj.put(DataElement::new(tags::BITS_ALLOCATED, VR::US, dicom_value!(U16, [16])));
  let pixels: Vec<u16> = (0..512u32 * 512).map(|i| (i * 7 % 4096) as u16).collect();
  obj.put(DataElement::new(
    tags::PIXEL_DATA,
    VR::OW,
    dicom_core::PrimitiveValue::U16(pixels.into()),
  ));
  obj
}

pub const STATUS_CANCEL: u16 = 0xFE00;
pub const QR_STUB_INSTANCE: &str = "1.2.840.999.1";

struct AcceptStorageScp;

impl dicom_ul::association::server::Negotiation for AcceptStorageScp {
  fn negotiate_roles(
    &self,
    _sop_class_uid: &str,
    scu_role: bool,
    scp_role: bool,
  ) -> Option<dicom_ul::pdu::RequestorRoles> {
    Some(dicom_ul::pdu::RequestorRoles {
      scu: scu_role,
      scp: scp_role,
    })
  }
}

/// A query the stub received: transfer syntax of its presentation context
/// plus the identifier bytes.
pub type ReceivedQuery = (String, Vec<u8>);

/// In-process TLS C-FIND/C-GET SCP standing in for a PACS.
///
/// Behaves like a real archive on the wire: commands and datasets go out as
/// separate PDUs and datasets are fragmented to the peer's max PDU length.
/// The query's PatientID selects the scenario:
/// - `ABORT`: abort the association instead of answering
/// - `CANCEL`: one pending response, then wait for C-CANCEL-RQ and answer with
///   status Cancel
/// - anything else: C-FIND gets one pending match plus success; C-GET sends
///   [`large_ct_image`] as a C-STORE sub-operation, then success
pub struct TestQrScp {
  pub port:    u16,
  pub finds:   Arc<Mutex<Vec<ReceivedQuery>>>,
  pub gets:    Arc<Mutex<Vec<ReceivedQuery>>>,
  pub cancels: Arc<Mutex<u32>>,
  _handle:     tokio::task::JoinHandle<()>,
}

pub async fn start_test_qr_scp(server_cert: &Path, server_key: &Path, transfer_syntaxes: &[&'static str]) -> TestQrScp {
  use dicom_router::dimse;
  use dicom_router::qr::{send_message, MessageReader};
  use dicom_ul::association::server::ServerAssociationOptions;

  let tls_cfg = dicom_router::tls::build_server_config(server_cert, server_key, None).expect("test QR SCP TLS config");
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
    .await
    .expect("bind test QR SCP");
  let port = listener.local_addr().unwrap().port();
  let finds = Arc::new(Mutex::new(Vec::new()));
  let gets = Arc::new(Mutex::new(Vec::new()));
  let cancels = Arc::new(Mutex::new(0));
  let transfer_syntaxes = transfer_syntaxes.to_vec();
  let (finds2, gets2, cancels2) = (finds.clone(), gets.clone(), cancels.clone());

  let handle = tokio::spawn(async move {
    loop {
      let (stream, _) = listener.accept().await.expect("accept");
      let mut options = ServerAssociationOptions::new()
        .accept_any()
        .ae_title("TEST-DEST")
        .promiscuous(true)
        .with_negotiation(AcceptStorageScp)
        .tls_config(tls_cfg.clone());
      for ts in &transfer_syntaxes {
        options = options.with_transfer_syntax(*ts);
      }
      for uid in dicom_router::qr::QR_SOP_CLASSES.iter().chain(&[uids::CT_IMAGE_STORAGE]) {
        options = options.with_abstract_syntax(*uid);
      }
      let (finds, gets, cancels) = (finds2.clone(), gets2.clone(), cancels2.clone());
      tokio::spawn(async move {
        let Ok(mut assoc) = options.establish_tls_async(stream).await else {
          return;
        };
        let mut reader = MessageReader::default();
        let Ok(request) = reader.read(&mut assoc).await else {
          return;
        };
        let pc = assoc
          .presentation_contexts()
          .iter()
          .find(|pc| pc.id == request.pc_id)
          .unwrap()
          .clone();
        let msgid = dimse::uint16(&request.command, dimse::TAG_MESSAGE_ID).unwrap();
        let identifier = request.dataset.unwrap();
        let patient_id = element_str(&decode_dataset(&identifier, &pc.transfer_syntax), tags::PATIENT_ID);
        let field = dimse::command_field(&request.command).unwrap();
        let query = (pc.transfer_syntax.clone(), identifier);
        if field == dimse::C_FIND_RQ {
          finds.lock().unwrap().push(query);
        } else {
          gets.lock().unwrap().push(query);
        }

        if patient_id == "ABORT" {
          let _ = assoc.abort().await;
          return;
        }
        if field == dimse::C_FIND_RQ {
          let mut matched = find_identifier(&patient_id);
          matched.put(DataElement::new(
            tags::STUDY_INSTANCE_UID,
            VR::UI,
            dicom_value!(Str, QR_STUB_INSTANCE),
          ));
          let pending = dimse::create_cfind_rsp(msgid, &pc.abstract_syntax, dimse::STATUS_PENDING, true);
          let data = encode_dataset(&matched, &pc.transfer_syntax);
          send_message(&mut assoc, pc.id, &pending, Some(&data)).await.unwrap();
          let status = if patient_id == "CANCEL" {
            let cancel = reader.read(&mut assoc).await.unwrap();
            assert_eq!(dimse::command_field(&cancel.command), Some(dimse::C_CANCEL_RQ));
            assert_eq!(
              dimse::uint16(&cancel.command, dimse::TAG_MESSAGE_ID_BEING_RESPONDED_TO),
              Some(msgid)
            );
            *cancels.lock().unwrap() += 1;
            STATUS_CANCEL
          } else {
            dimse::STATUS_SUCCESS
          };
          let last = dimse::create_cfind_rsp(msgid, &pc.abstract_syntax, status, false);
          send_message(&mut assoc, pc.id, &last, None).await.unwrap();
        } else {
          let ct_pc = assoc
            .presentation_contexts()
            .iter()
            .find(|p| {
              p.abstract_syntax == uids::CT_IMAGE_STORAGE
                && p.reason == dicom_ul::pdu::PresentationContextResultReason::Acceptance
            })
            .expect("CT storage context")
            .clone();
          let store = dimse::create_cstore_rq(41, uids::CT_IMAGE_STORAGE, QR_STUB_INSTANCE, dimse::PRIORITY_MEDIUM);
          let image = encode_dataset(&large_ct_image(QR_STUB_INSTANCE), &ct_pc.transfer_syntax);
          send_message(&mut assoc, ct_pc.id, &store, Some(&image)).await.unwrap();
          let rsp = reader.read(&mut assoc).await.unwrap();
          assert_eq!(dimse::command_field(&rsp.command), Some(dimse::C_STORE_RSP));
          assert_eq!(
            dimse::uint16(&rsp.command, dimse::TAG_MESSAGE_ID_BEING_RESPONDED_TO),
            Some(41)
          );
          let ok = dimse::uint16(&rsp.command, dimse::TAG_STATUS) == Some(dimse::STATUS_SUCCESS);
          let (completed, failed) = if ok { (1, 0) } else { (0, 1) };
          let last = dimse::create_cget_rsp(
            msgid,
            &pc.abstract_syntax,
            dimse::STATUS_SUCCESS,
            0,
            completed,
            failed,
            0,
          );
          send_message(&mut assoc, pc.id, &last, None).await.unwrap();
        }
        if let Ok(dicom_ul::Pdu::ReleaseRQ) = assoc.receive().await {
          let _ = assoc.send(&dicom_ul::Pdu::ReleaseRP).await;
        }
      });
    }
  });

  TestQrScp {
    port,
    finds,
    gets,
    cancels,
    _handle: handle,
  }
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

/// In-process cleartext C-STORE SCP (no TLS) that records received SOP Instance
/// UIDs.
pub async fn start_test_scp_plain() -> TestScp {
  use dicom_ul::association::server::ServerAssociationOptions;
  use dicom_ul::pdu::{PDataValue, PDataValueType};
  use dicom_ul::Pdu;

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
      let received = received2.clone();
      let association_count = association_count2.clone();
      tokio::spawn(async move {
        let options = ServerAssociationOptions::new()
          .accept_any()
          .ae_title("TEST-DEST")
          .promiscuous(true);
        let mut assoc = options.establish_async(stream).await.expect("assoc");
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
