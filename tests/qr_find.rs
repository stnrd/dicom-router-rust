mod common;

use std::sync::Arc;

use dicom_dictionary_std::uids;
use dicom_router::{config, dimse, scp, tls};
use dicom_ul::association::client::ClientAssociationOptions;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use dicom_ul::Pdu;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn cfind_is_forwarded_to_destination() {
  let dir = tempfile::tempdir().unwrap();
  let pki = common::write_pki(dir.path());
  let stub = common::start_test_qr_scp(&pki.server_cert, &pki.server_key).await;

  let yaml = format!(
    r#"
listen_addr: "127.0.0.1:12780"
ae_title: "QR-ROUTER"
queue_dir: "{q}"
dead_letter_dir: "{d}"
tls:
  server_cert: "{sc}"
  server_key: "{sk}"
destinations:
  - name: qr-dest
    ae_title: "TEST-DEST"
    host: 127.0.0.1
    port: {port}
    server_name: localhost
    ca_cert: "{ca}"
query_retrieve:
  destination: qr-dest
"#,
    q = dir.path().join("queue").display(),
    d = dir.path().join("dead").display(),
    sc = pki.server_cert.display(),
    sk = pki.server_key.display(),
    ca = pki.ca.display(),
    port = stub.port,
  );
  let cfg: config::Config = serde_yaml::from_str(&yaml).unwrap();
  cfg.validate().unwrap();
  let cfg = Arc::new(cfg);
  let token = CancellationToken::new();
  let log = slog::Logger::root(slog::Discard, slog::o!());
  let server_tls = tls::build_server_config(&pki.server_cert, &pki.server_key, None).unwrap();
  let client_tls = tls::build_client_config(&pki.ca, None, None).unwrap();
  let qr = Some(dicom_router::qr::QrClient {
    destination: cfg.destinations[0].clone(),
    client_tls:  Some(client_tls.clone()),
  });
  let _scp = scp::spawn(cfg.clone(), Some(server_tls), log, token.clone(), qr)
    .await
    .unwrap();

  let mut assoc = ClientAssociationOptions::new()
    .calling_ae_title("FIND-SCU")
    .with_presentation_context(uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND, vec![
      uids::EXPLICIT_VR_LITTLE_ENDIAN,
    ])
    .tls_config(client_tls)
    .server_name("localhost")
    .establish_with_async_tls("QR-ROUTER@127.0.0.1:12780")
    .await
    .unwrap();
  let pc_id = assoc.presentation_contexts()[0].id;
  let ident = common::encode_find_identifier("TEST", Some("1.2.3"));
  let cmd = dimse::create_cfind_rq(7, dimse::STUDY_ROOT_FIND_SOP_CLASS_UID, dimse::PRIORITY_MEDIUM);
  assoc
    .send(&Pdu::PData {
      data: vec![
        PDataValue {
          presentation_context_id: pc_id,
          value_type:              PDataValueType::Command,
          is_last:                 true,
          data:                    dimse::encode_command(&cmd),
        },
        PDataValue {
          presentation_context_id: pc_id,
          value_type:              PDataValueType::Data,
          is_last:                 true,
          data:                    ident,
        },
      ],
    })
    .await
    .unwrap();

  let mut statuses = Vec::new();
  loop {
    match assoc.receive().await.unwrap() {
      Pdu::PData { data } => {
        let c = dimse::decode_command(&data[0].data).unwrap();
        assert_eq!(dimse::command_field(&c), Some(dimse::C_FIND_RSP));
        assert_eq!(dimse::uint16(&c, dimse::TAG_MESSAGE_ID_BEING_RESPONDED_TO), Some(7));
        let st = dimse::uint16(&c, dimse::TAG_STATUS).unwrap();
        statuses.push(st);
        if st != dimse::STATUS_PENDING {
          break;
        }
      }
      other => panic!("unexpected {other:?}"),
    }
  }
  let _ = assoc.release().await;
  token.cancel();
  assert_eq!(statuses, vec![dimse::STATUS_PENDING, dimse::STATUS_SUCCESS]);
  assert_eq!(stub.finds.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cfind_patient_root_is_forwarded() {
  let dir = tempfile::tempdir().unwrap();
  let pki = common::write_pki(dir.path());
  let stub = common::start_test_qr_scp(&pki.server_cert, &pki.server_key).await;

  let yaml = format!(
    r#"
listen_addr: "127.0.0.1:12783"
ae_title: "QR-ROUTER"
queue_dir: "{q}"
dead_letter_dir: "{d}"
tls:
  server_cert: "{sc}"
  server_key: "{sk}"
destinations:
  - name: qr-dest
    ae_title: "TEST-DEST"
    host: 127.0.0.1
    port: {port}
    server_name: localhost
    ca_cert: "{ca}"
query_retrieve:
  destination: qr-dest
"#,
    q = dir.path().join("queue").display(),
    d = dir.path().join("dead").display(),
    sc = pki.server_cert.display(),
    sk = pki.server_key.display(),
    ca = pki.ca.display(),
    port = stub.port,
  );
  let cfg: config::Config = serde_yaml::from_str(&yaml).unwrap();
  cfg.validate().unwrap();
  let cfg = Arc::new(cfg);
  let token = CancellationToken::new();
  let log = slog::Logger::root(slog::Discard, slog::o!());
  let server_tls = tls::build_server_config(&pki.server_cert, &pki.server_key, None).unwrap();
  let client_tls = tls::build_client_config(&pki.ca, None, None).unwrap();
  let qr = Some(dicom_router::qr::QrClient {
    destination: cfg.destinations[0].clone(),
    client_tls:  Some(client_tls.clone()),
  });
  let _scp = scp::spawn(cfg.clone(), Some(server_tls), log, token.clone(), qr)
    .await
    .unwrap();

  let mut assoc = ClientAssociationOptions::new()
    .calling_ae_title("FIND-SCU")
    .with_presentation_context(uids::PATIENT_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND, vec![
      uids::EXPLICIT_VR_LITTLE_ENDIAN,
    ])
    .tls_config(client_tls)
    .server_name("localhost")
    .establish_with_async_tls("QR-ROUTER@127.0.0.1:12783")
    .await
    .unwrap();
  let pc_id = assoc.presentation_contexts()[0].id;
  let ident = common::encode_find_identifier("TEST", Some("1.2.3"));
  let cmd = dimse::create_cfind_rq(7, dimse::PATIENT_ROOT_FIND_SOP_CLASS_UID, dimse::PRIORITY_MEDIUM);
  assoc
    .send(&Pdu::PData {
      data: vec![
        PDataValue {
          presentation_context_id: pc_id,
          value_type:              PDataValueType::Command,
          is_last:                 true,
          data:                    dimse::encode_command(&cmd),
        },
        PDataValue {
          presentation_context_id: pc_id,
          value_type:              PDataValueType::Data,
          is_last:                 true,
          data:                    ident,
        },
      ],
    })
    .await
    .unwrap();

  let mut statuses = Vec::new();
  loop {
    match assoc.receive().await.unwrap() {
      Pdu::PData { data } => {
        let c = dimse::decode_command(&data[0].data).unwrap();
        assert_eq!(dimse::command_field(&c), Some(dimse::C_FIND_RSP));
        assert_eq!(dimse::uint16(&c, dimse::TAG_MESSAGE_ID_BEING_RESPONDED_TO), Some(7));
        let st = dimse::uint16(&c, dimse::TAG_STATUS).unwrap();
        statuses.push(st);
        if st != dimse::STATUS_PENDING {
          break;
        }
      }
      other => panic!("unexpected {other:?}"),
    }
  }
  let _ = assoc.release().await;
  token.cancel();
  assert_eq!(statuses, vec![dimse::STATUS_PENDING, dimse::STATUS_SUCCESS]);
  assert_eq!(stub.finds.lock().unwrap().len(), 1);
}
