mod common;

use std::sync::Arc;

use dicom_router::{config, dispatcher, scp, tls};
use dicom_ul::association::client::ClientAssociationOptions;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use dicom_ul::Pdu;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn end_to_end_tls_loopback() {
  let dir = tempfile::tempdir().unwrap();
  let pki = common::write_pki(dir.path());
  let dest_scp = common::start_test_scp(&pki.server_cert, &pki.server_key).await;

  let yaml = format!(
    r#"
listen_addr: "127.0.0.1:12770"
ae_title: "E2E-ROUTER"
queue_dir: "{q}"
dead_letter_dir: "{d}"
retry:
  initial_delay_ms: 20
  max_delay_ms: 50
  max_attempts: 5
tls:
  server_cert: "{sc}"
  server_key: "{sk}"
destinations:
  - name: loop-dest
    ae_title: "TEST-DEST"
    host: 127.0.0.1
    port: {port}
    server_name: localhost
    ca_cert: "{ca}"
"#,
    q = dir.path().join("queue").display(),
    d = dir.path().join("dead").display(),
    sc = pki.server_cert.display(),
    sk = pki.server_key.display(),
    ca = pki.ca.display(),
    port = dest_scp.port,
  );
  let cfg: config::Config = serde_yaml::from_str(&yaml).unwrap();
  cfg.validate().unwrap();
  let cfg = Arc::new(cfg);

  let token = CancellationToken::new();
  let log = slog::Logger::root(slog::Discard, slog::o!());
  let server_tls = tls::build_server_config(&pki.server_cert, &pki.server_key, None).unwrap();
  let _scp = scp::spawn(cfg.clone(), server_tls, log.clone(), token.clone())
    .await
    .unwrap();
  let client_tls = tls::build_client_config(&pki.ca, None, None).unwrap();
  let _disp = dispatcher::spawn(dispatcher::WorkerConfig {
    destination:          cfg.destinations[0].clone(),
    client_tls:           client_tls.clone(),
    queue_root:           cfg.queue_dir.clone(),
    dead_letter_dir:      cfg.dead_letter_dir.clone(),
    retry_cfg:            cfg.retry.clone(),
    calling_ae_title:     cfg.ae_title.clone(),
    max_pdu_length:       cfg.max_pdu_length,
    max_concurrent_sends: cfg.max_concurrent_sends,
    log:                  log.clone(),
    shutdown:             token.clone(),
  });

  let mut assoc = ClientAssociationOptions::new()
    .calling_ae_title("E2E-SCU")
    .with_presentation_context(dicom_dictionary_std::uids::CT_IMAGE_STORAGE, vec![
      dicom_dictionary_std::uids::EXPLICIT_VR_LITTLE_ENDIAN,
    ])
    .tls_config(client_tls)
    .server_name("localhost")
    .establish_with_async_tls("E2E-ROUTER@127.0.0.1:12770")
    .await
    .unwrap();
  let pc_id = assoc.presentation_contexts()[0].id;

  let rq = dicom_router::dimse::encode_command(&dicom_router::dimse::create_cstore_rq(
    1,
    dicom_dictionary_std::uids::CT_IMAGE_STORAGE,
    "1.2.3.99",
    dicom_router::dimse::PRIORITY_MEDIUM,
  ));
  let mut obj = dicom_object::InMemDicomObject::new_empty();
  use dicom_core::{dicom_value, DataElement, VR};
  use dicom_dictionary_std::tags;
  obj.put(DataElement::new(
    tags::SOP_CLASS_UID,
    VR::UI,
    dicom_value!(Str, dicom_dictionary_std::uids::CT_IMAGE_STORAGE),
  ));
  obj.put(DataElement::new(
    tags::SOP_INSTANCE_UID,
    VR::UI,
    dicom_value!(Str, "1.2.3.99"),
  ));
  let mut dataset = Vec::new();
  let ts = dicom_transfer_syntax_registry::entries::EXPLICIT_VR_LITTLE_ENDIAN.erased();
  obj.write_dataset_with_ts(&mut dataset, &ts).unwrap();

  assoc
    .send(&Pdu::PData {
      data: vec![
        PDataValue {
          presentation_context_id: pc_id,
          value_type:              PDataValueType::Command,
          is_last:                 true,
          data:                    rq,
        },
        PDataValue {
          presentation_context_id: pc_id,
          value_type:              PDataValueType::Data,
          is_last:                 true,
          data:                    dataset,
        },
      ],
    })
    .await
    .unwrap();

  match assoc.receive().await.unwrap() {
    Pdu::PData { data } => {
      let rsp = dicom_router::dimse::decode_command(&data[0].data).unwrap();
      assert_eq!(
        dicom_router::dimse::uint16(&rsp, dicom_router::dimse::TAG_STATUS).unwrap(),
        0x0000
      );
    }
    other => panic!("unexpected PDU: {other:?}"),
  }
  assoc.release().await.unwrap();

  let mut arrived = false;
  for _ in 0..100 {
    if !dest_scp.received.lock().unwrap().is_empty()
      && dicom_router::queue::scan(&cfg.queue_dir_for("loop-dest"))
        .unwrap()
        .is_empty()
    {
      arrived = true;
      break;
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
  }
  token.cancel();
  assert!(arrived, "destination never received object or queue did not drain");
  assert_eq!(dest_scp.received.lock().unwrap().as_slice(), &["1.2.3.99".to_string()]);
}
