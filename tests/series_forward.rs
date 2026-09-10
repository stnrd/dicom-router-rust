mod common;

use std::sync::Arc;
use std::time::Duration;

use dicom_router::{config, dispatcher, scp, tls};
use dicom_transfer_syntax_registry::TransferSyntaxIndex;
use dicom_ul::association::client::ClientAssociationOptions;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use dicom_ul::Pdu;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn router_forwards_series_on_one_outbound_association() {
  let dir = tempfile::tempdir().unwrap();
  let pki = common::write_pki(dir.path());
  let dest_scp = common::start_test_scp(&pki.server_cert, &pki.server_key).await;

  let yaml = format!(
    r#"
listen_addr: "127.0.0.1:12772"
ae_title: "SERIES-ROUTER"
queue_dir: "{q}"
dead_letter_dir: "{d}"
max_concurrent_sends: 4
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
    .calling_ae_title("SERIES-SCU")
    .with_presentation_context(dicom_dictionary_std::uids::CT_IMAGE_STORAGE, vec![
      dicom_dictionary_std::uids::EXPLICIT_VR_LITTLE_ENDIAN,
    ])
    .tls_config(client_tls)
    .server_name("localhost")
    .establish_with_async_tls("SERIES-ROUTER@127.0.0.1:12772")
    .await
    .unwrap();
  let pc_id = assoc.presentation_contexts()[0].id;

  for i in 1..=20 {
    let uid = format!("9.9.9.{i}");
    let rq = dicom_router::dimse::encode_command(&dicom_router::dimse::create_cstore_rq(
      i as u16,
      dicom_dictionary_std::uids::CT_IMAGE_STORAGE,
      &uid,
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
    obj.put(DataElement::new(tags::SOP_INSTANCE_UID, VR::UI, dicom_value!(Str, uid)));
    let mut dataset = Vec::new();
    obj
      .write_dataset_with_ts(
        &mut dataset,
        dicom_transfer_syntax_registry::TransferSyntaxRegistry
          .get(dicom_dictionary_std::uids::EXPLICIT_VR_LITTLE_ENDIAN)
          .unwrap(),
      )
      .unwrap();
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
    let _ = assoc.receive().await.unwrap();
  }
  let _ = assoc.release().await;

  let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
  while tokio::time::Instant::now() < deadline {
    if dest_scp.received.lock().unwrap().len() >= 20 {
      break;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
  }

  assert_eq!(dest_scp.received.lock().unwrap().len(), 20);
  assert_eq!(dest_scp.association_count.load(std::sync::atomic::Ordering::SeqCst), 1);
}
