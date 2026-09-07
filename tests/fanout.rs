mod common;

use std::sync::Arc;

use dicom_router::{config, queue, scp, tls};
use dicom_ul::association::client::ClientAssociationOptions;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use dicom_ul::Pdu;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn fan_out_spools_to_both_destinations() {
  let dir = tempfile::tempdir().unwrap();
  let pki = common::write_pki(dir.path());
  let queue_root = dir.path().join("queue");
  std::fs::create_dir_all(&queue_root).unwrap();

  let yaml = format!(
    r#"
listen_addr: "127.0.0.1:12771"
ae_title: "FANOUT-RTR"
queue_dir: "{q}"
dead_letter_dir: "{d}"
tls:
  server_cert: "{sc}"
  server_key: "{sk}"
destinations:
  - name: dest-a
    ae_title: "DEST-A"
    host: 127.0.0.1
    port: 1
    ca_cert: "{ca}"
  - name: dest-b
    ae_title: "DEST-B"
    host: 127.0.0.1
    port: 2
    ca_cert: "{ca}"
"#,
    q = queue_root.display(),
    d = dir.path().join("dead").display(),
    sc = pki.server_cert.display(),
    sk = pki.server_key.display(),
    ca = pki.ca.display(),
  );
  let cfg: config::Config = serde_yaml::from_str(&yaml).unwrap();
  cfg.validate().unwrap();
  let cfg = Arc::new(cfg);

  let token = CancellationToken::new();
  let log = slog::Logger::root(slog::Discard, slog::o!());
  let server_tls = tls::build_server_config(&pki.server_cert, &pki.server_key, None).unwrap();
  let client_tls = tls::build_client_config(&pki.ca, None, None).unwrap();
  let _scp = scp::spawn(cfg.clone(), server_tls, log.clone(), token.clone())
    .await
    .unwrap();

  let mut assoc = ClientAssociationOptions::new()
    .calling_ae_title("FANOUT-SCU")
    .with_presentation_context(dicom_dictionary_std::uids::CT_IMAGE_STORAGE, vec![
      dicom_dictionary_std::uids::EXPLICIT_VR_LITTLE_ENDIAN,
    ])
    .tls_config(client_tls)
    .server_name("localhost")
    .establish_with_async_tls("FANOUT-RTR@127.0.0.1:12771")
    .await
    .unwrap();
  let pc_id = assoc.presentation_contexts()[0].id;

  let rq = dicom_router::dimse::encode_command(&dicom_router::dimse::create_cstore_rq(
    1,
    dicom_dictionary_std::uids::CT_IMAGE_STORAGE,
    "1.2.3.88",
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
    dicom_value!(Str, "1.2.3.88"),
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

  assert_eq!(queue::scan(&cfg.queue_dir_for("dest-a")).unwrap().len(), 1);
  assert_eq!(queue::scan(&cfg.queue_dir_for("dest-b")).unwrap().len(), 1);

  token.cancel();
}
