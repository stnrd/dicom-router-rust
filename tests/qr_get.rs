mod common;

use std::sync::Arc;

use dicom_dictionary_std::{tags, uids};
use dicom_encoding::TransferSyntaxIndex;
use dicom_object::InMemDicomObject;
use dicom_router::{config, dimse, scp, tls};
use dicom_ul::association::client::ClientAssociationOptions;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use dicom_ul::Pdu;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn cget_relayed_with_cstore_suboperations() {
  let dir = tempfile::tempdir().unwrap();
  let pki = common::write_pki(dir.path());
  let stub = common::start_test_qr_scp(&pki.server_cert, &pki.server_key).await;

  let yaml = format!(
    r#"
listen_addr: "127.0.0.1:12781"
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
    .calling_ae_title("GET-SCU")
    .with_presentation_context(uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_GET, vec![
      uids::EXPLICIT_VR_LITTLE_ENDIAN,
    ])
    .with_presentation_context(uids::CT_IMAGE_STORAGE, vec![uids::EXPLICIT_VR_LITTLE_ENDIAN])
    .with_role_selection(uids::CT_IMAGE_STORAGE, false, true)
    .tls_config(client_tls)
    .server_name("localhost")
    .establish_with_async_tls("QR-ROUTER@127.0.0.1:12781")
    .await
    .unwrap();

  let get_pc_id = assoc
    .presentation_contexts()
    .iter()
    .find(|pc| pc.abstract_syntax == uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_GET)
    .unwrap()
    .id;
  let store_pc_id = assoc
    .presentation_contexts()
    .iter()
    .find(|pc| pc.abstract_syntax == uids::CT_IMAGE_STORAGE)
    .unwrap()
    .id;

  let ident = common::encode_find_identifier("TEST", Some("1.2.840.999.1"));
  let cmd = dimse::create_cget_rq(11, dimse::STUDY_ROOT_GET_SOP_CLASS_UID, dimse::PRIORITY_MEDIUM);
  assoc
    .send(&Pdu::PData {
      data: vec![
        PDataValue {
          presentation_context_id: get_pc_id,
          value_type:              PDataValueType::Command,
          is_last:                 true,
          data:                    dimse::encode_command(&cmd),
        },
        PDataValue {
          presentation_context_id: get_pc_id,
          value_type:              PDataValueType::Data,
          is_last:                 true,
          data:                    ident,
        },
      ],
    })
    .await
    .unwrap();

  let mut received = Vec::new();
  loop {
    match assoc.receive().await.unwrap() {
      Pdu::PData { data } => {
        let c = dimse::decode_command(&data[0].data).unwrap();
        let command_field = dimse::command_field(&c).unwrap();
        if command_field == dimse::C_STORE_RQ {
          let msgid = dimse::uint16(&c, dimse::TAG_MESSAGE_ID).unwrap();
          let sop_class = dimse::string(&c, dimse::TAG_AFFECTED_SOP_CLASS_UID).unwrap();
          let sop_instance = dimse::string(&c, dimse::TAG_AFFECTED_SOP_INSTANCE_UID).unwrap();
          let mut dataset = Vec::new();
          for dv in &data[1..] {
            if dv.value_type == PDataValueType::Data {
              dataset.extend_from_slice(&dv.data);
            }
          }
          let obj = InMemDicomObject::read_dataset_with_ts(
            dataset.as_slice(),
            dicom_transfer_syntax_registry::TransferSyntaxRegistry
              .get(uids::EXPLICIT_VR_LITTLE_ENDIAN)
              .unwrap(),
          )
          .unwrap();
          let uid = obj
            .element(tags::SOP_INSTANCE_UID)
            .unwrap()
            .to_str()
            .unwrap()
            .trim_end_matches(['\0', ' '])
            .to_string();
          received.push(uid);
          let rsp = dimse::create_cstore_rsp(msgid, sop_class, sop_instance, dimse::STATUS_SUCCESS);
          assoc
            .send(&Pdu::PData {
              data: vec![PDataValue {
                presentation_context_id: store_pc_id,
                value_type:              PDataValueType::Command,
                is_last:                 true,
                data:                    dimse::encode_command(&rsp),
              }],
            })
            .await
            .unwrap();
        } else if command_field == dimse::C_GET_RSP {
          let status = dimse::uint16(&c, dimse::TAG_STATUS).unwrap();
          if status == dimse::STATUS_PENDING {
            continue;
          }
          assert_eq!(dimse::uint16(&c, dimse::TAG_MESSAGE_ID_BEING_RESPONDED_TO), Some(11));
          assert_eq!(
            dimse::uint16(&c, dimse::TAG_NUMBER_OF_COMPLETED_SUBOPERATIONS),
            Some(1)
          );
          break;
        } else {
          panic!("unexpected command {command_field:#06x}");
        }
      }
      other => panic!("unexpected {other:?}"),
    }
  }

  let _ = assoc.release().await;
  token.cancel();
  assert_eq!(received, vec!["1.2.840.999.1"]);
  assert_eq!(stub.gets.lock().unwrap().len(), 1);
}
