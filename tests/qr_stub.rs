mod common;

use dicom_dictionary_std::uids;
use dicom_router::dimse;
use dicom_ul::association::client::ClientAssociationOptions;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use dicom_ul::Pdu;

#[tokio::test]
async fn stub_answers_cfind_with_one_pending_then_success() {
  let dir = tempfile::tempdir().unwrap();
  let pki = common::write_pki(dir.path());
  let stub = common::start_test_qr_scp(&pki.server_cert, &pki.server_key).await;
  let client_tls = dicom_router::tls::build_client_config(&pki.ca, None, None).unwrap();

  let mut assoc = ClientAssociationOptions::new()
    .calling_ae_title("FIND-SCU")
    .with_presentation_context(uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND, vec![
      uids::EXPLICIT_VR_LITTLE_ENDIAN,
    ])
    .tls_config(client_tls)
    .server_name("localhost")
    .establish_with_async_tls(&format!("TEST-DEST@127.0.0.1:{}", stub.port))
    .await
    .unwrap();
  let pc_id = assoc.presentation_contexts()[0].id;
  let ident = common::encode_find_identifier("TEST", Some("1.2.3"));
  let cmd = dimse::create_cfind_rq(1, dimse::STUDY_ROOT_FIND_SOP_CLASS_UID, dimse::PRIORITY_MEDIUM);
  assoc
    .send(&Pdu::PData {
      data: vec![
        PDataValue {
          presentation_context_id: pc_id,
          value_type: PDataValueType::Command,
          is_last: true,
          data: dimse::encode_command(&cmd),
        },
        PDataValue {
          presentation_context_id: pc_id,
          value_type: PDataValueType::Data,
          is_last: true,
          data: ident,
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
  assert_eq!(statuses, vec![dimse::STATUS_PENDING, dimse::STATUS_SUCCESS]);
  assert_eq!(stub.finds.lock().unwrap().len(), 1);
}
