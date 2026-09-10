mod common;

use dicom_router::{scu, tls};

#[tokio::test]
async fn reuses_one_association_for_many_cstores() {
  let dir = tempfile::tempdir().unwrap();
  let pki = common::write_pki(dir.path());
  let dest_scp = common::start_test_scp(&pki.server_cert, &pki.server_key).await;
  let client_tls = tls::build_client_config(&pki.ca, None, None).unwrap();

  let destination = dicom_router::config::Destination {
    name:             "test-dest".into(),
    ae_title:         "TEST-DEST".into(),
    host:             "127.0.0.1".into(),
    port:             dest_scp.port,
    server_name:      Some("localhost".into()),
    ca_cert:          pki.ca.clone(),
    client_cert:      None,
    client_key:       None,
    source_ae_titles: vec![],
  };

  let pcs = vec![scu::PresentationKey {
    abstract_syntax: dicom_dictionary_std::uids::CT_IMAGE_STORAGE.to_string(),
    transfer_syntax: dicom_dictionary_std::uids::EXPLICIT_VR_LITTLE_ENDIAN.to_string(),
  }];
  let mut assoc = scu::connect(&destination, client_tls.clone(), "ROUTER", 16_384, &pcs)
    .await
    .unwrap();

  for i in 1..=10 {
    let uid = format!("1.2.3.{i}");
    let file = common::test_object(&uid);
    let path = dir.path().join(format!("{i}.dcm"));
    file.write_to_file(&path).unwrap();
    let on_disk = dicom_object::open_file(&path).unwrap();
    let send_meta = scu::SpooledMeta {
      sop_class_uid:    dicom_dictionary_std::uids::CT_IMAGE_STORAGE.to_string(),
      sop_instance_uid: uid,
      transfer_syntax:  dicom_dictionary_std::uids::EXPLICIT_VR_LITTLE_ENDIAN.to_string(),
    };
    let log = slog::Logger::root(slog::Discard, slog::o!());
    scu::send_object(&mut assoc, &on_disk, &send_meta, &log).await.unwrap();
  }

  scu::release(assoc).await.unwrap();

  assert_eq!(dest_scp.received.lock().unwrap().len(), 10);
  assert_eq!(dest_scp.association_count.load(std::sync::atomic::Ordering::SeqCst), 1);
}
