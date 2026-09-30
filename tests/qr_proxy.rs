//! C-FIND / C-GET proxy: requestor → router → [`common::TestQrScp`].
//!
//! The requestor side uses the router's own outbound association and message
//! reader, so fragmentation is exercised in both directions.

mod common;

use std::sync::Arc;

use dicom_dictionary_std::{tags, uids};
use dicom_router::config::{self, Destination};
use dicom_router::dimse::{self, CommandSet};
use dicom_router::qr::{send_message, Message, MessageReader, QrClient};
use dicom_router::scu::{self, ClientAssoc, RoleSelection};
use dicom_router::{scp, tls};
use tokio_util::sync::CancellationToken;

const EVRLE: &str = uids::EXPLICIT_VR_LITTLE_ENDIAN;
const IVRLE: &str = uids::IMPLICIT_VR_LITTLE_ENDIAN;
const ALLOWED_AE: &str = "VIEWER";

struct Harness {
  stub:   common::TestQrScp,
  router: Destination,
  pki:    common::PkiPaths,
  token:  CancellationToken,
  _dir:   tempfile::TempDir,
}

impl Drop for Harness {
  fn drop(&mut self) { self.token.cancel(); }
}

/// Router on `listen_port` proxying QR to a stub that accepts `stub_ts`.
/// `qr_port` overrides the destination port (to simulate an outage).
async fn start(listen_port: u16, stub_ts: &[&'static str], qr_port: Option<u16>) -> Harness {
  let dir = tempfile::tempdir().unwrap();
  let pki = common::write_pki(dir.path());
  let stub = common::start_test_qr_scp(&pki.server_cert, &pki.server_key, stub_ts).await;
  let yaml = format!(
    r#"
listen_addr: "127.0.0.1:{listen_port}"
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
  allowed_ae_titles: ["{ALLOWED_AE}"]
"#,
    q = dir.path().join("queue").display(),
    d = dir.path().join("dead").display(),
    sc = pki.server_cert.display(),
    sk = pki.server_key.display(),
    ca = pki.ca.display(),
    port = qr_port.unwrap_or(stub.port),
  );
  let cfg: config::Config = serde_yaml::from_str(&yaml).unwrap();
  cfg.validate().unwrap();
  let cfg = Arc::new(cfg);
  let token = CancellationToken::new();
  let log = slog::Logger::root(slog::Discard, slog::o!());
  let qr = QrClient {
    destination:       cfg.destinations[0].clone(),
    client_tls:        Some(tls::build_client_config(&pki.ca, None, None).unwrap()),
    allowed_ae_titles: cfg.query_retrieve.as_ref().unwrap().allowed_ae_titles.clone(),
  };
  let server_tls = tls::build_server_config(&pki.server_cert, &pki.server_key, None).unwrap();
  scp::spawn(cfg.clone(), Some(server_tls), log, token.clone(), Some(qr))
    .await
    .unwrap();

  let mut router = cfg.destinations[0].clone();
  router.ae_title = "QR-ROUTER".into();
  router.port = listen_port;
  Harness {
    stub,
    router,
    pki,
    token,
    _dir: dir,
  }
}

impl Harness {
  /// Open a requestor association to the router.
  async fn requestor(
    &self,
    calling_ae: &str,
    max_pdu: u32,
    pcs: &[(&str, &[&str])],
    storage_scp: &[&str],
  ) -> ClientAssoc {
    let pcs: Vec<(String, Vec<String>)> = pcs
      .iter()
      .map(|(a, ts)| (a.to_string(), ts.iter().map(|t| t.to_string()).collect()))
      .collect();
    let roles: Vec<RoleSelection> = storage_scp
      .iter()
      .map(|c| RoleSelection {
        sop_class: c.to_string(),
        scu:       false,
        scp:       true,
      })
      .collect();
    let client_tls = tls::build_client_config(&self.pki.ca, None, None).unwrap();
    scu::connect_with_roles(&self.router, Some(client_tls), calling_ae, max_pdu, &pcs, &roles)
      .await
      .unwrap()
  }
}

fn pc_for(assoc: &ClientAssoc, abstract_syntax: &str) -> (u8, String) {
  let pc = assoc
    .presentation_contexts()
    .iter()
    .find(|pc| pc.abstract_syntax == abstract_syntax)
    .unwrap();
  (pc.id, pc.transfer_syntax.clone())
}

async fn send_query(assoc: &mut ClientAssoc, sop_class: &str, patient_id: &str) -> (u8, String) {
  let (pc_id, ts) = pc_for(assoc, sop_class);
  let rq = if sop_class.ends_with(".3") {
    dimse::create_cget_rq(7, sop_class, dimse::PRIORITY_MEDIUM)
  } else {
    dimse::create_cfind_rq(7, sop_class, dimse::PRIORITY_MEDIUM)
  };
  let ident = common::encode_dataset(&common::find_identifier(patient_id), &ts);
  send_message(assoc, pc_id, &rq, Some(&ident)).await.unwrap();
  (pc_id, ts)
}

fn status(m: &Message) -> u16 { dimse::uint16(&m.command, dimse::TAG_STATUS).unwrap() }

/// Read responses until a non-pending one arrives.
async fn collect_responses(assoc: &mut ClientAssoc) -> Vec<Message> {
  let mut reader = MessageReader::default();
  let mut out = Vec::new();
  loop {
    let m = reader.read(assoc).await.unwrap();
    assert_eq!(
      dimse::uint16(&m.command, dimse::TAG_MESSAGE_ID_BEING_RESPONDED_TO),
      Some(7)
    );
    let done = status(&m) != dimse::STATUS_PENDING;
    out.push(m);
    if done {
      return out;
    }
  }
}

#[tokio::test]
async fn cfind_relays_matches_for_both_information_models() {
  let h = start(12801, &[EVRLE], None).await;
  for sop_class in [
    dimse::STUDY_ROOT_FIND_SOP_CLASS_UID,
    dimse::PATIENT_ROOT_FIND_SOP_CLASS_UID,
  ] {
    let mut assoc = h.requestor(ALLOWED_AE, 16384, &[(sop_class, &[EVRLE])], &[]).await;
    send_query(&mut assoc, sop_class, "P1").await;
    let rsps = collect_responses(&mut assoc).await;
    let statuses: Vec<u16> = rsps.iter().map(status).collect();
    assert_eq!(statuses, vec![dimse::STATUS_PENDING, dimse::STATUS_SUCCESS]);
    let matched = common::decode_dataset(rsps[0].dataset.as_ref().unwrap(), EVRLE);
    assert_eq!(
      common::element_str(&matched, tags::STUDY_INSTANCE_UID),
      common::QR_STUB_INSTANCE
    );
    let _ = scu::release(assoc).await;
  }
  assert_eq!(h.stub.finds.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn cfind_from_implicit_only_requestor_is_recoded_both_ways() {
  let h = start(12802, &[EVRLE], None).await;
  let sop_class = dimse::STUDY_ROOT_FIND_SOP_CLASS_UID;
  let mut assoc = h.requestor(ALLOWED_AE, 16384, &[(sop_class, &[IVRLE])], &[]).await;
  send_query(&mut assoc, sop_class, "P-IMPLICIT").await;
  let rsps = collect_responses(&mut assoc).await;

  let (ts, identifier) = h.stub.finds.lock().unwrap()[0].clone();
  assert_eq!(ts, EVRLE);
  let at_pacs = common::decode_dataset(&identifier, EVRLE);
  assert_eq!(common::element_str(&at_pacs, tags::PATIENT_ID), "P-IMPLICIT");

  let matched = common::decode_dataset(rsps[0].dataset.as_ref().unwrap(), IVRLE);
  assert_eq!(common::element_str(&matched, tags::PATIENT_ID), "P-IMPLICIT");
  let _ = scu::release(assoc).await;
}

/// The PACS sends a 512 KiB image as a command PDU plus several data PDUs;
/// the requestor has a small max PDU so the router must re-fragment, and it
/// only accepted CT in implicit VR, which the PACS must then use.
#[tokio::test]
async fn cget_relays_fragmented_images_in_the_requestors_transfer_syntax() {
  let h = start(12803, &[EVRLE, IVRLE], None).await;
  let sop_class = dimse::STUDY_ROOT_GET_SOP_CLASS_UID;
  let mut assoc = h
    .requestor(
      ALLOWED_AE,
      16384,
      &[(sop_class, &[EVRLE]), (uids::CT_IMAGE_STORAGE, &[IVRLE])],
      &[uids::CT_IMAGE_STORAGE],
    )
    .await;
  let (ct_pc_id, ct_ts) = pc_for(&assoc, uids::CT_IMAGE_STORAGE);
  assert_eq!(ct_ts, IVRLE);
  send_query(&mut assoc, sop_class, "P1").await;

  let mut reader = MessageReader::default();
  let store = reader.read(&mut assoc).await.unwrap();
  assert_eq!(dimse::command_field(&store.command), Some(dimse::C_STORE_RQ));
  assert_eq!(store.pc_id, ct_pc_id);
  let image = common::decode_dataset(store.dataset.as_ref().unwrap(), IVRLE);
  let expected = common::large_ct_image(common::QR_STUB_INSTANCE);
  assert_eq!(
    image.element(tags::PIXEL_DATA).unwrap().to_bytes().unwrap(),
    expected.element(tags::PIXEL_DATA).unwrap().to_bytes().unwrap()
  );
  let rsp = dimse::create_cstore_rsp(
    dimse::uint16(&store.command, dimse::TAG_MESSAGE_ID).unwrap(),
    uids::CT_IMAGE_STORAGE,
    common::QR_STUB_INSTANCE,
    dimse::STATUS_SUCCESS,
  );
  send_message(&mut assoc, ct_pc_id, &rsp, None).await.unwrap();

  let last = reader.read(&mut assoc).await.unwrap();
  assert_eq!(dimse::command_field(&last.command), Some(dimse::C_GET_RSP));
  assert_eq!(status(&last), dimse::STATUS_SUCCESS);
  assert_eq!(
    dimse::uint16(&last.command, dimse::TAG_NUMBER_OF_COMPLETED_SUBOPERATIONS),
    Some(1)
  );
  let _ = scu::release(assoc).await;
}

#[tokio::test]
async fn cancel_from_requestor_reaches_destination() {
  let h = start(12804, &[EVRLE], None).await;
  let sop_class = dimse::STUDY_ROOT_FIND_SOP_CLASS_UID;
  let mut assoc = h.requestor(ALLOWED_AE, 16384, &[(sop_class, &[EVRLE])], &[]).await;
  let (pc_id, _) = send_query(&mut assoc, sop_class, "CANCEL").await;

  let mut reader = MessageReader::default();
  let first = reader.read(&mut assoc).await.unwrap();
  assert_eq!(status(&first), dimse::STATUS_PENDING);
  let mut cancel = CommandSet::new();
  cancel
    .set_u16(dimse::TAG_COMMAND_FIELD, dimse::C_CANCEL_RQ)
    .set_u16(dimse::TAG_MESSAGE_ID_BEING_RESPONDED_TO, 7)
    .set_u16(dimse::TAG_COMMAND_DATA_SET_TYPE, dimse::NO_DATA_SET);
  send_message(&mut assoc, pc_id, &cancel, None).await.unwrap();

  let last = reader.read(&mut assoc).await.unwrap();
  assert_eq!(status(&last), common::STATUS_CANCEL);
  assert_eq!(*h.stub.cancels.lock().unwrap(), 1);
  let _ = scu::release(assoc).await;
}

#[tokio::test]
async fn destination_abort_ends_with_a_failure_response() {
  let h = start(12805, &[EVRLE], None).await;
  let sop_class = dimse::STUDY_ROOT_FIND_SOP_CLASS_UID;
  let mut assoc = h.requestor(ALLOWED_AE, 16384, &[(sop_class, &[EVRLE])], &[]).await;
  send_query(&mut assoc, sop_class, "ABORT").await;
  let rsps = collect_responses(&mut assoc).await;
  assert_eq!(rsps.iter().map(status).collect::<Vec<_>>(), vec![
    dimse::STATUS_OUT_OF_RESOURCES
  ]);
  let _ = scu::release(assoc).await;
}

#[tokio::test]
async fn calling_ae_not_in_allow_list_is_refused() {
  let h = start(12806, &[EVRLE], None).await;
  let sop_class = dimse::STUDY_ROOT_GET_SOP_CLASS_UID;
  let mut assoc = h.requestor("INTRUDER", 16384, &[(sop_class, &[EVRLE])], &[]).await;
  send_query(&mut assoc, sop_class, "P1").await;
  let rsps = collect_responses(&mut assoc).await;
  assert_eq!(rsps.iter().map(status).collect::<Vec<_>>(), vec![
    dimse::STATUS_NOT_AUTHORIZED
  ]);
  assert!(h.stub.gets.lock().unwrap().is_empty());
  let _ = scu::release(assoc).await;
}

#[tokio::test]
async fn unreachable_destination_refuses_query_but_echo_still_works() {
  let unused = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
  let dead_port = unused.local_addr().unwrap().port();
  drop(unused);
  let h = start(12807, &[EVRLE], Some(dead_port)).await;
  let sop_class = dimse::STUDY_ROOT_FIND_SOP_CLASS_UID;
  let mut assoc = h
    .requestor(
      ALLOWED_AE,
      16384,
      &[(sop_class, &[EVRLE]), (uids::VERIFICATION, &[EVRLE])],
      &[],
    )
    .await;
  send_query(&mut assoc, sop_class, "P1").await;
  let rsps = collect_responses(&mut assoc).await;
  assert_eq!(rsps.iter().map(status).collect::<Vec<_>>(), vec![
    dimse::STATUS_OUT_OF_RESOURCES
  ]);

  let (echo_pc, _) = pc_for(&assoc, uids::VERIFICATION);
  send_message(&mut assoc, echo_pc, &dimse::create_cecho_request(3), None)
    .await
    .unwrap();
  let echo = MessageReader::default().read(&mut assoc).await.unwrap();
  assert_eq!(dimse::command_field(&echo.command), Some(dimse::C_ECHO_RSP));
  assert_eq!(status(&echo), dimse::STATUS_SUCCESS);
  let _ = scu::release(assoc).await;
}
