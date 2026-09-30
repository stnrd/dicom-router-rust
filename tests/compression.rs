mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use dicom_dictionary_std::{tags, uids};
use dicom_object::InMemDicomObject;
use dicom_router::{config, dispatcher, queue, scp, tls};
use dicom_transfer_syntax_registry::{TransferSyntaxIndex, TransferSyntaxRegistry};
use tokio_util::sync::CancellationToken;

fn router_config(
  dir: &Path,
  name: &str,
  pki: &common::PkiPaths,
  listen_port: u16,
  dest_port: u16,
  compression: &str,
) -> config::Config {
  let yaml = format!(
    r#"
listen_addr: "127.0.0.1:{listen_port}"
ae_title: "{name}"
queue_dir: "{q}"
dead_letter_dir: "{d}"
min_free_bytes: 0
retry:
  initial_delay_ms: 20
  max_delay_ms: 50
  max_attempts: 5
tls:
  server_cert: "{sc}"
  server_key: "{sk}"
destinations:
  - name: next-hop
    ae_title: "NEXT-HOP"
    host: 127.0.0.1
    port: {dest_port}
    server_name: localhost
    ca_cert: "{ca}"
    compression: {compression}
"#,
    q = dir.join(name).join("queue").display(),
    d = dir.join(name).join("dead").display(),
    sc = pki.server_cert.display(),
    sk = pki.server_key.display(),
    ca = pki.ca.display(),
  );
  let cfg: config::Config = serde_yaml::from_str(&yaml).unwrap();
  cfg.validate().unwrap();
  cfg
}

fn spawn_dispatcher(cfg: &config::Config, pki: &common::PkiPaths, token: &CancellationToken) {
  let log = slog::Logger::root(slog::Discard, slog::o!());
  dispatcher::spawn(dispatcher::WorkerConfig {
    destination: cfg.destinations[0].clone(),
    client_tls: Some(tls::build_client_config(&pki.ca, None, None).unwrap()),
    queue_root: cfg.queue_dir.clone(),
    dead_letter_dir: cfg.dead_letter_dir.clone(),
    retry_cfg: cfg.retry.clone(),
    calling_ae_title: cfg.ae_title.clone(),
    max_pdu_length: cfg.max_pdu_length,
    max_concurrent_sends: cfg.max_concurrent_sends,
    log,
    shutdown: token.clone(),
  });
}

fn spool(cfg: &config::Config, obj: &dicom_object::FileDicomObject<InMemDicomObject>) {
  let dir = cfg.queue_dir_for(&cfg.destinations[0].name);
  std::fs::create_dir_all(&dir).unwrap();
  queue::enqueue(&dir, obj, 0).unwrap();
}

async fn wait_for_objects(dest: &common::TestScp, n: usize) -> Vec<common::ReceivedObject> {
  let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
  while tokio::time::Instant::now() < deadline {
    if dest.objects.lock().unwrap().len() >= n {
      break;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
  }
  dest.objects.lock().unwrap().clone()
}

fn pixel_data(ts_uid: &str, dataset: &[u8]) -> Vec<u8> {
  let ts = TransferSyntaxRegistry.get(ts_uid).unwrap();
  let obj = InMemDicomObject::read_dataset_with_ts(dataset, ts).unwrap();
  obj.element(tags::PIXEL_DATA).unwrap().to_bytes().unwrap().into_owned()
}

fn original_pixels(obj: &dicom_object::FileDicomObject<InMemDicomObject>) -> Vec<u8> {
  obj.element(tags::PIXEL_DATA).unwrap().to_bytes().unwrap().into_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn sends_jpeg_xl_when_destination_accepts_it() {
  let dir = tempfile::tempdir().unwrap();
  let pki = common::write_pki(dir.path());
  let dest = common::start_test_scp(&pki.server_cert, &pki.server_key).await;
  let token = CancellationToken::new();

  let cfg = router_config(dir.path(), "SENDER", &pki, 0, dest.port, "jpeg-xl-lossless");
  let image = common::test_image("7.7.7.1");
  let raw_size = original_pixels(&image).len();
  spool(&cfg, &image);
  spawn_dispatcher(&cfg, &pki, &token);

  let objects = wait_for_objects(&dest, 1).await;
  token.cancel();
  assert_eq!(objects.len(), 1);
  let (ts, dataset) = &objects[0];
  assert_eq!(ts, uids::JPEGXL_LOSSLESS);
  assert!(
    dataset.len() < raw_size / 2,
    "{} bytes sent for {raw_size} raw pixel bytes",
    dataset.len()
  );
}

#[tokio::test(flavor = "multi_thread")]
async fn falls_back_to_original_when_destination_rejects_jpeg_xl() {
  let dir = tempfile::tempdir().unwrap();
  let pki = common::write_pki(dir.path());
  let dest =
    common::start_test_scp_with_ts(&pki.server_cert, &pki.server_key, &[uids::EXPLICIT_VR_LITTLE_ENDIAN]).await;
  let token = CancellationToken::new();

  let cfg = router_config(dir.path(), "SENDER", &pki, 0, dest.port, "jpeg-xl-lossless");
  let image = common::test_image("7.7.7.2");
  spool(&cfg, &image);
  spool(&cfg, &common::test_object("7.7.7.3"));
  spawn_dispatcher(&cfg, &pki, &token);

  let objects = wait_for_objects(&dest, 2).await;
  token.cancel();
  assert_eq!(objects.len(), 2);
  for (ts, _) in &objects {
    assert_eq!(ts, uids::EXPLICIT_VR_LITTLE_ENDIAN);
  }
  let with_pixels = objects
    .iter()
    .find(|(ts, ds)| {
      let obj = InMemDicomObject::read_dataset_with_ts(&ds[..], TransferSyntaxRegistry.get(ts).unwrap()).unwrap();
      obj.element(tags::PIXEL_DATA).is_ok()
    })
    .unwrap();
  assert_eq!(pixel_data(&with_pixels.0, &with_pixels.1), original_pixels(&image));
}

/// Sender router compresses, receiving router decompresses in front of the
/// PACS: the PACS gets the original pixels uncompressed.
#[tokio::test(flavor = "multi_thread")]
async fn router_chain_compresses_in_transit_and_restores_original_pixels() {
  let dir = tempfile::tempdir().unwrap();
  let pki = common::write_pki(dir.path());
  let pacs = common::start_test_scp(&pki.server_cert, &pki.server_key).await;
  let token = CancellationToken::new();
  let log = slog::Logger::root(slog::Discard, slog::o!());

  let receiver = Arc::new(router_config(
    dir.path(),
    "RECEIVER",
    &pki,
    12791,
    pacs.port,
    "explicit-le",
  ));
  let server_tls = tls::build_server_config(&pki.server_cert, &pki.server_key, None).unwrap();
  let _scp = scp::spawn(receiver.clone(), Some(server_tls), log, token.clone())
    .await
    .unwrap();
  spawn_dispatcher(&receiver, &pki, &token);

  let sender = router_config(dir.path(), "SENDER", &pki, 0, 12791, "jpeg-xl-lossless");
  let image = common::test_image("7.7.7.4");
  spool(&sender, &image);
  spawn_dispatcher(&sender, &pki, &token);

  let objects = wait_for_objects(&pacs, 1).await;
  token.cancel();
  assert_eq!(objects.len(), 1);
  let (ts, dataset) = &objects[0];
  assert_eq!(ts, uids::EXPLICIT_VR_LITTLE_ENDIAN);
  assert_eq!(pixel_data(ts, dataset), original_pixels(&image));
}
