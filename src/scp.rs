//! Inbound DICOM Service Class Provider (C-STORE SCP over TLS).

use std::sync::Arc;

use dicom_dictionary_std::uids;
use dicom_encoding::transfer_syntax::TransferSyntaxIndex;
use dicom_object::{FileMetaTableBuilder, InMemDicomObject};
use dicom_transfer_syntax_registry::TransferSyntaxRegistry;
use dicom_ul::association::server::{AcceptAny, Negotiation, ServerAssociationOptions};
use dicom_ul::association::{Association, AsyncServerAssociation};
use dicom_ul::pdu::{PDataValue, PDataValueType, PresentationContextResultReason};
use dicom_ul::Pdu;
use slog::{debug, error, info, o, warn, Logger};
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::dimse::{self, CommandSet, TAG_AFFECTED_SOP_CLASS_UID, TAG_AFFECTED_SOP_INSTANCE_UID, TAG_MESSAGE_ID};
use crate::qr::{self, QrClient, QrKind};
use crate::queue::{self, QueueError};

enum Pending {
  None,
  Store {
    msgid:            u16,
    sop_class_uid:    String,
    sop_instance_uid: String,
  },
  Query {
    kind:          QrKind,
    msgid:         u16,
    sop_class_uid: String,
    pc_id:         u8,
  },
}

#[derive(Clone)]
struct EchoRoles;

impl Negotiation for EchoRoles {
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

#[derive(Debug, Snafu)]
pub enum ScpError {
  #[snafu(display("missing presentation context for incoming C-STORE"))]
  MissingPresentationContext,

  #[snafu(display("unsupported transfer syntax {ts_uid}"))]
  UnsupportedTransferSyntax { ts_uid: String },

  #[snafu(display("failed to read dataset: {source}"))]
  ReadDataset { source: Box<dicom_object::ReadError> },

  #[snafu(display("missing SOP Class UID in dataset"))]
  MissingSopClassUid,

  #[snafu(display("missing SOP Instance UID in dataset"))]
  MissingSopInstanceUid,

  #[snafu(display("failed to build DICOM file meta: {reason}"))]
  BuildFileMeta { reason: String },

  #[snafu(display("no destination configured for calling AE {calling_ae}"))]
  NoDestination { calling_ae: String },

  #[snafu(display("spool task panicked: {source}"))]
  SpoolTaskPanicked { source: tokio::task::JoinError },

  #[snafu(display("spool failed for {destination_count} destination(s): {source}"))]
  Spool {
    destination_count: usize,
    source:            QueueError,
  },

  #[snafu(display("could not send response PDU: {source}"))]
  SendPdu { source: Box<dicom_ul::association::Error> },
}

type Result<T> = std::result::Result<T, ScpError>;

/// Storage SOP classes accepted by default.
pub const ABSTRACT_SYNTAXES: &[&str] = &[
  uids::VERIFICATION,
  uids::CT_IMAGE_STORAGE,
  uids::MR_IMAGE_STORAGE,
  uids::SECONDARY_CAPTURE_IMAGE_STORAGE,
  uids::COMPUTED_RADIOGRAPHY_IMAGE_STORAGE,
  uids::DIGITAL_X_RAY_IMAGE_STORAGE_FOR_PRESENTATION,
  uids::ULTRASOUND_IMAGE_STORAGE,
  uids::NUCLEAR_MEDICINE_IMAGE_STORAGE,
  uids::POSITRON_EMISSION_TOMOGRAPHY_IMAGE_STORAGE,
];

fn server_options(
  cfg: &Config,
  tls: Option<Arc<rustls::ServerConfig>>,
  qr: Option<&QrClient>,
) -> ServerAssociationOptions<'static, AcceptAny, EchoRoles> {
  let mut options = ServerAssociationOptions::new()
    .accept_any()
    .ae_title(cfg.ae_title.clone())
    .max_pdu_length(cfg.max_pdu_length)
    .promiscuous(cfg.promiscuous)
    .with_negotiation(EchoRoles);
  if let Some(tls_cfg) = tls {
    options = options.tls_config(tls_cfg);
  }
  for ts in TransferSyntaxRegistry.iter() {
    if !ts.is_unsupported() {
      options = options.with_transfer_syntax(ts.uid());
    }
  }
  for uid in ABSTRACT_SYNTAXES {
    options = options.with_abstract_syntax(*uid);
  }
  if qr.is_some() {
    for uid in qr::QR_SOP_CLASSES {
      options = options.with_abstract_syntax(*uid);
    }
  }
  options
}

/// Bind the listener and spawn the accept loop.
pub async fn spawn(
  cfg: Arc<Config>,
  tls: Option<Arc<rustls::ServerConfig>>,
  log: Logger,
  shutdown: CancellationToken,
  qr: Option<QrClient>,
) -> std::io::Result<tokio::task::JoinHandle<()>> {
  let listener = TcpListener::bind(&cfg.listen_addr).await?;
  let inbound_tls = tls.is_some();
  info!(
      log,
      "listening for DICOM associations";
      "listen_addr" => &cfg.listen_addr,
      "ae_title" => &cfg.ae_title,
      "tls" => inbound_tls
  );
  let options = server_options(&cfg, tls, qr.as_ref());
  let conn_sem = Arc::new(Semaphore::new(cfg.max_concurrent_associations));

  let handle = tokio::spawn(async move {
    let mut connections = tokio::task::JoinSet::new();
    loop {
      tokio::select! {
          _ = shutdown.cancelled() => break,
          accepted = listener.accept() => {
              let (stream, peer_addr) = match accepted {
                  Ok(v) => v,
                  Err(e) => {
                      warn!(log, "accept failed"; "error" => %e);
                      continue;
                  }
              };
              let permit = match conn_sem.clone().try_acquire_owned() {
                  Ok(p) => p,
                  Err(_) => {
                      warn!(log, "too many concurrent associations, dropping connection"; "peer" => %peer_addr);
                      drop(stream);
                      continue;
                  }
              };
              let peer_str = peer_addr.to_string();
              let conn_log = log.new(o!("peer" => peer_str));
              let cfg = cfg.clone();
              let options = options.clone();
              let qr = qr.clone();
              connections.spawn(async move {
                  let _permit = permit;
                  if inbound_tls {
                    match options.establish_tls_async(stream).await {
                      Ok(association) => serve(association, &cfg, qr, &conn_log).await,
                      Err(e) => debug!(conn_log, "association rejected"; "error" => %e),
                    }
                  } else {
                    match options.establish_async(stream).await {
                      Ok(association) => serve(association, &cfg, qr, &conn_log).await,
                      Err(e) => debug!(conn_log, "association rejected"; "error" => %e),
                    }
                  }
              });
          }
      }
    }
    while connections.join_next().await.is_some() {}
    info!(log, "SCP accept loop stopped");
  });
  Ok(handle)
}

async fn serve<S>(association: AsyncServerAssociation<S>, cfg: &Config, qr: Option<QrClient>, conn_log: &Logger)
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
  let peer_ae = association.peer_ae_title().to_string();
  let alog = conn_log.new(o!("calling_ae" => peer_ae));
  info!(
      alog,
      "association established";
      "accepted_presentation_contexts" => association
          .presentation_contexts()
          .iter()
          .filter(|pc| pc.reason == PresentationContextResultReason::Acceptance)
          .count() as u64
  );
  if let Err(e) = handle_association(association, cfg, qr, alog.clone()).await {
    warn!(alog, "association ended with error"; "error" => %e);
  }
  info!(alog, "association closed");
}

async fn handle_association<S>(
  mut association: AsyncServerAssociation<S>,
  cfg: &Config,
  qr: Option<QrClient>,
  log: Logger,
) -> Result<()>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
  let mut dataset_buffer: Vec<u8> = Vec::with_capacity(1024 * 1024);
  let mut pending = Pending::None;

  loop {
    let pdu = match association.receive().await {
      Ok(p) => p,
      Err(dicom_ul::association::Error::ReceivePdu { .. }) => break,
      Err(e) => {
        warn!(log, "unexpected receive error"; "error" => %e);
        break;
      }
    };

    match pdu {
      Pdu::PData { mut data } =>
        for data_value in &mut data {
          if data_value.value_type == PDataValueType::Data && !data_value.is_last {
            dataset_buffer.append(&mut data_value.data);
          } else if data_value.value_type == PDataValueType::Command && data_value.is_last {
            let obj = match dimse::decode_command(&data_value.data) {
              Ok(o) => o,
              Err(e) => {
                warn!(log, "malformed command set"; "error" => %e);
                continue;
              }
            };
            let command_field = dimse::command_field(&obj).unwrap_or(0);
            if command_field == dimse::C_ECHO_RQ {
              let msgid = dimse::uint16(&obj, TAG_MESSAGE_ID).unwrap_or(0);
              let rsp = dimse::create_cecho_rsp(msgid, dimse::STATUS_SUCCESS);
              send_command(&mut association, &rsp, data_value.presentation_context_id).await?;
            } else if command_field == dimse::C_STORE_RQ {
              pending = Pending::Store {
                msgid:            dimse::uint16(&obj, TAG_MESSAGE_ID).unwrap_or(0),
                sop_class_uid:    dimse::string(&obj, TAG_AFFECTED_SOP_CLASS_UID)
                  .unwrap_or("")
                  .to_string(),
                sop_instance_uid: dimse::string(&obj, TAG_AFFECTED_SOP_INSTANCE_UID)
                  .unwrap_or("")
                  .to_string(),
              };
              dataset_buffer.clear();
            } else if command_field == dimse::C_FIND_RQ || command_field == dimse::C_GET_RQ {
              pending = Pending::Query {
                kind:          if command_field == dimse::C_FIND_RQ {
                  QrKind::Find
                } else {
                  QrKind::Get
                },
                msgid:         dimse::uint16(&obj, TAG_MESSAGE_ID).unwrap_or(0),
                sop_class_uid: dimse::string(&obj, TAG_AFFECTED_SOP_CLASS_UID)
                  .unwrap_or("")
                  .to_string(),
                pc_id:         data_value.presentation_context_id,
              };
              dataset_buffer.clear();
            } else {
              warn!(
                  log,
                  "unsupported DIMSE command";
                  "command_field" => format!("{command_field:#06x}")
              );
            }
          } else if data_value.value_type == PDataValueType::Data && data_value.is_last {
            dataset_buffer.append(&mut data_value.data);
            match std::mem::replace(&mut pending, Pending::None) {
              Pending::Store {
                msgid,
                sop_class_uid,
                sop_instance_uid,
              } => {
                let status = match store_instance(
                  cfg,
                  &association,
                  data_value.presentation_context_id,
                  &dataset_buffer,
                  &log,
                )
                .await
                {
                  Ok(()) => dimse::STATUS_SUCCESS,
                  Err(e) => {
                    error!(
                        log,
                        "failed to spool object";
                        "sop_instance_uid" => &sop_instance_uid,
                        "error" => %e
                    );
                    dimse::STATUS_OUT_OF_RESOURCES
                  }
                };
                dataset_buffer.clear();
                let rsp = dimse::create_cstore_rsp(msgid, &sop_class_uid, &sop_instance_uid, status);
                send_command(&mut association, &rsp, data_value.presentation_context_id).await?;
              }
              Pending::Query {
                kind,
                msgid,
                sop_class_uid,
                pc_id,
              } => {
                let identifier = std::mem::take(&mut dataset_buffer);
                let peer_ae = association.peer_ae_title().to_string();
                let status = match qr.as_ref() {
                  None => Some(dimse::STATUS_SOP_CLASS_NOT_SUPPORTED),
                  Some(qr_client) if !qr_client.allows(&peer_ae) => {
                    warn!(log, "query/retrieve refused: calling AE title not allowed"; "operation" => ?kind);
                    Some(dimse::STATUS_NOT_AUTHORIZED)
                  }
                  Some(qr_client) => {
                    let req = qr::QrProxyRequest {
                      kind,
                      inbound_pc_id: pc_id,
                      inbound_msgid: msgid,
                      sop_class_uid: &sop_class_uid,
                      identifier: &identifier,
                      calling_ae_title: &cfg.ae_title,
                      max_pdu_length: cfg.max_pdu_length,
                    };
                    match qr::proxy(&mut association, &req, qr_client, &log).await {
                      Ok(()) => None,
                      Err(e) => {
                        warn!(log, "query/retrieve proxy failed"; "operation" => ?kind, "error" => %e);
                        Some(dimse::STATUS_OUT_OF_RESOURCES)
                      }
                    }
                  }
                };
                if let Some(status) = status {
                  let _ = qr::refuse(&mut association, kind, pc_id, msgid, &sop_class_uid, status).await;
                }
              }
              Pending::None => {
                dataset_buffer.clear();
                warn!(log, "unexpected data PDU with no pending command");
              }
            }
          }
        },
      Pdu::ReleaseRQ => {
        let _ = association.send(&Pdu::ReleaseRP).await;
        break;
      }
      Pdu::AbortRQ { source } => {
        info!(log, "association aborted by peer"; "source" => ?source);
        break;
      }
      _ => {}
    }
  }
  Ok(())
}

async fn send_command<S>(association: &mut AsyncServerAssociation<S>, cmd: &CommandSet, pc_id: u8) -> Result<()>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
  let data = dimse::encode_command(cmd);
  association
    .send(&Pdu::PData {
      data: vec![PDataValue {
        presentation_context_id: pc_id,
        value_type: PDataValueType::Command,
        is_last: true,
        data,
      }],
    })
    .await
    .map_err(|source| ScpError::SendPdu {
      source: Box::new(source),
    })?;
  Ok(())
}

async fn store_instance<S>(
  cfg: &Config,
  association: &AsyncServerAssociation<S>,
  pc_id: u8,
  dataset: &[u8],
  log: &Logger,
) -> Result<()>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
  let pc = association
    .presentation_contexts()
    .iter()
    .find(|pc| pc.id == pc_id)
    .context(MissingPresentationContextSnafu)?;
  let ts_uid = &pc.transfer_syntax;
  let ts = TransferSyntaxRegistry
    .get(ts_uid)
    .context(UnsupportedTransferSyntaxSnafu { ts_uid: ts_uid.clone() })?;

  let obj =
    InMemDicomObject::read_dataset_with_ts(dataset, ts).map_err(|e| ScpError::ReadDataset { source: Box::new(e) })?;
  let sop_class = obj
    .element(dicom_dictionary_std::tags::SOP_CLASS_UID)
    .ok()
    .and_then(|e| e.to_str().ok())
    .map(|s| s.trim_end_matches(['\0', ' ']).to_string())
    .context(MissingSopClassUidSnafu)?;
  let sop_instance = obj
    .element(dicom_dictionary_std::tags::SOP_INSTANCE_UID)
    .ok()
    .and_then(|e| e.to_str().ok())
    .map(|s| s.trim_end_matches(['\0', ' ']).to_string())
    .context(MissingSopInstanceUidSnafu)?;
  let file_meta = FileMetaTableBuilder::new()
    .media_storage_sop_class_uid(&sop_class)
    .media_storage_sop_instance_uid(&sop_instance)
    .transfer_syntax(ts_uid)
    .build()
    .map_err(|e| ScpError::BuildFileMeta { reason: e.to_string() })?;
  let file_obj = obj.with_exact_meta(file_meta);

  let calling_ae = association.peer_ae_title();
  let destinations = cfg.destinations_for_source(calling_ae);
  if destinations.is_empty() {
    return NoDestinationSnafu {
      calling_ae: calling_ae.to_string(),
    }
    .fail();
  }
  let dirs: Vec<std::path::PathBuf> = destinations.iter().map(|dest| cfg.queue_dir_for(&dest.name)).collect();
  let dest_count = destinations.len();
  let min_free_bytes = cfg.min_free_bytes;
  tokio::task::spawn_blocking(move || {
    let dir_refs: Vec<&std::path::Path> = dirs.iter().map(|p| p.as_path()).collect();
    queue::enqueue_fanout(&dir_refs, &file_obj, min_free_bytes)
  })
  .await
  .context(SpoolTaskPanickedSnafu)?
  .map_err(|source| ScpError::Spool {
    destination_count: dest_count,
    source,
  })?;
  info!(
      log,
      "object spooled";
      "sop_instance_uid" => sop_instance.to_string(),
      "sop_class_uid" => sop_class.to_string(),
      "bytes" => dataset.len() as u64
  );
  Ok(())
}
