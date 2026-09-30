//! Outbound DICOM Service Class User: forwards spooled objects to destinations.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use dicom_encoding::TransferSyntaxIndex;
use dicom_transfer_syntax_registry::TransferSyntaxRegistry;
use dicom_ul::association::client::{AsyncTlsStream, ClientAssociationOptions};
use dicom_ul::association::AsyncClientAssociation;
use dicom_ul::pdu::{PDataValue, PDataValueType, PresentationContextNegotiated};
use dicom_ul::Pdu;
use slog::{debug, info, warn, Logger};
use snafu::Snafu;
use tokio::io::AsyncWriteExt;

use crate::compression::{self, DicomFile};
use crate::config::Destination;
use crate::dimse::{self, TAG_STATUS};
use crate::queue::SpooledObject;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PresentationKey {
  pub abstract_syntax: String,
  pub transfer_syntax: String,
}

/// Outbound association over TLS or cleartext TCP, depending on destination
/// config.
pub enum ClientAssoc {
  Tls(Box<AsyncClientAssociation<AsyncTlsStream>>),
  Plain(Box<AsyncClientAssociation<tokio::net::TcpStream>>),
}

impl ClientAssoc {
  pub fn presentation_contexts(&self) -> &[PresentationContextNegotiated] {
    match self {
      Self::Tls(a) => a.presentation_contexts(),
      Self::Plain(a) => a.presentation_contexts(),
    }
  }

  pub fn acceptor_max_pdu_length(&self) -> u32 {
    match self {
      Self::Tls(a) => a.acceptor_max_pdu_length(),
      Self::Plain(a) => a.acceptor_max_pdu_length(),
    }
  }

  pub async fn send(&mut self, pdu: &Pdu) -> Result<(), ScuError> {
    match self {
      Self::Tls(a) => a.send(pdu).await.map_err(|e| ScuError::Io { source: Box::new(e) }),
      Self::Plain(a) => a.send(pdu).await.map_err(|e| ScuError::Io { source: Box::new(e) }),
    }
  }

  pub async fn receive(&mut self) -> Result<Pdu, ScuError> {
    match self {
      Self::Tls(a) => a.receive().await.map_err(|e| ScuError::Io { source: Box::new(e) }),
      Self::Plain(a) => a.receive().await.map_err(|e| ScuError::Io { source: Box::new(e) }),
    }
  }
}

#[derive(Debug, Clone)]
pub struct SpooledMeta {
  pub sop_class_uid:    String,
  pub sop_instance_uid: String,
  pub transfer_syntax:  String,
}

#[derive(Debug, Snafu)]
pub enum ScuError {
  #[snafu(display("could not read spooled file {}: {source}", path.display()))]
  ReadFile {
    path:   PathBuf,
    source: Box<dicom_object::ReadError>,
  },
  #[snafu(display("association with {} failed: {source}", ae_address))]
  Association {
    ae_address: String,
    source:     Box<dicom_ul::association::Error>,
  },
  #[snafu(display("no matching presentation context for SOP class {sop_class_uid} / TS {transfer_syntax}"))]
  NoPresentationContext {
    sop_class_uid:   String,
    transfer_syntax: String,
  },
  #[snafu(display("dataset re-encode failed: {source}"))]
  Encode { source: Box<dicom_object::WriteError> },
  #[snafu(display("send/receive failed: {source}"))]
  Io { source: Box<dicom_ul::association::Error> },
  #[snafu(display("write P-Data failed: {source}"))]
  WritePData { source: std::io::Error },
  #[snafu(display("destination refused storage of {sop_instance_uid}: status {status:#06x}"))]
  StoreRefused {
    sop_instance_uid: String,
    status:           u16,
  },
  #[snafu(display("outbound session unavailable: {reason}"))]
  SessionUnavailable { reason: String },
  #[snafu(display("destination {:?} requires TLS but no client TLS config was provided", destination))]
  MissingClientTls { destination: String },
}

pub fn read_spooled_meta(path: &Path) -> Result<SpooledMeta, ScuError> { Ok(meta_of(&open_spooled(path)?)) }

pub fn open_spooled(path: &Path) -> Result<DicomFile, ScuError> {
  dicom_object::open_file(path).map_err(|e| ScuError::ReadFile {
    path:   path.to_path_buf(),
    source: Box::new(e),
  })
}

pub fn meta_of(file: &DicomFile) -> SpooledMeta {
  let meta = file.meta();
  SpooledMeta {
    sop_class_uid:    meta
      .media_storage_sop_class_uid
      .trim_end_matches(['\0', ' '])
      .to_string(),
    sop_instance_uid: meta
      .media_storage_sop_instance_uid
      .trim_end_matches(['\0', ' '])
      .to_string(),
    transfer_syntax:  meta.transfer_syntax.trim_end_matches(['\0', ' ']).to_string(),
  }
}

/// Presentation contexts to propose for an object: the compression target (if
/// any) and the original transfer syntax as fallback, each in its own context
/// so either can be accepted independently.
pub fn presentation_keys_for_meta(meta: &SpooledMeta, target: Option<&str>) -> Vec<PresentationKey> {
  let key = |ts: &str| PresentationKey {
    abstract_syntax: meta.sop_class_uid.clone(),
    transfer_syntax: ts.to_string(),
  };
  match target {
    Some(ts) if ts != meta.transfer_syntax => vec![key(ts), key(&meta.transfer_syntax)],
    _ => vec![key(&meta.transfer_syntax)],
  }
}

/// Choose what to send: the object transcoded to `target` when the
/// association accepted it and transcoding succeeds, otherwise the original.
pub async fn prepare_object(
  accepted: &[PresentationContextNegotiated],
  file: DicomFile,
  meta: SpooledMeta,
  target: Option<&'static str>,
  log: &Logger,
) -> (Arc<DicomFile>, SpooledMeta) {
  let file = Arc::new(file);
  let Some(target) = target else {
    return (file, meta);
  };
  if !accepted
    .iter()
    .any(|pc| pc.abstract_syntax == meta.sop_class_uid && pc.transfer_syntax == target)
  {
    debug!(
        log,
        "destination did not accept compressed transfer syntax, sending original";
        "sop_instance_uid" => &meta.sop_instance_uid,
        "transfer_syntax" => target
    );
    return (file, meta);
  }

  let started = Instant::now();
  let source = file.clone();
  match tokio::task::spawn_blocking(move || compression::transcode(&source, target)).await {
    Ok(Ok(transcoded)) => {
      debug!(
          log,
          "object transcoded";
          "sop_instance_uid" => &meta.sop_instance_uid,
          "from" => &meta.transfer_syntax,
          "to" => target,
          "duration_ms" => started.elapsed().as_millis() as u64
      );
      let meta = SpooledMeta {
        transfer_syntax: target.to_string(),
        ..meta
      };
      (Arc::new(transcoded), meta)
    }
    Ok(Err(e)) => {
      warn!(
          log,
          "transcode failed, sending original";
          "sop_instance_uid" => &meta.sop_instance_uid,
          "error" => %e
      );
      (file, meta)
    }
    Err(e) => {
      warn!(
          log,
          "transcode task panicked, sending original";
          "sop_instance_uid" => &meta.sop_instance_uid,
          "error" => %e
      );
      (file, meta)
    }
  }
}

/// Open an association with the given presentation contexts.
pub async fn connect(
  destination: &Destination,
  client_tls: Option<Arc<rustls::ClientConfig>>,
  calling_ae_title: &str,
  max_pdu_length: u32,
  pcs: &[PresentationKey],
) -> Result<ClientAssoc, ScuError> {
  let ae_address = format!("{}@{}:{}", destination.ae_title, destination.host, destination.port);

  let mut options = ClientAssociationOptions::new()
    .calling_ae_title(calling_ae_title.to_string())
    .called_ae_title(destination.ae_title.clone())
    .max_pdu_length(max_pdu_length);

  for pc in pcs {
    options = options.with_presentation_context(pc.abstract_syntax.clone(), vec![pc.transfer_syntax.clone()]);
  }

  if destination.tls {
    let tls_cfg = client_tls.ok_or_else(|| ScuError::MissingClientTls {
      destination: destination.name.clone(),
    })?;
    let server_name = destination
      .server_name
      .clone()
      .unwrap_or_else(|| destination.host.clone());
    options = options.tls_config(tls_cfg).server_name(&server_name);
    options
      .establish_with_async_tls(&ae_address)
      .await
      .map_err(|e| ScuError::Association {
        ae_address: ae_address.clone(),
        source:     Box::new(e),
      })
      .map(|a| ClientAssoc::Tls(Box::new(a)))
  } else {
    options
      .establish_with_async(&ae_address)
      .await
      .map_err(|e| ScuError::Association {
        ae_address: ae_address.clone(),
        source:     Box::new(e),
      })
      .map(|a| ClientAssoc::Plain(Box::new(a)))
  }
}

pub async fn release(assoc: ClientAssoc) -> Result<(), ScuError> {
  match assoc {
    ClientAssoc::Tls(a) => a.release().await.map_err(|e| ScuError::Io { source: Box::new(e) }),
    ClientAssoc::Plain(a) => a.release().await.map_err(|e| ScuError::Io { source: Box::new(e) }),
  }
}

/// Forward one spooled object over a fresh association (tests / fallback).
pub async fn forward(
  destination: &Destination,
  client_tls: Option<Arc<rustls::ClientConfig>>,
  spooled: &SpooledObject,
  calling_ae_title: &str,
  max_pdu_length: u32,
  log: &Logger,
) -> Result<(), ScuError> {
  let file = open_spooled(&spooled.dcm_path)?;
  let meta = meta_of(&file);
  let target = destination.compression.target_for(&file);
  let pcs = presentation_keys_for_meta(&meta, target);
  let mut assoc = connect(destination, client_tls, calling_ae_title, max_pdu_length, &pcs).await?;
  let (file, meta) = prepare_object(assoc.presentation_contexts(), file, meta, target, log).await;
  let result = send_object(&mut assoc, &file, &meta, log).await;
  let _ = release(assoc).await;
  let bytes = result?;

  info!(
      log,
      "object forwarded";
      "destination" => &destination.name,
      "sop_instance_uid" => &meta.sop_instance_uid,
      "transfer_syntax" => &meta.transfer_syntax,
      "bytes" => bytes as u64,
      "attempt" => spooled.attempts + 1
  );
  Ok(())
}

pub async fn send_object(
  assoc: &mut ClientAssoc,
  file: &dicom_object::FileDicomObject<dicom_object::InMemDicomObject>,
  meta: &SpooledMeta,
  log: &Logger,
) -> Result<usize, ScuError> {
  let sop_class_uid = &meta.sop_class_uid;
  let sop_instance_uid = &meta.sop_instance_uid;
  let transfer_syntax = &meta.transfer_syntax;

  let pc = assoc
    .presentation_contexts()
    .iter()
    .find(|pc| pc.abstract_syntax == *sop_class_uid && pc.transfer_syntax == *transfer_syntax)
    .cloned()
    .ok_or_else(|| ScuError::NoPresentationContext {
      sop_class_uid:   sop_class_uid.clone(),
      transfer_syntax: transfer_syntax.clone(),
    })?;

  static MESSAGE_ID: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(1);
  let message_id = MESSAGE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed).max(1);

  let cmd = dimse::create_cstore_rq(message_id, sop_class_uid, sop_instance_uid, dimse::PRIORITY_MEDIUM);
  let cmd_data = dimse::encode_command(&cmd);

  let ts = TransferSyntaxRegistry
    .get(transfer_syntax)
    .ok_or_else(|| ScuError::NoPresentationContext {
      sop_class_uid:   sop_class_uid.clone(),
      transfer_syntax: transfer_syntax.clone(),
    })?;
  let mut object_data = Vec::with_capacity(2048);
  file
    .write_dataset_with_ts(&mut object_data, ts)
    .map_err(|e| ScuError::Encode { source: Box::new(e) })?;

  let bytes = object_data.len();
  if cmd_data.len() + object_data.len() < assoc.acceptor_max_pdu_length().saturating_sub(100) as usize {
    assoc
      .send(&Pdu::PData {
        data: vec![
          PDataValue {
            presentation_context_id: pc.id,
            value_type:              PDataValueType::Command,
            is_last:                 true,
            data:                    cmd_data,
          },
          PDataValue {
            presentation_context_id: pc.id,
            value_type:              PDataValueType::Data,
            is_last:                 true,
            data:                    object_data,
          },
        ],
      })
      .await?;
  } else {
    assoc
      .send(&Pdu::PData {
        data: vec![PDataValue {
          presentation_context_id: pc.id,
          value_type:              PDataValueType::Command,
          is_last:                 true,
          data:                    cmd_data,
        }],
      })
      .await?;
    match assoc {
      ClientAssoc::Tls(a) => {
        a.send_pdata(pc.id)
          .write_all(&object_data)
          .await
          .map_err(|e| ScuError::WritePData { source: e })?;
      }
      ClientAssoc::Plain(a) => {
        a.send_pdata(pc.id)
          .write_all(&object_data)
          .await
          .map_err(|e| ScuError::WritePData { source: e })?;
      }
    }
  }

  let rsp_pdu = assoc.receive().await?;
  match rsp_pdu {
    Pdu::PData { data } => {
      let cmd_obj = dimse::decode_command(&data[0].data).map_err(|_| ScuError::StoreRefused {
        sop_instance_uid: sop_instance_uid.clone(),
        status:           0xFFFF,
      })?;
      let status = dimse::uint16(&cmd_obj, TAG_STATUS).ok_or_else(|| ScuError::StoreRefused {
        sop_instance_uid: sop_instance_uid.clone(),
        status:           0xFFFF,
      })?;
      match status {
        dimse::STATUS_SUCCESS => Ok(bytes),
        0x0001 | 0x0107 | 0x0116 | 0xB000..=0xBFFF => {
          warn!(
              log,
              "destination stored with warning";
              "sop_instance_uid" => sop_instance_uid,
              "status" => format!("{status:#06x}")
          );
          Ok(bytes)
        }
        _ => Err(ScuError::StoreRefused {
          sop_instance_uid: sop_instance_uid.clone(),
          status,
        }),
      }
    }
    pdu => {
      debug!(log, "unexpected PDU while awaiting C-STORE-RSP"; "pdu" => ?pdu);
      Err(ScuError::StoreRefused {
        sop_instance_uid: sop_instance_uid.clone(),
        status:           0xFFFF,
      })
    }
  }
}

/// Build presentation context list for reconnect (union), capped at 128.
pub fn merge_presentation_keys(
  existing: &HashSet<(String, String)>,
  keys: &[PresentationKey],
) -> Result<Vec<PresentationKey>, ScuError> {
  let mut set = existing.clone();
  for key in keys {
    set.insert((key.abstract_syntax.clone(), key.transfer_syntax.clone()));
  }
  if set.len() > 128 {
    let key = &keys[0];
    return Err(ScuError::NoPresentationContext {
      sop_class_uid:   key.abstract_syntax.clone(),
      transfer_syntax: key.transfer_syntax.clone(),
    });
  }
  Ok(
    set
      .into_iter()
      .map(|(abstract_syntax, transfer_syntax)| PresentationKey {
        abstract_syntax,
        transfer_syntax,
      })
      .collect(),
  )
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn merge_presentation_keys_adds_new_meta() {
    let existing = HashSet::from([(
      "1.2.840.10008.5.1.4.1.1.2".to_string(),
      "1.2.840.10008.1.2.1".to_string(),
    )]);
    let meta = SpooledMeta {
      sop_class_uid:    "1.2.840.10008.5.1.4.1.1.2".to_string(),
      sop_instance_uid: "1.2.3".to_string(),
      transfer_syntax:  "1.2.840.10008.1.2".to_string(),
    };
    let merged = merge_presentation_keys(&existing, &presentation_keys_for_meta(&meta, None)).unwrap();
    assert_eq!(merged.len(), 2);
  }

  #[test]
  fn presentation_keys_propose_target_then_original() {
    let meta = SpooledMeta {
      sop_class_uid:    "1.2.840.10008.5.1.4.1.1.2".to_string(),
      sop_instance_uid: "1.2.3".to_string(),
      transfer_syntax:  "1.2.840.10008.1.2.1".to_string(),
    };
    let ts = |keys: Vec<PresentationKey>| keys.into_iter().map(|k| k.transfer_syntax).collect::<Vec<_>>();
    assert_eq!(ts(presentation_keys_for_meta(&meta, None)), vec!["1.2.840.10008.1.2.1"]);
    assert_eq!(
      ts(presentation_keys_for_meta(&meta, Some("1.2.840.10008.1.2.4.110"))),
      vec!["1.2.840.10008.1.2.4.110", "1.2.840.10008.1.2.1"]
    );
    assert_eq!(
      ts(presentation_keys_for_meta(&meta, Some("1.2.840.10008.1.2.1"))),
      vec!["1.2.840.10008.1.2.1"]
    );
  }
}
