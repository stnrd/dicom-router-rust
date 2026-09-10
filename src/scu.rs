//! Outbound DICOM Service Class User: forwards spooled objects over TLS.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dicom_encoding::TransferSyntaxIndex;
use dicom_transfer_syntax_registry::TransferSyntaxRegistry;
use dicom_ul::association::client::{AsyncTlsStream, ClientAssociationOptions};
use dicom_ul::association::AsyncClientAssociation;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use dicom_ul::Pdu;
use slog::{debug, info, warn, Logger};
use snafu::Snafu;
use tokio::io::AsyncWriteExt;

use crate::config::Destination;
use crate::dimse::{self, TAG_STATUS};
use crate::queue::SpooledObject;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PresentationKey {
  pub abstract_syntax: String,
  pub transfer_syntax: String,
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
}

pub fn read_spooled_meta(path: &Path) -> Result<SpooledMeta, ScuError> {
  let file = dicom_object::open_file(path).map_err(|e| ScuError::ReadFile {
    path:   path.to_path_buf(),
    source: Box::new(e),
  })?;
  let meta = file.meta();
  Ok(SpooledMeta {
    sop_class_uid:    meta
      .media_storage_sop_class_uid
      .trim_end_matches(['\0', ' '])
      .to_string(),
    sop_instance_uid: meta
      .media_storage_sop_instance_uid
      .trim_end_matches(['\0', ' '])
      .to_string(),
    transfer_syntax:  meta.transfer_syntax.trim_end_matches(['\0', ' ']).to_string(),
  })
}

pub fn presentation_keys_for_meta(meta: &SpooledMeta) -> Vec<PresentationKey> {
  vec![PresentationKey {
    abstract_syntax: meta.sop_class_uid.clone(),
    transfer_syntax: meta.transfer_syntax.clone(),
  }]
}

/// Open a TLS association with the given presentation contexts.
pub async fn connect(
  destination: &Destination,
  client_tls: Arc<rustls::ClientConfig>,
  calling_ae_title: &str,
  max_pdu_length: u32,
  pcs: &[PresentationKey],
) -> Result<AsyncClientAssociation<AsyncTlsStream>, ScuError> {
  let ae_address = format!("{}@{}:{}", destination.ae_title, destination.host, destination.port);
  let server_name = destination
    .server_name
    .clone()
    .unwrap_or_else(|| destination.host.clone());

  let mut options = ClientAssociationOptions::new()
    .calling_ae_title(calling_ae_title.to_string())
    .called_ae_title(destination.ae_title.clone())
    .max_pdu_length(max_pdu_length)
    .tls_config(client_tls)
    .server_name(&server_name);

  for pc in pcs {
    options = options.with_presentation_context(pc.abstract_syntax.clone(), vec![pc.transfer_syntax.clone()]);
  }

  options
    .establish_with_async_tls(&ae_address)
    .await
    .map_err(|e| ScuError::Association {
      ae_address: ae_address.clone(),
      source:     Box::new(e),
    })
}

pub async fn release<S>(assoc: AsyncClientAssociation<S>) -> Result<(), ScuError>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
  assoc.release().await.map_err(|e| ScuError::Io { source: Box::new(e) })
}

/// Forward one spooled object over a fresh TLS association (tests / fallback).
pub async fn forward(
  destination: &Destination,
  client_tls: Arc<rustls::ClientConfig>,
  spooled: &SpooledObject,
  calling_ae_title: &str,
  max_pdu_length: u32,
  log: &Logger,
) -> Result<(), ScuError> {
  let meta = read_spooled_meta(&spooled.dcm_path)?;
  let file = dicom_object::open_file(&spooled.dcm_path).map_err(|e| ScuError::ReadFile {
    path:   spooled.dcm_path.clone(),
    source: Box::new(e),
  })?;
  let pcs = presentation_keys_for_meta(&meta);
  let mut assoc = connect(destination, client_tls, calling_ae_title, max_pdu_length, &pcs).await?;
  let result = send_object(&mut assoc, &file, &meta, log).await;
  let _ = release(assoc).await;
  result?;

  info!(
      log,
      "object forwarded";
      "destination" => &destination.name,
      "sop_instance_uid" => &meta.sop_instance_uid,
      "attempt" => spooled.attempts + 1
  );
  Ok(())
}

pub async fn send_object<S>(
  assoc: &mut AsyncClientAssociation<S>,
  file: &dicom_object::FileDicomObject<dicom_object::InMemDicomObject>,
  meta: &SpooledMeta,
  log: &Logger,
) -> Result<(), ScuError>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
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

  let map_io = |e: dicom_ul::association::Error| ScuError::Io { source: Box::new(e) };
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
      .await
      .map_err(map_io)?;
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
      .await
      .map_err(map_io)?;
    let mut pdata = assoc.send_pdata(pc.id);
    pdata
      .write_all(&object_data)
      .await
      .map_err(|e| ScuError::WritePData { source: e })?;
  }

  let rsp_pdu = assoc.receive().await.map_err(map_io)?;
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
        dimse::STATUS_SUCCESS => Ok(()),
        0x0001 | 0x0107 | 0x0116 | 0xB000..=0xBFFF => {
          warn!(
              log,
              "destination stored with warning";
              "sop_instance_uid" => sop_instance_uid,
              "status" => format!("{status:#06x}")
          );
          Ok(())
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
  meta: &SpooledMeta,
) -> Result<Vec<PresentationKey>, ScuError> {
  let mut set = existing.clone();
  set.insert((meta.sop_class_uid.clone(), meta.transfer_syntax.clone()));
  if set.len() > 128 {
    return Err(ScuError::NoPresentationContext {
      sop_class_uid:   meta.sop_class_uid.clone(),
      transfer_syntax: meta.transfer_syntax.clone(),
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
    let merged = merge_presentation_keys(&existing, &meta).unwrap();
    assert_eq!(merged.len(), 2);
  }
}
