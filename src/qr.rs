//! Proxy inbound C-FIND / C-GET to one TLS destination.

use std::sync::Arc;

use dicom_ul::association::AsyncServerAssociation;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use dicom_ul::Pdu;
use slog::{info, Logger};
use snafu::Snafu;

use crate::config::Destination;
use crate::dimse;
use crate::scu::{self, PresentationKey, ScuError};

pub const QR_SOP_CLASSES: &[&str] = &[
  dimse::PATIENT_ROOT_FIND_SOP_CLASS_UID,
  dimse::STUDY_ROOT_FIND_SOP_CLASS_UID,
  dimse::PATIENT_ROOT_GET_SOP_CLASS_UID,
  dimse::STUDY_ROOT_GET_SOP_CLASS_UID,
];

#[derive(Clone)]
pub struct QrClient {
  pub destination: Destination,
  pub client_tls:  Arc<rustls::ClientConfig>,
}

#[derive(Debug, Snafu)]
pub enum QrError {
  #[snafu(display("QR outbound association failed: {source}"))]
  Outbound { source: ScuError },
  #[snafu(display("QR PDU send/receive failed: {source}"))]
  Io { source: Box<dicom_ul::association::Error> },
  #[snafu(display("malformed DIMSE command: {source}"))]
  Command { source: dimse::DimseError },
  #[snafu(display("no presentation context for {sop_class_uid}"))]
  NoPresentationContext { sop_class_uid: String },
}

fn map_io(e: dicom_ul::association::Error) -> QrError {
  QrError::Io { source: Box::new(e) }
}

pub async fn proxy_find<S>(
  inbound: &mut AsyncServerAssociation<S>,
  inbound_pc_id: u8,
  inbound_msgid: u16,
  sop_class_uid: &str,
  identifier: &[u8],
  qr: &QrClient,
  calling_ae_title: &str,
  max_pdu_length: u32,
  log: &Logger,
) -> Result<(), QrError>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
  let pcs = [PresentationKey {
    abstract_syntax: sop_class_uid.to_string(),
    transfer_syntax: dicom_dictionary_std::uids::EXPLICIT_VR_LITTLE_ENDIAN.to_string(),
  }];
  let mut outbound = scu::connect(
    &qr.destination,
    qr.client_tls.clone(),
    calling_ae_title,
    max_pdu_length,
    &pcs,
  )
  .await
  .map_err(|source| QrError::Outbound { source })?;

  let outbound_pc = outbound
    .presentation_contexts()
    .iter()
    .find(|pc| pc.abstract_syntax == sop_class_uid)
    .ok_or_else(|| QrError::NoPresentationContext {
      sop_class_uid: sop_class_uid.to_string(),
    })?;
  let outbound_pc_id = outbound_pc.id;

  let out_cmd = dimse::create_cfind_rq(1, sop_class_uid, dimse::PRIORITY_MEDIUM);
  outbound
    .send(&Pdu::PData {
      data: vec![
        PDataValue {
          presentation_context_id: outbound_pc_id,
          value_type:              PDataValueType::Command,
          is_last:                 true,
          data:                    dimse::encode_command(&out_cmd),
        },
        PDataValue {
          presentation_context_id: outbound_pc_id,
          value_type:              PDataValueType::Data,
          is_last:                 true,
          data:                    identifier.to_vec(),
        },
      ],
    })
    .await
    .map_err(map_io)?;

  loop {
    let pdu = outbound.receive().await.map_err(map_io)?;
    match pdu {
      Pdu::PData { data } => {
        let mut relayed = Vec::with_capacity(data.len());
        let mut final_status = None;
        for dv in data {
          if dv.value_type == PDataValueType::Command && dv.is_last {
            let mut cmd = dimse::decode_command(&dv.data).map_err(|source| QrError::Command { source })?;
            cmd.set_u16(dimse::TAG_MESSAGE_ID_BEING_RESPONDED_TO, inbound_msgid);
            let status = dimse::uint16(&cmd, dimse::TAG_STATUS).unwrap_or(dimse::STATUS_CANNOT_UNDERSTAND);
            final_status = Some(status);
            relayed.push(PDataValue {
              presentation_context_id: inbound_pc_id,
              value_type:              PDataValueType::Command,
              is_last:                 true,
              data:                    dimse::encode_command(&cmd),
            });
          } else {
            relayed.push(PDataValue {
              presentation_context_id: inbound_pc_id,
              value_type:              dv.value_type,
              is_last:                 dv.is_last,
              data:                    dv.data,
            });
          }
        }
        inbound
          .send(&Pdu::PData { data: relayed })
          .await
          .map_err(map_io)?;
        if let Some(st) = final_status {
          if st != dimse::STATUS_PENDING {
            break;
          }
        }
      }
      Pdu::AbortRQ { .. } | Pdu::ReleaseRQ => break,
      _ => {}
    }
  }

  let _ = scu::release(outbound).await;
  info!(log, "C-FIND proxy finished"; "destination" => &qr.destination.name);
  Ok(())
}

pub async fn refuse_find<S>(
  inbound: &mut AsyncServerAssociation<S>,
  inbound_pc_id: u8,
  inbound_msgid: u16,
  sop_class_uid: &str,
) -> Result<(), QrError>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
  let rsp = dimse::create_cfind_rsp(
    inbound_msgid,
    sop_class_uid,
    dimse::STATUS_OUT_OF_RESOURCES,
    false,
  );
  inbound
    .send(&Pdu::PData {
      data: vec![PDataValue {
        presentation_context_id: inbound_pc_id,
        value_type:              PDataValueType::Command,
        is_last:                 true,
        data:                    dimse::encode_command(&rsp),
      }],
    })
    .await
    .map_err(map_io)
}
