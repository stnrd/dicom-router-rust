//! Proxy inbound C-FIND / C-GET to one configured destination.

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use dicom_dictionary_std::uids;
use dicom_ul::association::AsyncServerAssociation;
use dicom_ul::pdu::{PDataValue, PDataValueType, PresentationContextResultReason};
use dicom_ul::Pdu;
use slog::{info, Logger};
use snafu::Snafu;

use crate::config::Destination;
use crate::dimse::{self, TAG_AFFECTED_SOP_CLASS_UID, TAG_MESSAGE_ID, TAG_MESSAGE_ID_BEING_RESPONDED_TO, TAG_STATUS};
use crate::scp::ABSTRACT_SYNTAXES;
use crate::scu::{self, PresentationKey, RoleSelection, ScuError};

pub const QR_SOP_CLASSES: &[&str] = &[
  dimse::PATIENT_ROOT_FIND_SOP_CLASS_UID,
  dimse::STUDY_ROOT_FIND_SOP_CLASS_UID,
  dimse::PATIENT_ROOT_GET_SOP_CLASS_UID,
  dimse::STUDY_ROOT_GET_SOP_CLASS_UID,
];

#[derive(Clone)]
pub struct QrClient {
  pub destination: Destination,
  pub client_tls:  Option<Arc<rustls::ClientConfig>>,
}

pub struct QrProxyRequest<'a> {
  pub inbound_pc_id:    u8,
  pub inbound_msgid:    u16,
  pub sop_class_uid:    &'a str,
  pub identifier:       &'a [u8],
  pub calling_ae_title: &'a str,
  pub max_pdu_length:   u32,
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

fn map_io(e: dicom_ul::association::Error) -> QrError { QrError::Io { source: Box::new(e) } }

fn map_scu(e: ScuError) -> QrError {
  match e {
    ScuError::Io { source } => QrError::Io { source },
    other => QrError::Outbound { source: other },
  }
}

pub async fn proxy_find<S>(
  inbound: &mut AsyncServerAssociation<S>,
  req: &QrProxyRequest<'_>,
  qr: &QrClient,
  log: &Logger,
) -> Result<(), QrError>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
  let pcs = [PresentationKey {
    abstract_syntax: req.sop_class_uid.to_string(),
    transfer_syntax: dicom_dictionary_std::uids::EXPLICIT_VR_LITTLE_ENDIAN.to_string(),
  }];
  let mut outbound = scu::connect(
    &qr.destination,
    qr.client_tls.clone(),
    req.calling_ae_title,
    req.max_pdu_length,
    &pcs,
  )
  .await
  .map_err(|source| QrError::Outbound { source })?;

  let outbound_pc = outbound
    .presentation_contexts()
    .iter()
    .find(|pc| pc.abstract_syntax == req.sop_class_uid)
    .ok_or_else(|| QrError::NoPresentationContext {
      sop_class_uid: req.sop_class_uid.to_string(),
    })?;
  let outbound_pc_id = outbound_pc.id;

  let out_cmd = dimse::create_cfind_rq(1, req.sop_class_uid, dimse::PRIORITY_MEDIUM);
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
          data:                    req.identifier.to_vec(),
        },
      ],
    })
    .await
    .map_err(map_scu)?;

  loop {
    let pdu = outbound.receive().await.map_err(map_scu)?;
    match pdu {
      Pdu::PData { data } => {
        let mut relayed = Vec::with_capacity(data.len());
        let mut final_status = None;
        for dv in data {
          if dv.value_type == PDataValueType::Command && dv.is_last {
            let mut cmd = dimse::decode_command(&dv.data).map_err(|source| QrError::Command { source })?;
            cmd.set_u16(dimse::TAG_MESSAGE_ID_BEING_RESPONDED_TO, req.inbound_msgid);
            let status = dimse::uint16(&cmd, dimse::TAG_STATUS).unwrap_or(dimse::STATUS_CANNOT_UNDERSTAND);
            final_status = Some(status);
            relayed.push(PDataValue {
              presentation_context_id: req.inbound_pc_id,
              value_type:              PDataValueType::Command,
              is_last:                 true,
              data:                    dimse::encode_command(&cmd),
            });
          } else {
            relayed.push(PDataValue {
              presentation_context_id: req.inbound_pc_id,
              value_type:              dv.value_type,
              is_last:                 dv.is_last,
              data:                    dv.data,
            });
          }
        }
        inbound.send(&Pdu::PData { data: relayed }).await.map_err(map_io)?;
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
  let rsp = dimse::create_cfind_rsp(inbound_msgid, sop_class_uid, dimse::STATUS_OUT_OF_RESOURCES, false);
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

pub async fn proxy_get<S>(
  inbound: &mut AsyncServerAssociation<S>,
  req: &QrProxyRequest<'_>,
  qr: &QrClient,
  log: &Logger,
) -> Result<(), QrError>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
  let evrle = uids::EXPLICIT_VR_LITTLE_ENDIAN.to_string();
  let mut pcs = vec![PresentationKey {
    abstract_syntax: req.sop_class_uid.to_string(),
    transfer_syntax: evrle.clone(),
  }];
  let mut roles = Vec::new();
  for uid in ABSTRACT_SYNTAXES {
    if *uid == uids::VERIFICATION {
      continue;
    }
    pcs.push(PresentationKey {
      abstract_syntax: uid.to_string(),
      transfer_syntax: evrle.clone(),
    });
    roles.push(RoleSelection {
      sop_class: uid.to_string(),
      scu:       false,
      scp:       true,
    });
  }

  let mut outbound = scu::connect_with_roles(
    &qr.destination,
    qr.client_tls.clone(),
    req.calling_ae_title,
    req.max_pdu_length,
    &pcs,
    &roles,
  )
  .await
  .map_err(|source| QrError::Outbound { source })?;

  let outbound_pc = outbound
    .presentation_contexts()
    .iter()
    .find(|pc| pc.abstract_syntax == req.sop_class_uid)
    .ok_or_else(|| QrError::NoPresentationContext {
      sop_class_uid: req.sop_class_uid.to_string(),
    })?;
  let outbound_pc_id = outbound_pc.id;

  let out_cmd = dimse::create_cget_rq(1, req.sop_class_uid, dimse::PRIORITY_MEDIUM);
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
          data:                    req.identifier.to_vec(),
        },
      ],
    })
    .await
    .map_err(map_scu)?;

  static STORE_MSG_ID: AtomicU16 = AtomicU16::new(100);

  loop {
    let pdu = outbound.receive().await.map_err(map_scu)?;
    match pdu {
      Pdu::PData { data } => {
        let cmd_dv = data
          .iter()
          .find(|dv| dv.value_type == PDataValueType::Command && dv.is_last);
        let Some(cmd_dv) = cmd_dv else {
          continue;
        };
        let cmd = dimse::decode_command(&cmd_dv.data).map_err(|source| QrError::Command { source })?;
        let command_field = dimse::command_field(&cmd).unwrap_or(0);

        if command_field == dimse::C_STORE_RQ {
          let outbound_store_msgid = dimse::uint16(&cmd, TAG_MESSAGE_ID).unwrap_or(0);
          let store_sop_class = dimse::string(&cmd, TAG_AFFECTED_SOP_CLASS_UID)
            .unwrap_or("")
            .to_string();
          let inbound_storage_pc = inbound
            .presentation_contexts()
            .iter()
            .find(|pc| {
              pc.abstract_syntax == store_sop_class && pc.reason == PresentationContextResultReason::Acceptance
            })
            .ok_or_else(|| QrError::NoPresentationContext {
              sop_class_uid: store_sop_class.clone(),
            })?;
          let inbound_storage_pc_id = inbound_storage_pc.id;
          let inbound_store_msgid = STORE_MSG_ID.fetch_add(1, Ordering::Relaxed).max(1);

          let mut relay_to_inbound = Vec::with_capacity(data.len());
          for dv in &data {
            if dv.value_type == PDataValueType::Command && dv.is_last {
              let mut store_cmd = dimse::decode_command(&dv.data).map_err(|source| QrError::Command { source })?;
              store_cmd.set_u16(TAG_MESSAGE_ID, inbound_store_msgid);
              relay_to_inbound.push(PDataValue {
                presentation_context_id: inbound_storage_pc_id,
                value_type:              PDataValueType::Command,
                is_last:                 true,
                data:                    dimse::encode_command(&store_cmd),
              });
            } else {
              relay_to_inbound.push(PDataValue {
                presentation_context_id: inbound_storage_pc_id,
                value_type:              dv.value_type.clone(),
                is_last:                 dv.is_last,
                data:                    dv.data.clone(),
              });
            }
          }
          inbound
            .send(&Pdu::PData { data: relay_to_inbound })
            .await
            .map_err(map_io)?;

          loop {
            let inbound_pdu = inbound.receive().await.map_err(map_io)?;
            match inbound_pdu {
              Pdu::PData { data: inbound_data } => {
                let rsp_cmd =
                  dimse::decode_command(&inbound_data[0].data).map_err(|source| QrError::Command { source })?;
                if dimse::command_field(&rsp_cmd) == Some(dimse::C_STORE_RSP) {
                  let mut relay_rsp = rsp_cmd;
                  relay_rsp.set_u16(TAG_MESSAGE_ID_BEING_RESPONDED_TO, outbound_store_msgid);
                  outbound
                    .send(&Pdu::PData {
                      data: vec![PDataValue {
                        presentation_context_id: cmd_dv.presentation_context_id,
                        value_type:              PDataValueType::Command,
                        is_last:                 true,
                        data:                    dimse::encode_command(&relay_rsp),
                      }],
                    })
                    .await
                    .map_err(map_scu)?;
                  break;
                }
              }
              Pdu::AbortRQ { .. } | Pdu::ReleaseRQ => {
                return Err(QrError::NoPresentationContext {
                  sop_class_uid: store_sop_class,
                });
              }
              _ => {}
            }
          }
        } else if command_field == dimse::C_GET_RSP {
          let mut relayed = Vec::with_capacity(data.len());
          let mut final_status = None;
          for dv in data {
            if dv.value_type == PDataValueType::Command && dv.is_last {
              let mut get_rsp = dimse::decode_command(&dv.data).map_err(|source| QrError::Command { source })?;
              get_rsp.set_u16(TAG_MESSAGE_ID_BEING_RESPONDED_TO, req.inbound_msgid);
              let status = dimse::uint16(&get_rsp, TAG_STATUS).unwrap_or(dimse::STATUS_CANNOT_UNDERSTAND);
              final_status = Some(status);
              relayed.push(PDataValue {
                presentation_context_id: req.inbound_pc_id,
                value_type:              PDataValueType::Command,
                is_last:                 true,
                data:                    dimse::encode_command(&get_rsp),
              });
            } else {
              relayed.push(PDataValue {
                presentation_context_id: req.inbound_pc_id,
                value_type:              dv.value_type,
                is_last:                 dv.is_last,
                data:                    dv.data,
              });
            }
          }
          inbound.send(&Pdu::PData { data: relayed }).await.map_err(map_io)?;
          if let Some(st) = final_status {
            if st != dimse::STATUS_PENDING {
              break;
            }
          }
        }
      }
      Pdu::AbortRQ { .. } | Pdu::ReleaseRQ => break,
      _ => {}
    }
  }

  let _ = scu::release(outbound).await;
  info!(log, "C-GET proxy finished"; "destination" => &qr.destination.name);
  Ok(())
}

pub async fn refuse_get<S>(
  inbound: &mut AsyncServerAssociation<S>,
  inbound_pc_id: u8,
  inbound_msgid: u16,
  sop_class_uid: &str,
) -> Result<(), QrError>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
  let rsp = dimse::create_cget_rsp(inbound_msgid, sop_class_uid, dimse::STATUS_OUT_OF_RESOURCES, 0, 0, 0, 0);
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
