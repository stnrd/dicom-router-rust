//! Proxy inbound C-FIND / C-GET to one configured destination.
//!
//! The proxy works on whole DIMSE messages rather than PDUs: commands and
//! datasets are reassembled from however many P-DATA fragments the sender
//! used, then re-fragmented for the receiver's maximum PDU length. Both sides
//! are read concurrently so a C-CANCEL-RQ from the requestor reaches the
//! destination while responses are still flowing.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::Arc;

use dicom_dictionary_std::uids;
use dicom_encoding::TransferSyntaxIndex;
use dicom_object::InMemDicomObject;
use dicom_transfer_syntax_registry::TransferSyntaxRegistry;
use dicom_ul::association::AsyncServerAssociation;
use dicom_ul::pdu::{PDataValue, PDataValueType, PresentationContextNegotiated, PresentationContextResultReason};
use dicom_ul::Pdu;
use slog::{info, warn, Logger};
use snafu::Snafu;

use crate::config::Destination;
use crate::dimse::{self, CommandSet, TAG_MESSAGE_ID, TAG_MESSAGE_ID_BEING_RESPONDED_TO, TAG_STATUS};
use crate::scu::{self, ClientAssoc, PresentationKeys, RoleSelection, ScuError};

pub const QR_SOP_CLASSES: &[&str] = &[
  dimse::PATIENT_ROOT_FIND_SOP_CLASS_UID,
  dimse::STUDY_ROOT_FIND_SOP_CLASS_UID,
  dimse::PATIENT_ROOT_GET_SOP_CLASS_UID,
  dimse::STUDY_ROOT_GET_SOP_CLASS_UID,
];

/// Message ID used for the single request sent on each outbound association.
const OUTBOUND_MSGID: u16 = 1;

/// PDU header (6) + PDV item header (6).
const PDATA_OVERHEAD: usize = 12;

#[derive(Clone)]
pub struct QrClient {
  pub destination:       Destination,
  pub client_tls:        Option<Arc<rustls::ClientConfig>>,
  pub allowed_ae_titles: Vec<String>,
}

impl QrClient {
  pub fn allows(&self, calling_ae_title: &str) -> bool {
    self.allowed_ae_titles.iter().any(|t| t == calling_ae_title.trim())
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QrKind {
  Find,
  Get,
}

impl QrKind {
  pub fn rq_command(self) -> u16 {
    match self {
      Self::Find => dimse::C_FIND_RQ,
      Self::Get => dimse::C_GET_RQ,
    }
  }

  fn rsp_command(self) -> u16 {
    match self {
      Self::Find => dimse::C_FIND_RSP,
      Self::Get => dimse::C_GET_RSP,
    }
  }

  fn name(self) -> &'static str {
    match self {
      Self::Find => "C-FIND",
      Self::Get => "C-GET",
    }
  }
}

pub struct QrProxyRequest<'a> {
  pub kind:             QrKind,
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
  #[snafu(display("cannot convert dataset from {from} to {to}: {reason}"))]
  Recode {
    from:   String,
    to:     String,
    reason: String,
  },
  #[snafu(display("{side} peer ended the association during the operation"))]
  PeerClosed { side: &'static str },
  #[snafu(display("unexpected P-DATA fragment: {reason}"))]
  Protocol { reason: &'static str },
}

fn map_scu(e: ScuError) -> QrError {
  match e {
    ScuError::Io { source } => QrError::Io { source },
    other => QrError::Outbound { source: other },
  }
}

// --- PDU transport over either side
// ----------------------------------------------------------

/// One end of the proxy: the inbound requestor or the outbound destination.
pub trait Peer: Send {
  fn send_pdu(&mut self, pdu: &Pdu) -> impl Future<Output = Result<(), QrError>> + Send;
  /// Must be cancel-safe: the relay drops it inside `select!`.
  fn receive_pdu(&mut self) -> impl Future<Output = Result<Pdu, QrError>> + Send;
  fn peer_max_pdu_length(&self) -> u32;
  fn side(&self) -> &'static str;
}

impl<S> Peer for AsyncServerAssociation<S>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
  async fn send_pdu(&mut self, pdu: &Pdu) -> Result<(), QrError> {
    self.send(pdu).await.map_err(|e| QrError::Io { source: Box::new(e) })
  }

  async fn receive_pdu(&mut self) -> Result<Pdu, QrError> {
    self.receive().await.map_err(|e| QrError::Io { source: Box::new(e) })
  }

  fn peer_max_pdu_length(&self) -> u32 { self.requestor_max_pdu_length() }

  fn side(&self) -> &'static str { "inbound" }
}

impl Peer for ClientAssoc {
  async fn send_pdu(&mut self, pdu: &Pdu) -> Result<(), QrError> { self.send(pdu).await.map_err(map_scu) }

  async fn receive_pdu(&mut self) -> Result<Pdu, QrError> { self.receive().await.map_err(map_scu) }

  fn peer_max_pdu_length(&self) -> u32 { self.acceptor_max_pdu_length() }

  fn side(&self) -> &'static str { "outbound" }
}

/// A complete DIMSE message.
#[derive(Debug)]
pub struct Message {
  pub pc_id:   u8,
  pub command: CommandSet,
  pub dataset: Option<Vec<u8>>,
}

/// Reassembles DIMSE messages from P-DATA fragments.
///
/// All progress lives in the reader, so [`MessageReader::read`] is
/// cancel-safe as long as the peer's `receive_pdu` is.
#[derive(Default)]
pub struct MessageReader {
  queue:   VecDeque<PDataValue>,
  pc_id:   u8,
  command: Vec<u8>,
  header:  Option<CommandSet>,
  dataset: Vec<u8>,
}

impl MessageReader {
  pub async fn read<P: Peer>(&mut self, peer: &mut P) -> Result<Message, QrError> {
    loop {
      while let Some(pdv) = self.queue.pop_front() {
        if let Some(message) = self.push(pdv)? {
          return Ok(message);
        }
      }
      match peer.receive_pdu().await? {
        Pdu::PData { data } => self.queue.extend(data),
        Pdu::ReleaseRQ | Pdu::AbortRQ { .. } => return Err(QrError::PeerClosed { side: peer.side() }),
        _ => {}
      }
    }
  }

  fn push(&mut self, mut pdv: PDataValue) -> Result<Option<Message>, QrError> {
    match (pdv.value_type, self.header.is_some()) {
      (PDataValueType::Command, false) => {
        self.pc_id = pdv.presentation_context_id;
        self.command.append(&mut pdv.data);
        if !pdv.is_last {
          return Ok(None);
        }
        let command =
          dimse::decode_command(&std::mem::take(&mut self.command)).map_err(|source| QrError::Command { source })?;
        if dimse::uint16(&command, dimse::TAG_COMMAND_DATA_SET_TYPE) == Some(dimse::NO_DATA_SET) {
          return Ok(Some(Message {
            pc_id: self.pc_id,
            command,
            dataset: None,
          }));
        }
        self.header = Some(command);
        Ok(None)
      }
      (PDataValueType::Data, true) => {
        self.dataset.append(&mut pdv.data);
        if !pdv.is_last {
          return Ok(None);
        }
        Ok(Some(Message {
          pc_id:   self.pc_id,
          command: self.header.take().expect("checked above"),
          dataset: Some(std::mem::take(&mut self.dataset)),
        }))
      }
      (PDataValueType::Command, true) => Err(QrError::Protocol {
        reason: "command fragment while a dataset was expected",
      }),
      (PDataValueType::Data, false) => Err(QrError::Protocol {
        reason: "dataset fragment without a command",
      }),
    }
  }
}

/// Send `bytes` as one or more PDUs that each fit the peer's maximum length.
async fn send_fragments<P: Peer>(
  peer: &mut P,
  pc_id: u8,
  value_type: PDataValueType,
  bytes: &[u8],
) -> Result<(), QrError> {
  let max = match peer.peer_max_pdu_length() as usize {
    0 => usize::MAX,
    n => n,
  };
  let chunk = max.saturating_sub(PDATA_OVERHEAD).max(1);
  let mut chunks = bytes.chunks(chunk).peekable();
  if chunks.peek().is_none() {
    return peer
      .send_pdu(&Pdu::PData {
        data: vec![PDataValue {
          presentation_context_id: pc_id,
          value_type,
          is_last: true,
          data: Vec::new(),
        }],
      })
      .await;
  }
  while let Some(part) = chunks.next() {
    peer
      .send_pdu(&Pdu::PData {
        data: vec![PDataValue {
          presentation_context_id: pc_id,
          value_type:              value_type.clone(),
          is_last:                 chunks.peek().is_none(),
          data:                    part.to_vec(),
        }],
      })
      .await?;
  }
  Ok(())
}

pub async fn send_message<P: Peer>(
  peer: &mut P,
  pc_id: u8,
  command: &CommandSet,
  dataset: Option<&[u8]>,
) -> Result<(), QrError> {
  send_fragments(peer, pc_id, PDataValueType::Command, &dimse::encode_command(command)).await?;
  if let Some(bytes) = dataset {
    send_fragments(peer, pc_id, PDataValueType::Data, bytes).await?;
  }
  Ok(())
}

// --- Transfer syntax handling
// ----------------------------------------------------------------

/// Re-encode a (pixel-free) dataset between two native transfer syntaxes.
fn recode(bytes: Vec<u8>, from: &str, to: &str) -> Result<Vec<u8>, QrError> {
  if from == to {
    return Ok(bytes);
  }
  let err = |reason: String| QrError::Recode {
    from: from.to_string(),
    to: to.to_string(),
    reason,
  };
  let from_ts = TransferSyntaxRegistry
    .get(from)
    .ok_or_else(|| err("unknown source transfer syntax".into()))?;
  let to_ts = TransferSyntaxRegistry
    .get(to)
    .ok_or_else(|| err("unknown target transfer syntax".into()))?;
  let obj = InMemDicomObject::read_dataset_with_ts(bytes.as_slice(), from_ts).map_err(|e| err(e.to_string()))?;
  let mut out = Vec::with_capacity(bytes.len());
  obj
    .write_dataset_with_ts(&mut out, to_ts)
    .map_err(|e| err(e.to_string()))?;
  Ok(out)
}

fn accepted(pcs: &[PresentationContextNegotiated]) -> impl Iterator<Item = &PresentationContextNegotiated> {
  pcs
    .iter()
    .filter(|pc| pc.reason == PresentationContextResultReason::Acceptance)
}

/// Outbound proposal: the QR SOP class with the requestor's transfer syntax
/// first, then the two native ones every DICOM node understands. For C-GET,
/// every storage (SOP class, transfer syntax) pair the requestor accepted is
/// offered back with the SCP role, so the destination can only send datasets
/// the requestor can take unchanged.
fn outbound_proposal(
  req: &QrProxyRequest<'_>,
  inbound_ts: &str,
  inbound_pcs: &[PresentationContextNegotiated],
) -> (PresentationKeys, Vec<RoleSelection>) {
  let mut qr_ts = vec![inbound_ts.to_string()];
  for ts in [uids::EXPLICIT_VR_LITTLE_ENDIAN, uids::IMPLICIT_VR_LITTLE_ENDIAN] {
    if !qr_ts.iter().any(|t| t == ts) {
      qr_ts.push(ts.to_string());
    }
  }
  let mut pcs = vec![(req.sop_class_uid.to_string(), qr_ts)];
  let mut roles = Vec::new();
  if req.kind == QrKind::Get {
    for pc in accepted(inbound_pcs) {
      if QR_SOP_CLASSES.contains(&pc.abstract_syntax.as_str()) || pc.abstract_syntax == uids::VERIFICATION {
        continue;
      }
      pcs.push((pc.abstract_syntax.clone(), vec![pc.transfer_syntax.clone()]));
      if !roles.iter().any(|r: &RoleSelection| r.sop_class == pc.abstract_syntax) {
        roles.push(RoleSelection {
          sop_class: pc.abstract_syntax.clone(),
          scu:       false,
          scp:       true,
        });
      }
    }
  }
  (pcs, roles)
}

// --- Proxy
// -----------------------------------------------------------------------------------

/// Where to send the requestor's C-STORE-RSP for a relayed sub-operation.
struct PendingStore {
  outbound_msgid: u16,
  outbound_pc_id: u8,
}

/// Forward one C-FIND or C-GET to the destination and relay everything back
/// until the final response. Returns `Ok` once a final response has been sent
/// to the requestor; on `Err` the caller must still send one.
pub async fn proxy<S>(
  inbound: &mut AsyncServerAssociation<S>,
  req: &QrProxyRequest<'_>,
  qr: &QrClient,
  log: &Logger,
) -> Result<(), QrError>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
  let inbound_pcs = inbound.presentation_contexts().to_vec();
  let inbound_ts = inbound_pcs
    .iter()
    .find(|pc| pc.id == req.inbound_pc_id)
    .map(|pc| pc.transfer_syntax.clone())
    .ok_or_else(|| QrError::NoPresentationContext {
      sop_class_uid: req.sop_class_uid.to_string(),
    })?;
  let (pcs, roles) = outbound_proposal(req, &inbound_ts, &inbound_pcs);

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

  let result = relay(inbound, &mut outbound, req, &inbound_ts, &inbound_pcs, log).await;
  match &result {
    Ok(()) => {
      let _ = scu::release(outbound).await;
      info!(log, "QR proxy finished"; "operation" => req.kind.name(), "destination" => &qr.destination.name);
    }
    Err(_) => {
      let _ = outbound.abort().await;
    }
  }
  result
}

async fn relay<S>(
  inbound: &mut AsyncServerAssociation<S>,
  outbound: &mut ClientAssoc,
  req: &QrProxyRequest<'_>,
  inbound_ts: &str,
  inbound_pcs: &[PresentationContextNegotiated],
  log: &Logger,
) -> Result<(), QrError>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
  let outbound_pcs = outbound.presentation_contexts().to_vec();
  let qr_pc = accepted(&outbound_pcs)
    .find(|pc| pc.abstract_syntax == req.sop_class_uid)
    .ok_or_else(|| QrError::NoPresentationContext {
      sop_class_uid: req.sop_class_uid.to_string(),
    })?;
  let (qr_pc_id, outbound_ts) = (qr_pc.id, qr_pc.transfer_syntax.clone());

  let mut rq = match req.kind {
    QrKind::Find => dimse::create_cfind_rq(OUTBOUND_MSGID, req.sop_class_uid, dimse::PRIORITY_MEDIUM),
    QrKind::Get => dimse::create_cget_rq(OUTBOUND_MSGID, req.sop_class_uid, dimse::PRIORITY_MEDIUM),
  };
  rq.set_u16(TAG_MESSAGE_ID, OUTBOUND_MSGID);
  let identifier = recode(req.identifier.to_vec(), inbound_ts, &outbound_ts)?;
  send_message(outbound, qr_pc_id, &rq, Some(&identifier)).await?;

  let mut from_outbound = MessageReader::default();
  let mut from_inbound = MessageReader::default();
  let mut stores: HashMap<u16, PendingStore> = HashMap::new();
  let mut next_store_msgid: u16 = 1;

  loop {
    tokio::select! {
      message = from_outbound.read(outbound) => {
        let Message { pc_id, mut command, dataset } = message?;
        let field = dimse::command_field(&command);
        if field == Some(req.kind.rsp_command()) {
          command.set_u16(TAG_MESSAGE_ID_BEING_RESPONDED_TO, req.inbound_msgid);
          let status = dimse::uint16(&command, TAG_STATUS).unwrap_or(dimse::STATUS_CANNOT_UNDERSTAND);
          let dataset = dataset.map(|d| recode(d, &outbound_ts, inbound_ts)).transpose()?;
          send_message(inbound, req.inbound_pc_id, &command, dataset.as_deref()).await?;
          if !is_pending(status) {
            return Ok(());
          }
        } else if field == Some(dimse::C_STORE_RQ) && req.kind == QrKind::Get {
          let outbound_msgid = dimse::uint16(&command, TAG_MESSAGE_ID).unwrap_or(0);
          let Some(inbound_pc_id) = storage_pc_for(&outbound_pcs, inbound_pcs, pc_id) else {
            warn!(log, "C-GET sub-operation has no matching inbound presentation context");
            let rsp = store_failure(&command, outbound_msgid);
            send_message(outbound, pc_id, &rsp, None).await?;
            continue;
          };
          let inbound_msgid = next_store_msgid;
          next_store_msgid = next_store_msgid.checked_add(1).unwrap_or(1);
          command.set_u16(TAG_MESSAGE_ID, inbound_msgid);
          stores.insert(inbound_msgid, PendingStore { outbound_msgid, outbound_pc_id: pc_id });
          send_message(inbound, inbound_pc_id, &command, dataset.as_deref()).await?;
        } else {
          warn!(log, "ignoring unexpected message from QR destination"; "command_field" => field.unwrap_or(0));
        }
      }
      message = from_inbound.read(inbound) => {
        let Message { mut command, .. } = message?;
        let field = dimse::command_field(&command);
        if field == Some(dimse::C_STORE_RSP) {
          let inbound_msgid = dimse::uint16(&command, TAG_MESSAGE_ID_BEING_RESPONDED_TO).unwrap_or(0);
          let Some(store) = stores.remove(&inbound_msgid) else {
            warn!(log, "C-STORE-RSP for unknown sub-operation"; "message_id" => inbound_msgid);
            continue;
          };
          command.set_u16(TAG_MESSAGE_ID_BEING_RESPONDED_TO, store.outbound_msgid);
          send_message(outbound, store.outbound_pc_id, &command, None).await?;
        } else if field == Some(dimse::C_CANCEL_RQ)
          && dimse::uint16(&command, TAG_MESSAGE_ID_BEING_RESPONDED_TO) == Some(req.inbound_msgid)
        {
          command.set_u16(TAG_MESSAGE_ID_BEING_RESPONDED_TO, OUTBOUND_MSGID);
          send_message(outbound, qr_pc_id, &command, None).await?;
        } else {
          warn!(log, "ignoring message received during QR proxy"; "command_field" => field.unwrap_or(0));
        }
      }
    }
  }
}

fn is_pending(status: u16) -> bool { status == dimse::STATUS_PENDING || status == dimse::STATUS_PENDING_WARNING }

/// Inbound presentation context carrying the same SOP class and transfer
/// syntax as the outbound one the destination used for a sub-operation.
fn storage_pc_for(
  outbound_pcs: &[PresentationContextNegotiated],
  inbound_pcs: &[PresentationContextNegotiated],
  outbound_pc_id: u8,
) -> Option<u8> {
  let out = accepted(outbound_pcs).find(|pc| pc.id == outbound_pc_id)?;
  accepted(inbound_pcs)
    .find(|pc| pc.abstract_syntax == out.abstract_syntax && pc.transfer_syntax == out.transfer_syntax)
    .map(|pc| pc.id)
}

fn store_failure(store_rq: &CommandSet, outbound_msgid: u16) -> CommandSet {
  dimse::create_cstore_rsp(
    outbound_msgid,
    dimse::string(store_rq, dimse::TAG_AFFECTED_SOP_CLASS_UID).unwrap_or(""),
    dimse::string(store_rq, dimse::TAG_AFFECTED_SOP_INSTANCE_UID).unwrap_or(""),
    dimse::STATUS_SOP_CLASS_NOT_SUPPORTED,
  )
}

/// Final failure response for a C-FIND or C-GET that could not be proxied.
pub async fn refuse<S>(
  inbound: &mut AsyncServerAssociation<S>,
  kind: QrKind,
  inbound_pc_id: u8,
  inbound_msgid: u16,
  sop_class_uid: &str,
  status: u16,
) -> Result<(), QrError>
where
  S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
  let rsp = match kind {
    QrKind::Find => dimse::create_cfind_rsp(inbound_msgid, sop_class_uid, status, false),
    QrKind::Get => dimse::create_cget_rsp(inbound_msgid, sop_class_uid, status, 0, 0, 0, 0),
  };
  send_message(inbound, inbound_pc_id, &rsp, None).await
}

#[cfg(test)]
mod tests {
  use super::*;

  fn pdv(value_type: PDataValueType, is_last: bool, data: &[u8]) -> PDataValue {
    PDataValue {
      presentation_context_id: 3,
      value_type,
      is_last,
      data: data.to_vec(),
    }
  }

  #[test]
  fn reader_reassembles_fragmented_command_and_dataset() {
    let cmd = dimse::encode_command(&dimse::create_cstore_rq(
      7,
      uids::CT_IMAGE_STORAGE,
      "1.2.3",
      dimse::PRIORITY_MEDIUM,
    ));
    let (a, b) = cmd.split_at(10);
    let mut reader = MessageReader::default();
    assert!(reader.push(pdv(PDataValueType::Command, false, a)).unwrap().is_none());
    assert!(reader.push(pdv(PDataValueType::Command, true, b)).unwrap().is_none());
    assert!(reader.push(pdv(PDataValueType::Data, false, b"abc")).unwrap().is_none());
    let message = reader.push(pdv(PDataValueType::Data, true, b"def")).unwrap().unwrap();
    assert_eq!(message.pc_id, 3);
    assert_eq!(dimse::uint16(&message.command, TAG_MESSAGE_ID), Some(7));
    assert_eq!(message.dataset.as_deref(), Some(&b"abcdef"[..]));
  }

  #[test]
  fn reader_returns_command_only_message_without_dataset() {
    let cmd = dimse::encode_command(&dimse::create_cfind_rsp(
      1,
      dimse::STUDY_ROOT_FIND_SOP_CLASS_UID,
      dimse::STATUS_SUCCESS,
      false,
    ));
    let mut reader = MessageReader::default();
    let message = reader.push(pdv(PDataValueType::Command, true, &cmd)).unwrap().unwrap();
    assert!(message.dataset.is_none());
  }

  #[test]
  fn reader_rejects_dataset_without_command() {
    let mut reader = MessageReader::default();
    assert!(reader.push(pdv(PDataValueType::Data, true, b"x")).is_err());
  }

  #[test]
  fn recode_implicit_to_explicit_preserves_elements() {
    use dicom_core::{dicom_value, DataElement, VR};
    use dicom_dictionary_std::tags;
    let mut obj = InMemDicomObject::new_empty();
    obj.put(DataElement::new(tags::PATIENT_ID, VR::LO, dicom_value!(Str, "P1")));
    let mut implicit = Vec::new();
    obj
      .write_dataset_with_ts(
        &mut implicit,
        TransferSyntaxRegistry.get(uids::IMPLICIT_VR_LITTLE_ENDIAN).unwrap(),
      )
      .unwrap();
    let explicit = recode(
      implicit.clone(),
      uids::IMPLICIT_VR_LITTLE_ENDIAN,
      uids::EXPLICIT_VR_LITTLE_ENDIAN,
    )
    .unwrap();
    assert_ne!(explicit, implicit);
    let back = InMemDicomObject::read_dataset_with_ts(
      explicit.as_slice(),
      TransferSyntaxRegistry.get(uids::EXPLICIT_VR_LITTLE_ENDIAN).unwrap(),
    )
    .unwrap();
    assert_eq!(back.element(tags::PATIENT_ID).unwrap().to_str().unwrap(), "P1");
  }

  #[test]
  fn get_proposal_mirrors_inbound_storage_contexts() {
    let inbound = vec![
      PresentationContextNegotiated {
        id:              1,
        reason:          PresentationContextResultReason::Acceptance,
        abstract_syntax: dimse::STUDY_ROOT_GET_SOP_CLASS_UID.into(),
        transfer_syntax: uids::IMPLICIT_VR_LITTLE_ENDIAN.into(),
      },
      PresentationContextNegotiated {
        id:              3,
        reason:          PresentationContextResultReason::Acceptance,
        abstract_syntax: uids::CT_IMAGE_STORAGE.into(),
        transfer_syntax: uids::JPEG_LOSSLESS_SV1.into(),
      },
      PresentationContextNegotiated {
        id:              5,
        reason:          PresentationContextResultReason::AbstractSyntaxNotSupported,
        abstract_syntax: uids::MR_IMAGE_STORAGE.into(),
        transfer_syntax: uids::IMPLICIT_VR_LITTLE_ENDIAN.into(),
      },
    ];
    let req = QrProxyRequest {
      kind:             QrKind::Get,
      inbound_pc_id:    1,
      inbound_msgid:    1,
      sop_class_uid:    dimse::STUDY_ROOT_GET_SOP_CLASS_UID,
      identifier:       &[],
      calling_ae_title: "R",
      max_pdu_length:   16384,
    };
    let (pcs, roles) = outbound_proposal(&req, uids::IMPLICIT_VR_LITTLE_ENDIAN, &inbound);
    assert_eq!(pcs, vec![
      (dimse::STUDY_ROOT_GET_SOP_CLASS_UID.to_string(), vec![
        uids::IMPLICIT_VR_LITTLE_ENDIAN.to_string(),
        uids::EXPLICIT_VR_LITTLE_ENDIAN.to_string(),
      ]),
      (uids::CT_IMAGE_STORAGE.to_string(), vec![
        uids::JPEG_LOSSLESS_SV1.to_string()
      ]),
    ]);
    assert_eq!(roles.len(), 1);
    assert_eq!(roles[0].sop_class, uids::CT_IMAGE_STORAGE);
  }
}
