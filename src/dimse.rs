//! DIMSE command set builders and parsers. Command sets are always Implicit VR Little Endian.
//!
//! A DIMSE command set is a small group of data elements, all in group `0x0000`, carried as
//! the command PDV of a P-DATA-TF PDU. Because it is always encoded Implicit VR Little
//! Endian, the value representation of each element is *not* present on the wire — it is
//! determined solely by the element's tag, per the DICOM data dictionary (PS3.7). This module
//! hand-rolls that encoding rather than pulling in a general-purpose dataset codec, since the
//! command set vocabulary needed by a C-STORE/C-ECHO router is tiny and fixed.

use snafu::Snafu;
use std::collections::BTreeMap;

// --- Well-known command field values (PS3.7 Section 9.3) -------------------------------------

pub const C_STORE_RQ: u16 = 0x0001;
pub const C_STORE_RSP: u16 = 0x8001;
pub const C_ECHO_RQ: u16 = 0x0030;
pub const C_ECHO_RSP: u16 = 0x8030;

// --- Well-known status codes (PS3.7 Annex C) --------------------------------------------------

pub const STATUS_SUCCESS: u16 = 0x0000;
pub const STATUS_OUT_OF_RESOURCES: u16 = 0xA700;
pub const STATUS_CANNOT_UNDERSTAND: u16 = 0xC000;

/// `CommandDataSetType` value meaning "no data set follows this command".
pub const NO_DATA_SET: u16 = 0x0101;
/// `CommandDataSetType` value conventionally used to mean "a data set follows this command".
/// Any value other than [`NO_DATA_SET`] means a data set follows; this is the value this
/// module emits.
pub const DATA_SET_PRESENT: u16 = 0x0001;

// --- Command set element tags (all group 0x0000, PS3.7 Annex E) ------------------------------

pub type Tag = (u16, u16);

pub const TAG_COMMAND_GROUP_LENGTH: Tag = (0x0000, 0x0000);
pub const TAG_AFFECTED_SOP_CLASS_UID: Tag = (0x0000, 0x0002);
pub const TAG_COMMAND_FIELD: Tag = (0x0000, 0x0100);
pub const TAG_MESSAGE_ID: Tag = (0x0000, 0x0110);
pub const TAG_MESSAGE_ID_BEING_RESPONDED_TO: Tag = (0x0000, 0x0120);
pub const TAG_PRIORITY: Tag = (0x0000, 0x0700);
pub const TAG_COMMAND_DATA_SET_TYPE: Tag = (0x0000, 0x0800);
pub const TAG_STATUS: Tag = (0x0000, 0x0900);
pub const TAG_AFFECTED_SOP_INSTANCE_UID: Tag = (0x0000, 0x1000);

/// SOP Class UID of the DICOM Verification Service Class (used by C-ECHO).
pub const VERIFICATION_SOP_CLASS_UID: &str = "1.2.840.10008.1.1";

/// Default priority (PS3.7 9.3.1.1): `MEDIUM`.
pub const PRIORITY_MEDIUM: u16 = 0x0000;

#[derive(Debug, Snafu)]
pub enum DimseError {
    #[snafu(display("truncated command set: expected {expected} more byte(s) at offset {offset}, found {available}"))]
    Truncated {
        offset: usize,
        expected: usize,
        available: usize,
    },
    #[snafu(display(
        "tag ({:#06x},{:#06x}) at offset {offset}: expected a {expected}-byte value, found {actual} byte(s)",
        tag.0, tag.1
    ))]
    BadLength {
        tag: Tag,
        offset: usize,
        expected: usize,
        actual: usize,
    },
    #[snafu(display("tag ({:#06x},{:#06x}) at offset {offset}: value is not valid UTF-8", tag.0, tag.1))]
    InvalidText { tag: Tag, offset: usize },
}

/// A decoded command set element value. Implicit VR Little Endian carries no VR on the wire,
/// so the variant used for a given tag is fixed by the DICOM data dictionary (see
/// [`element_kind`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandValue {
    /// VR US: a 16-bit unsigned integer.
    UShort(u16),
    /// VR UL: a 32-bit unsigned integer.
    ULong(u32),
    /// VR UI (or similarly text-like VRs): a string, padded to even length on the wire with a
    /// trailing NUL, which is stripped back off on decode.
    Text(String),
}

/// The wire representation used for a given command set tag, absent a VR on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ElementKind {
    UShort,
    ULong,
    Text,
}

/// Look up the fixed wire representation for a well-known command set tag. Unrecognized tags
/// fall back to inferring the kind from the encoded value length (2 bytes -> US, 4 bytes ->
/// UL, anything else -> text), which is good enough for a router that only needs to look past
/// elements it doesn't understand.
fn element_kind(tag: Tag, value_len: usize) -> ElementKind {
    match tag {
        TAG_COMMAND_GROUP_LENGTH => ElementKind::ULong,
        TAG_AFFECTED_SOP_CLASS_UID => ElementKind::Text,
        TAG_COMMAND_FIELD => ElementKind::UShort,
        TAG_MESSAGE_ID => ElementKind::UShort,
        TAG_MESSAGE_ID_BEING_RESPONDED_TO => ElementKind::UShort,
        TAG_PRIORITY => ElementKind::UShort,
        TAG_COMMAND_DATA_SET_TYPE => ElementKind::UShort,
        TAG_STATUS => ElementKind::UShort,
        TAG_AFFECTED_SOP_INSTANCE_UID => ElementKind::Text,
        _ => match value_len {
            2 => ElementKind::UShort,
            4 => ElementKind::ULong,
            _ => ElementKind::Text,
        },
    }
}

/// An ordered set of DIMSE command elements (group `0x0000`). Iterating a `BTreeMap` keyed by
/// `(group, element)` naturally yields ascending tag order, which is what [`encode_command`]
/// requires.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandSet {
    elements: BTreeMap<Tag, CommandValue>,
}

impl CommandSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_u16(&mut self, tag: Tag, value: u16) -> &mut Self {
        self.elements.insert(tag, CommandValue::UShort(value));
        self
    }

    pub fn set_u32(&mut self, tag: Tag, value: u32) -> &mut Self {
        self.elements.insert(tag, CommandValue::ULong(value));
        self
    }

    pub fn set_str(&mut self, tag: Tag, value: impl Into<String>) -> &mut Self {
        self.elements.insert(tag, CommandValue::Text(value.into()));
        self
    }

    pub fn get(&self, tag: Tag) -> Option<&CommandValue> {
        self.elements.get(&tag)
    }
}

// --- Generic read helpers ---------------------------------------------------------------------

/// Read a US-typed element out of a decoded command set.
pub fn uint16(cmd: &CommandSet, tag: Tag) -> Option<u16> {
    match cmd.get(tag) {
        Some(CommandValue::UShort(v)) => Some(*v),
        _ => None,
    }
}

/// Read a UL-typed element out of a decoded command set.
pub fn uint32(cmd: &CommandSet, tag: Tag) -> Option<u32> {
    match cmd.get(tag) {
        Some(CommandValue::ULong(v)) => Some(*v),
        _ => None,
    }
}

/// Read a text-typed (e.g. UI) element out of a decoded command set.
pub fn string(cmd: &CommandSet, tag: Tag) -> Option<&str> {
    match cmd.get(tag) {
        Some(CommandValue::Text(v)) => Some(v.as_str()),
        _ => None,
    }
}

/// Read the `CommandField` (0000,0100) — the DIMSE message type — out of a decoded command
/// set. Compare against [`C_STORE_RQ`], [`C_STORE_RSP`], [`C_ECHO_RQ`], [`C_ECHO_RSP`], etc.
/// to dispatch on the message type after [`decode_command`].
pub fn command_field(cmd: &CommandSet) -> Option<u16> {
    uint16(cmd, TAG_COMMAND_FIELD)
}

// --- Builders ----------------------------------------------------------------------------------

/// Build a C-STORE-RQ command set.
pub fn create_cstore_rq(
    message_id: u16,
    affected_sop_class_uid: &str,
    affected_sop_instance_uid: &str,
    priority: u16,
) -> CommandSet {
    let mut cmd = CommandSet::new();
    cmd.set_str(TAG_AFFECTED_SOP_CLASS_UID, affected_sop_class_uid);
    cmd.set_u16(TAG_COMMAND_FIELD, C_STORE_RQ);
    cmd.set_u16(TAG_MESSAGE_ID, message_id);
    cmd.set_u16(TAG_PRIORITY, priority);
    cmd.set_u16(TAG_COMMAND_DATA_SET_TYPE, DATA_SET_PRESENT);
    cmd.set_str(TAG_AFFECTED_SOP_INSTANCE_UID, affected_sop_instance_uid);
    cmd
}

/// Build a C-STORE-RSP command set.
pub fn create_cstore_rsp(
    message_id_being_responded_to: u16,
    affected_sop_class_uid: &str,
    affected_sop_instance_uid: &str,
    status: u16,
) -> CommandSet {
    let mut cmd = CommandSet::new();
    cmd.set_str(TAG_AFFECTED_SOP_CLASS_UID, affected_sop_class_uid);
    cmd.set_u16(TAG_COMMAND_FIELD, C_STORE_RSP);
    cmd.set_u16(
        TAG_MESSAGE_ID_BEING_RESPONDED_TO,
        message_id_being_responded_to,
    );
    cmd.set_u16(TAG_COMMAND_DATA_SET_TYPE, NO_DATA_SET);
    cmd.set_u16(TAG_STATUS, status);
    cmd.set_str(TAG_AFFECTED_SOP_INSTANCE_UID, affected_sop_instance_uid);
    cmd
}

/// Build a C-ECHO-RQ command set.
pub fn create_cecho_request(message_id: u16) -> CommandSet {
    let mut cmd = CommandSet::new();
    cmd.set_str(TAG_AFFECTED_SOP_CLASS_UID, VERIFICATION_SOP_CLASS_UID);
    cmd.set_u16(TAG_COMMAND_FIELD, C_ECHO_RQ);
    cmd.set_u16(TAG_MESSAGE_ID, message_id);
    cmd.set_u16(TAG_COMMAND_DATA_SET_TYPE, NO_DATA_SET);
    cmd
}

/// Build a C-ECHO-RSP command set.
pub fn create_cecho_rsp(message_id_being_responded_to: u16, status: u16) -> CommandSet {
    let mut cmd = CommandSet::new();
    cmd.set_str(TAG_AFFECTED_SOP_CLASS_UID, VERIFICATION_SOP_CLASS_UID);
    cmd.set_u16(TAG_COMMAND_FIELD, C_ECHO_RSP);
    cmd.set_u16(
        TAG_MESSAGE_ID_BEING_RESPONDED_TO,
        message_id_being_responded_to,
    );
    cmd.set_u16(TAG_COMMAND_DATA_SET_TYPE, NO_DATA_SET);
    cmd.set_u16(TAG_STATUS, status);
    cmd
}

// --- Encode / decode -----------------------------------------------------------------------

/// Encode one data element (Implicit VR Little Endian: tag, 4-byte length, value) into `out`.
fn encode_element(out: &mut Vec<u8>, tag: Tag, value: &CommandValue) {
    out.extend_from_slice(&tag.0.to_le_bytes());
    out.extend_from_slice(&tag.1.to_le_bytes());
    match value {
        CommandValue::UShort(v) => {
            out.extend_from_slice(&2u32.to_le_bytes());
            out.extend_from_slice(&v.to_le_bytes());
        }
        CommandValue::ULong(v) => {
            out.extend_from_slice(&4u32.to_le_bytes());
            out.extend_from_slice(&v.to_le_bytes());
        }
        CommandValue::Text(s) => {
            let mut bytes = s.as_bytes().to_vec();
            if bytes.len() % 2 != 0 {
                bytes.push(0); // DICOM pads odd-length values to even length with NUL.
            }
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&bytes);
        }
    }
}

/// Encode a command set as Implicit VR Little Endian bytes, prefixed with the
/// `CommandGroupLength` (0000,0000) element the DICOM standard requires as the first element
/// of every command set. Any `CommandGroupLength` already present in `cmd` is ignored and
/// recomputed.
pub fn encode_command(cmd: &CommandSet) -> Vec<u8> {
    let mut body = Vec::new();
    for (tag, value) in cmd
        .elements
        .iter()
        .filter(|(tag, _)| **tag != TAG_COMMAND_GROUP_LENGTH)
    {
        encode_element(&mut body, *tag, value);
    }

    let mut out = Vec::with_capacity(body.len() + 12);
    encode_element(
        &mut out,
        TAG_COMMAND_GROUP_LENGTH,
        &CommandValue::ULong(body.len() as u32),
    );
    out.extend_from_slice(&body);
    out
}

/// Decode a command set from Implicit VR Little Endian bytes, as produced by
/// [`encode_command`]. Elements are decoded until the input is exhausted; the
/// `CommandGroupLength` value itself is not validated against the remaining length (it exists
/// on the wire for compatibility with implementations that read it to size a buffer, but this
/// decoder simply consumes elements until `bytes` runs out).
pub fn decode_command(bytes: &[u8]) -> Result<CommandSet, DimseError> {
    let mut cmd = CommandSet::new();
    let mut pos = 0usize;

    while pos < bytes.len() {
        if bytes.len() - pos < 8 {
            return Err(DimseError::Truncated {
                offset: pos,
                expected: 8,
                available: bytes.len() - pos,
            });
        }
        let group = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]);
        let element = u16::from_le_bytes([bytes[pos + 2], bytes[pos + 3]]);
        let length = u32::from_le_bytes([
            bytes[pos + 4],
            bytes[pos + 5],
            bytes[pos + 6],
            bytes[pos + 7],
        ]) as usize;
        pos += 8;
        let tag = (group, element);

        if bytes.len() - pos < length {
            return Err(DimseError::Truncated {
                offset: pos,
                expected: length,
                available: bytes.len() - pos,
            });
        }
        let value_bytes = &bytes[pos..pos + length];
        pos += length;

        let value = match element_kind(tag, length) {
            ElementKind::UShort => {
                if length != 2 {
                    return Err(DimseError::BadLength {
                        tag,
                        offset: pos - length,
                        expected: 2,
                        actual: length,
                    });
                }
                CommandValue::UShort(u16::from_le_bytes([value_bytes[0], value_bytes[1]]))
            }
            ElementKind::ULong => {
                if length != 4 {
                    return Err(DimseError::BadLength {
                        tag,
                        offset: pos - length,
                        expected: 4,
                        actual: length,
                    });
                }
                CommandValue::ULong(u32::from_le_bytes(value_bytes.try_into().unwrap()))
            }
            ElementKind::Text => {
                let text = std::str::from_utf8(value_bytes)
                    .map_err(|_| DimseError::InvalidText {
                        tag,
                        offset: pos - length,
                    })?
                    .trim_end_matches(['\0', ' '])
                    .to_string();
                CommandValue::Text(text)
            }
        };

        // The group length element only exists to size a read buffer in some
        // implementations; this decoder doesn't need it, but keep it for round-trip fidelity.
        cmd.elements.insert(tag, value);
    }

    Ok(cmd)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cstore_rq_roundtrip() {
        let cmd = create_cstore_rq(
            42,
            "1.2.840.10008.5.1.4.1.1.7",
            "1.2.3.4.5",
            PRIORITY_MEDIUM,
        );
        let bytes = encode_command(&cmd);
        let decoded = decode_command(&bytes).expect("decode C-STORE-RQ");

        assert_eq!(command_field(&decoded), Some(C_STORE_RQ));
        assert_eq!(uint16(&decoded, TAG_MESSAGE_ID), Some(42));
        assert_eq!(uint16(&decoded, TAG_PRIORITY), Some(PRIORITY_MEDIUM));
        assert_eq!(
            uint16(&decoded, TAG_COMMAND_DATA_SET_TYPE),
            Some(DATA_SET_PRESENT)
        );
        assert_eq!(
            string(&decoded, TAG_AFFECTED_SOP_CLASS_UID),
            Some("1.2.840.10008.5.1.4.1.1.7")
        );
        assert_eq!(
            string(&decoded, TAG_AFFECTED_SOP_INSTANCE_UID),
            Some("1.2.3.4.5")
        );
        assert_eq!(
            uint32(&decoded, TAG_COMMAND_GROUP_LENGTH),
            Some((bytes.len() - 12) as u32)
        );
    }

    #[test]
    fn cstore_rsp_roundtrip() {
        let cmd = create_cstore_rsp(7, "1.2.840.10008.5.1.4.1.1.7", "1.2.3.4.5", STATUS_SUCCESS);
        let bytes = encode_command(&cmd);
        let decoded = decode_command(&bytes).expect("decode C-STORE-RSP");

        assert_eq!(command_field(&decoded), Some(C_STORE_RSP));
        assert_eq!(uint16(&decoded, TAG_MESSAGE_ID_BEING_RESPONDED_TO), Some(7));
        assert_eq!(uint16(&decoded, TAG_STATUS), Some(STATUS_SUCCESS));
        assert_eq!(
            uint16(&decoded, TAG_COMMAND_DATA_SET_TYPE),
            Some(NO_DATA_SET)
        );
        assert_eq!(
            string(&decoded, TAG_AFFECTED_SOP_INSTANCE_UID),
            Some("1.2.3.4.5")
        );
    }

    #[test]
    fn cecho_rsp_roundtrip() {
        let cmd = create_cecho_rsp(99, STATUS_SUCCESS);
        let bytes = encode_command(&cmd);
        let decoded = decode_command(&bytes).expect("decode C-ECHO-RSP");

        assert_eq!(command_field(&decoded), Some(C_ECHO_RSP));
        assert_eq!(
            uint16(&decoded, TAG_MESSAGE_ID_BEING_RESPONDED_TO),
            Some(99)
        );
        assert_eq!(uint16(&decoded, TAG_STATUS), Some(STATUS_SUCCESS));
        assert_eq!(
            string(&decoded, TAG_AFFECTED_SOP_CLASS_UID),
            Some(VERIFICATION_SOP_CLASS_UID)
        );
    }

    #[test]
    fn cecho_request_roundtrip() {
        let cmd = create_cecho_request(5);
        let bytes = encode_command(&cmd);
        let decoded = decode_command(&bytes).expect("decode C-ECHO-RQ");

        assert_eq!(command_field(&decoded), Some(C_ECHO_RQ));
        assert_eq!(uint16(&decoded, TAG_MESSAGE_ID), Some(5));
        assert_eq!(
            uint16(&decoded, TAG_COMMAND_DATA_SET_TYPE),
            Some(NO_DATA_SET)
        );
    }

    #[test]
    fn decode_rejects_truncated_input() {
        let cmd = create_cecho_request(1);
        let mut bytes = encode_command(&cmd);
        bytes.truncate(bytes.len() - 1);
        assert!(decode_command(&bytes).is_err());
    }
}
