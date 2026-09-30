//! Optional per-destination transcoding of outbound objects.
//!
//! The sender proposes both the target transfer syntax and the object's
//! original one; the object is only transcoded when the destination accepts the
//! target, and falls back to the original bytes on any transcoding failure.

use dicom_core::Tag;
use dicom_dictionary_std::{tags, uids};
use dicom_encoding::adapters::EncodeOptions;
use dicom_encoding::TransferSyntaxIndex;
use dicom_object::{FileDicomObject, InMemDicomObject};
use dicom_pixeldata::{PixelDecoder, Transcode};
use dicom_transfer_syntax_registry::TransferSyntaxRegistry;
use serde::Deserialize;
use snafu::{ensure, OptionExt, ResultExt, Snafu};

pub type DicomFile = FileDicomObject<InMemDicomObject>;

/// Outbound compression mode for a destination.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Compression {
  /// Forward objects in the transfer syntax they were received in.
  #[default]
  None,
  /// Re-encode uncompressed images as JPEG XL Lossless. Anything else
  /// (already compressed, non-image, unsupported pixel layout) is forwarded
  /// unchanged.
  JpegXlLossless,
  /// Decode compressed pixel data to Explicit VR Little Endian. Intended for
  /// the receiving router in front of a PACS.
  ExplicitLe,
}

#[derive(Debug, Snafu)]
pub enum CompressionError {
  #[snafu(display("unknown transfer syntax {uid}"))]
  UnknownTransferSyntax { uid: String },
  #[snafu(display("transcode failed: {source}"))]
  Transcode { source: dicom_pixeldata::TranscodeError },
  #[snafu(display("verification decode failed: {source}"))]
  VerifyDecode { source: dicom_pixeldata::Error },
  #[snafu(display("original pixel data unreadable"))]
  OriginalPixelData,
  #[snafu(display("lossless verification failed: decoded pixel data differs from original"))]
  VerifyMismatch,
}

/// Uncompressed little-endian transfer syntaxes whose pixel data can be fed
/// straight into an encoder.
const NATIVE_LE_SOURCES: &[&str] = &[
  uids::IMPLICIT_VR_LITTLE_ENDIAN,
  uids::EXPLICIT_VR_LITTLE_ENDIAN,
  uids::DEFLATED_EXPLICIT_VR_LITTLE_ENDIAN,
];

/// Photometric interpretations the JPEG XL encoder stores without altering
/// their meaning (it relabels anything else as MONOCHROME2 or RGB).
const JPEG_XL_PHOTOMETRICS: &[&str] = &["MONOCHROME1", "MONOCHROME2", "PALETTE COLOR", "RGB"];

impl Compression {
  /// Transfer syntax to propose for `file` in addition to its current one, or
  /// `None` to forward it unchanged.
  pub fn target_for(self, file: &DicomFile) -> Option<&'static str> {
    let current = file.meta().transfer_syntax();
    match self {
      Compression::None => None,
      Compression::JpegXlLossless => jpeg_xl_eligible(file, current).then_some(uids::JPEGXL_LOSSLESS),
      Compression::ExplicitLe => {
        let ts = TransferSyntaxRegistry.get(current)?;
        (current != uids::EXPLICIT_VR_LITTLE_ENDIAN && !ts.is_unsupported()).then_some(uids::EXPLICIT_VR_LITTLE_ENDIAN)
      }
    }
  }
}

fn jpeg_xl_eligible(file: &DicomFile, current_ts: &str) -> bool {
  if !NATIVE_LE_SOURCES.contains(&current_ts) || file.get(tags::PIXEL_DATA).is_none() {
    return false;
  }
  let int = |tag: Tag| file.get(tag).and_then(|e| e.to_int::<u16>().ok());
  let photometric = file
    .get(tags::PHOTOMETRIC_INTERPRETATION)
    .and_then(|e| e.to_str().ok())
    .map(|s| s.trim_end_matches(['\0', ' ']).to_string());
  let bits_ok = matches!(int(tags::BITS_ALLOCATED), Some(8 | 16));
  let layout_ok = match int(tags::SAMPLES_PER_PIXEL) {
    Some(1) => true,
    // Encoder assumes interleaved samples.
    Some(3) => int(tags::PLANAR_CONFIGURATION).unwrap_or(0) == 0,
    _ => false,
  };
  let photometric_ok = photometric.is_some_and(|p| JPEG_XL_PHOTOMETRICS.contains(&p.as_str()));
  bits_ok && layout_ok && photometric_ok
}

/// Transcode a copy of `file` to `ts_uid`. When the source pixel data is
/// uncompressed, the result is decoded again and compared byte-for-byte with
/// the original so a codec defect can never silently alter pixel values.
pub fn transcode(file: &DicomFile, ts_uid: &str) -> Result<DicomFile, CompressionError> {
  let ts = TransferSyntaxRegistry
    .get(ts_uid)
    .context(UnknownTransferSyntaxSnafu { uid: ts_uid })?;
  let mut out = file.clone();
  let mut options = EncodeOptions::new();
  options.quality = Some(100);
  out.transcode_with_options(ts, options).context(TranscodeSnafu)?;

  if ts.is_encapsulated_pixel_data() && NATIVE_LE_SOURCES.contains(&file.meta().transfer_syntax()) {
    verify_lossless(file, &out)?;
  }
  Ok(out)
}

fn verify_lossless(original: &DicomFile, encoded: &DicomFile) -> Result<(), CompressionError> {
  let original = original
    .get(tags::PIXEL_DATA)
    .and_then(|e| e.to_bytes().ok())
    .context(OriginalPixelDataSnafu)?;
  let decoded = encoded.decode_pixel_data().context(VerifyDecodeSnafu)?;
  let decoded = decoded.data();
  // Native pixel data may carry one trailing padding byte.
  ensure!(
    original.len() >= decoded.len() && original.len() - decoded.len() <= 1 && original[..decoded.len()] == *decoded,
    VerifyMismatchSnafu
  );
  Ok(())
}

#[cfg(test)]
mod tests {
  use dicom_core::{dicom_value, DataElement, PrimitiveValue, VR};
  use dicom_object::FileMetaTableBuilder;

  use super::*;

  fn image(ts: &str, bits: u16, samples: u16, photometric: &str, frames: u32, pixels: Vec<u8>) -> DicomFile {
    let mut obj = InMemDicomObject::new_empty();
    obj.put(DataElement::new(
      tags::SOP_CLASS_UID,
      VR::UI,
      dicom_value!(Str, uids::CT_IMAGE_STORAGE),
    ));
    obj.put(DataElement::new(
      tags::SOP_INSTANCE_UID,
      VR::UI,
      dicom_value!(Str, "1.2.3.4"),
    ));
    obj.put(DataElement::new(tags::ROWS, VR::US, dicom_value!(U16, [64])));
    obj.put(DataElement::new(tags::COLUMNS, VR::US, dicom_value!(U16, [48])));
    obj.put(DataElement::new(
      tags::SAMPLES_PER_PIXEL,
      VR::US,
      dicom_value!(U16, [samples]),
    ));
    obj.put(DataElement::new(
      tags::BITS_ALLOCATED,
      VR::US,
      dicom_value!(U16, [bits]),
    ));
    obj.put(DataElement::new(tags::BITS_STORED, VR::US, dicom_value!(U16, [bits])));
    obj.put(DataElement::new(tags::HIGH_BIT, VR::US, dicom_value!(U16, [bits - 1])));
    obj.put(DataElement::new(
      tags::PIXEL_REPRESENTATION,
      VR::US,
      dicom_value!(U16, [1]),
    ));
    obj.put(DataElement::new(
      tags::PHOTOMETRIC_INTERPRETATION,
      VR::CS,
      dicom_value!(Str, photometric),
    ));
    if samples == 3 {
      obj.put(DataElement::new(
        tags::PLANAR_CONFIGURATION,
        VR::US,
        dicom_value!(U16, [0]),
      ));
    }
    if frames > 1 {
      obj.put(DataElement::new(
        tags::NUMBER_OF_FRAMES,
        VR::IS,
        dicom_value!(Str, frames.to_string()),
      ));
    }
    let vr = if bits == 8 { VR::OB } else { VR::OW };
    obj.put(DataElement::new(tags::PIXEL_DATA, vr, PrimitiveValue::from(pixels)));
    let meta = FileMetaTableBuilder::new()
      .media_storage_sop_class_uid(uids::CT_IMAGE_STORAGE)
      .media_storage_sop_instance_uid("1.2.3.4")
      .transfer_syntax(ts)
      .build()
      .unwrap();
    obj.with_exact_meta(meta)
  }

  /// Deterministic pseudo-random bytes covering the full value range.
  fn noise(len: usize) -> Vec<u8> {
    let mut x: u32 = 0x1234_5678;
    (0..len)
      .map(|_| {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        x as u8
      })
      .collect()
  }

  fn round_trip(file: &DicomFile) {
    let jxl = transcode(file, uids::JPEGXL_LOSSLESS).unwrap();
    assert_eq!(jxl.meta().transfer_syntax(), uids::JPEGXL_LOSSLESS);
    assert!(jxl.get(tags::LOSSY_IMAGE_COMPRESSION).is_none());
    let back = transcode(&jxl, uids::EXPLICIT_VR_LITTLE_ENDIAN).unwrap();
    assert_eq!(back.meta().transfer_syntax(), uids::EXPLICIT_VR_LITTLE_ENDIAN);
    assert_eq!(
      back.get(tags::PIXEL_DATA).unwrap().to_bytes().unwrap(),
      file.get(tags::PIXEL_DATA).unwrap().to_bytes().unwrap()
    );
  }

  #[test]
  fn jpeg_xl_round_trip_is_bit_exact_for_16_bit_signed_noise() {
    round_trip(&image(
      uids::EXPLICIT_VR_LITTLE_ENDIAN,
      16,
      1,
      "MONOCHROME2",
      1,
      noise(64 * 48 * 2),
    ));
  }

  #[test]
  fn jpeg_xl_round_trip_is_bit_exact_for_multi_frame_8_bit_rgb() {
    round_trip(&image(
      uids::IMPLICIT_VR_LITTLE_ENDIAN,
      8,
      3,
      "RGB",
      3,
      noise(64 * 48 * 3 * 3),
    ));
  }

  #[test]
  fn jpeg_xl_targets_only_eligible_images() {
    let px = noise(64 * 48 * 2);
    let mono = image(uids::EXPLICIT_VR_LITTLE_ENDIAN, 16, 1, "MONOCHROME2", 1, px.clone());
    assert_eq!(
      Compression::JpegXlLossless.target_for(&mono),
      Some(uids::JPEGXL_LOSSLESS)
    );
    assert_eq!(Compression::None.target_for(&mono), None);

    let ybr = image(uids::EXPLICIT_VR_LITTLE_ENDIAN, 8, 3, "YBR_FULL", 1, noise(64 * 48 * 3));
    assert_eq!(Compression::JpegXlLossless.target_for(&ybr), None);

    // Explicit VR Big Endian (retired).
    let big_endian = image("1.2.840.10008.1.2.2", 16, 1, "MONOCHROME2", 1, px.clone());
    assert_eq!(Compression::JpegXlLossless.target_for(&big_endian), None);

    let mut no_pixels = mono.clone();
    no_pixels.remove_element(tags::PIXEL_DATA);
    assert_eq!(Compression::JpegXlLossless.target_for(&no_pixels), None);

    let jxl = transcode(&mono, uids::JPEGXL_LOSSLESS).unwrap();
    assert_eq!(Compression::JpegXlLossless.target_for(&jxl), None);
  }

  #[test]
  fn explicit_le_targets_anything_not_already_explicit_le() {
    let mono = image(
      uids::EXPLICIT_VR_LITTLE_ENDIAN,
      16,
      1,
      "MONOCHROME2",
      1,
      noise(64 * 48 * 2),
    );
    assert_eq!(Compression::ExplicitLe.target_for(&mono), None);
    let jxl = transcode(&mono, uids::JPEGXL_LOSSLESS).unwrap();
    assert_eq!(
      Compression::ExplicitLe.target_for(&jxl),
      Some(uids::EXPLICIT_VR_LITTLE_ENDIAN)
    );
  }

  #[test]
  fn parses_config_values() {
    let parse = |s: &str| serde_yaml::from_str::<Compression>(s).unwrap();
    assert_eq!(parse("none"), Compression::None);
    assert_eq!(parse("jpeg-xl-lossless"), Compression::JpegXlLossless);
    assert_eq!(parse("explicit-le"), Compression::ExplicitLe);
    assert!(serde_yaml::from_str::<Compression>("jpeg-xl").is_err());
  }
}
