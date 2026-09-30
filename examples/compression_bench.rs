//! Measure outbound compression on real DICOM files.
//!
//! ```bash
//! cargo run --release --example compression_bench -- path/to/*.dcm
//! ```
//!
//! For each file: wire size before/after JPEG XL Lossless, encode time
//! (including the lossless verification decode) and decode time on the
//! receiving side. Files that would be forwarded unchanged are reported as
//! skipped.

use std::time::{Duration, Instant};

use dicom_dictionary_std::uids;
use dicom_encoding::TransferSyntaxIndex;
use dicom_router::compression::{self, Compression, DicomFile};
use dicom_transfer_syntax_registry::TransferSyntaxRegistry;

fn wire_size(file: &DicomFile) -> usize {
  let ts = TransferSyntaxRegistry.get(file.meta().transfer_syntax()).unwrap();
  let mut out = Vec::new();
  file.write_dataset_with_ts(&mut out, ts).unwrap();
  out.len()
}

fn main() {
  let (mut raw_total, mut jxl_total) = (0usize, 0usize);
  let (mut encode_total, mut decode_total) = (Duration::ZERO, Duration::ZERO);
  println!(
    "{:<60} {:>12} {:>12} {:>7} {:>10} {:>10}",
    "file", "raw", "jxl", "ratio", "enc ms", "dec ms"
  );
  for path in std::env::args().skip(1) {
    let file = match dicom_object::open_file(&path) {
      Ok(f) => f,
      Err(e) => {
        println!("{path:<60} unreadable: {e}");
        continue;
      }
    };
    let name: String = path
      .chars()
      .rev()
      .take(60)
      .collect::<Vec<_>>()
      .into_iter()
      .rev()
      .collect();
    if Compression::JpegXlLossless.target_for(&file).is_none() {
      println!("{name:<60} skipped (ts {})", file.meta().transfer_syntax());
      continue;
    }
    let raw = wire_size(&file);
    let started = Instant::now();
    let jxl = match compression::transcode(&file, uids::JPEGXL_LOSSLESS) {
      Ok(f) => f,
      Err(e) => {
        println!("{name:<60} transcode failed: {e}");
        continue;
      }
    };
    let encode = started.elapsed();
    let started = Instant::now();
    compression::transcode(&jxl, uids::EXPLICIT_VR_LITTLE_ENDIAN).unwrap();
    let decode = started.elapsed();
    let size = wire_size(&jxl);
    println!(
      "{name:<60} {raw:>12} {size:>12} {:>6.2}x {:>10.1} {:>10.1}",
      raw as f64 / size as f64,
      encode.as_secs_f64() * 1e3,
      decode.as_secs_f64() * 1e3
    );
    raw_total += raw;
    jxl_total += size;
    encode_total += encode;
    decode_total += decode;
  }
  if jxl_total > 0 {
    println!(
      "{:<60} {raw_total:>12} {jxl_total:>12} {:>6.2}x {:>10.1} {:>10.1}",
      "TOTAL",
      raw_total as f64 / jxl_total as f64,
      encode_total.as_secs_f64() * 1e3,
      decode_total.as_secs_f64() * 1e3
    );
  }
}
