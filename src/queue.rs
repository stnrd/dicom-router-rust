//! Durable per-destination spool with atomic writes (part-file + fsync +
//! rename) and retry sidecars.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use dicom_object::{FileDicomObject, InMemDicomObject};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

/// Default age threshold for [`cleanup_stale`]: files older than this with no
/// matching sidecar (or any `.part` suffix) are treated as crash debris.
pub const DEFAULT_STALE_MAX_AGE: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Sidecar {
  pub attempts:   u32,
  pub last_error: Option<String>,
  #[serde(default)]
  pub delivered:  bool,
}

#[derive(Debug, Clone)]
pub struct SpooledObject {
  pub dcm_path:     PathBuf,
  pub sidecar_path: PathBuf,
  pub attempts:     u32,
  pub last_error:   Option<String>,
  pub delivered:    bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CleanupStats {
  pub part_files_removed: u32,
  pub orphan_dcm_removed: u32,
}

#[derive(Debug, Snafu)]
pub enum QueueError {
  #[snafu(display("queue I/O error on {}: {source}", path.display()))]
  Io { path: PathBuf, source: std::io::Error },
  #[snafu(display("could not write DICOM object {}: {source}", path.display()))]
  WriteObject {
    path:   PathBuf,
    source: Box<dicom_object::WriteError>,
  },
  #[snafu(display("could not serialize sidecar: {source}"))]
  Sidecar { source: serde_yaml::Error },
  #[snafu(display(
        "insufficient disk space in {}: {available} bytes free, {required} required",
        path.display()
    ))]
  DiskFull {
    path:      PathBuf,
    available: u64,
    required:  u64,
  },
}

type Result<T> = std::result::Result<T, QueueError>;

fn io(path: &Path) -> impl Fn(std::io::Error) -> QueueError + '_ {
  move |source| QueueError::Io {
    path: path.to_path_buf(),
    source,
  }
}

/// Atomically persist `obj` for later forwarding. Fails before writing if the
/// filesystem has less than `min_free_bytes` available.
pub fn enqueue(dir: &Path, obj: &FileDicomObject<InMemDicomObject>, min_free_bytes: u64) -> Result<PathBuf> {
  std::fs::create_dir_all(dir).map_err(io(dir))?;
  let available = fs4::available_space(dir).map_err(io(dir))?;
  if available < min_free_bytes {
    return Err(QueueError::DiskFull {
      path: dir.to_path_buf(),
      available,
      required: min_free_bytes,
    });
  }

  let uid = obj
    .meta()
    .media_storage_sop_instance_uid
    .trim_end_matches(['\0', ' '])
    .to_string();
  let dcm_part = dir.join(format!("{uid}.dcm.part"));
  let dcm_final = dir.join(format!("{uid}.dcm"));
  let sidecar_part = dir.join(format!("{uid}.yaml.part"));
  let sidecar_final = dir.join(format!("{uid}.yaml"));

  obj.write_to_file(&dcm_part).map_err(|e| QueueError::WriteObject {
    path:   dcm_part.clone(),
    source: Box::new(e),
  })?;
  fsync_file(&dcm_part)?;
  std::fs::rename(&dcm_part, &dcm_final).map_err(io(&dcm_final))?;

  let yaml = serde_yaml::to_string(&Sidecar::default()).map_err(|e| QueueError::Sidecar { source: e })?;
  std::fs::write(&sidecar_part, yaml).map_err(io(&sidecar_part))?;
  fsync_file(&sidecar_part)?;
  std::fs::rename(&sidecar_part, &sidecar_final).map_err(io(&sidecar_final))?;
  fsync_dir(dir)?;
  Ok(dcm_final)
}

/// Remove a committed spool entry (`.dcm` + `.yaml`). Used to roll back a
/// partial fan-out when a later destination enqueue fails.
///
/// Best-effort and idempotent: missing files are ignored. Returns the first I/O
/// error encountered while removing files that do exist.
pub fn rollback(dcm_path: &Path) -> Result<()> {
  let sidecar_path = dcm_path.with_extension("yaml");
  let dcm_err = std::fs::remove_file(dcm_path).err();
  let sc_err = std::fs::remove_file(&sidecar_path).err();
  match (dcm_err, sc_err) {
    (Some(e), _) if e.kind() != std::io::ErrorKind::NotFound => Err(QueueError::Io {
      path:   dcm_path.to_path_buf(),
      source: e,
    }),
    (_, Some(e)) if e.kind() != std::io::ErrorKind::NotFound => Err(QueueError::Io {
      path:   sidecar_path,
      source: e,
    }),
    _ => Ok(()),
  }
}

/// Atomically spool `obj` to every directory in `dirs`.
///
/// On success, returns the final `.dcm` path for each directory. On any
/// failure, rolls back all destinations that were already written and returns
/// the error from the failing `enqueue` call.
pub fn enqueue_fanout(
  dirs: &[&Path],
  obj: &FileDicomObject<InMemDicomObject>,
  min_free_bytes: u64,
) -> Result<Vec<PathBuf>> {
  let mut committed = Vec::new();
  for dir in dirs {
    match enqueue(dir, obj, min_free_bytes) {
      Ok(path) => committed.push(path),
      Err(e) => {
        for path in &committed {
          let _ = rollback(path);
        }
        return Err(e);
      }
    }
  }
  Ok(committed)
}

fn fsync_file(path: &Path) -> Result<()> {
  std::fs::File::open(path)
    .map_err(io(path))?
    .sync_all()
    .map_err(io(path))
}

fn fsync_dir(path: &Path) -> Result<()> {
  std::fs::File::open(path)
    .map_err(io(path))?
    .sync_all()
    .map_err(io(path))
}

/// List committed objects ready for forwarding (`.dcm` + matching `.yaml`).
///
/// This function is read-only: it never deletes files. In-progress enqueues
/// (`.part` files, `.dcm` without a sidecar yet) are skipped so concurrent
/// dispatcher scans cannot race with [`enqueue`].
pub fn scan(dir: &Path) -> Result<Vec<SpooledObject>> {
  let mut out = Vec::new();
  if !dir.exists() {
    return Ok(out);
  }
  for entry in std::fs::read_dir(dir).map_err(io(dir))? {
    let entry = entry.map_err(io(dir))?;
    let path = entry.path();
    let name = entry.file_name().to_string_lossy().to_string();
    if name.ends_with(".part") || !name.ends_with(".dcm") {
      continue;
    }
    let sidecar_path = path.with_extension("yaml");
    if !sidecar_path.exists() {
      continue;
    }
    let text = std::fs::read_to_string(&sidecar_path).map_err(io(&sidecar_path))?;
    let sidecar: Sidecar = serde_yaml::from_str(&text).unwrap_or_default();
    out.push(SpooledObject {
      dcm_path: path,
      sidecar_path,
      attempts: sidecar.attempts,
      last_error: sidecar.last_error,
      delivered: sidecar.delivered,
    });
  }
  out.sort_by(|a, b| a.dcm_path.cmp(&b.dcm_path));
  Ok(out)
}

/// Remove abandoned `.part` files and orphan `.dcm` files older than `max_age`.
///
/// Call at router startup (and optionally on a slow timer) — not from the hot
/// dispatcher scan loop, which would race with in-flight [`enqueue`] writes.
pub fn cleanup_stale(dir: &Path, max_age: Duration) -> Result<CleanupStats> {
  let mut stats = CleanupStats::default();
  if !dir.exists() {
    return Ok(stats);
  }
  for entry in std::fs::read_dir(dir).map_err(io(dir))? {
    let entry = entry.map_err(io(dir))?;
    let path = entry.path();
    let name = entry.file_name().to_string_lossy().to_string();
    if name.ends_with(".part") {
      if is_older_than(&path, max_age)? {
        let _ = std::fs::remove_file(&path);
        stats.part_files_removed += 1;
      }
      continue;
    }
    if name.ends_with(".dcm") {
      let sidecar_path = path.with_extension("yaml");
      if !sidecar_path.exists() && is_older_than(&path, max_age)? {
        let _ = std::fs::remove_file(&path);
        stats.orphan_dcm_removed += 1;
      }
    }
  }
  Ok(stats)
}

fn is_older_than(path: &Path, max_age: Duration) -> Result<bool> {
  let mtime = std::fs::metadata(path)
    .map_err(io(path))?
    .modified()
    .map_err(io(path))?;
  let cutoff = SystemTime::now().checked_sub(max_age).unwrap_or(SystemTime::UNIX_EPOCH);
  Ok(mtime < cutoff)
}

/// Persist a failed attempt (increments counter, records error).
pub fn record_failure(obj: &SpooledObject, error: &str) -> Result<()> {
  let sidecar = Sidecar {
    attempts:   obj.attempts + 1,
    last_error: Some(error.chars().take(500).collect()),
    delivered:  false,
  };
  let yaml = serde_yaml::to_string(&sidecar).map_err(|e| QueueError::Sidecar { source: e })?;
  let part = obj.sidecar_path.with_extension("yaml.part");
  std::fs::write(&part, yaml).map_err(io(&part))?;
  fsync_file(&part)?;
  std::fs::rename(&part, &obj.sidecar_path).map_err(io(&obj.sidecar_path))
}

/// Mark an object as successfully forwarded so a failed `acknowledge` does not
/// trigger a duplicate C-STORE on retry.
pub fn mark_delivered(obj: &SpooledObject) -> Result<()> {
  let sidecar = Sidecar {
    attempts:   obj.attempts,
    last_error: obj.last_error.clone(),
    delivered:  true,
  };
  let yaml = serde_yaml::to_string(&sidecar).map_err(|e| QueueError::Sidecar { source: e })?;
  let part = obj.sidecar_path.with_extension("yaml.part");
  std::fs::write(&part, yaml).map_err(io(&part))?;
  fsync_file(&part)?;
  std::fs::rename(&part, &obj.sidecar_path).map_err(io(&obj.sidecar_path))
}

/// Delete object + sidecar after the destination confirmed storage.
pub fn acknowledge(obj: &SpooledObject) -> Result<()> {
  let _ = std::fs::remove_file(&obj.sidecar_path);
  std::fs::remove_file(&obj.dcm_path).map_err(io(&obj.dcm_path))
}

/// Move object + sidecar to the dead-letter directory.
pub fn move_to_dead_letter(obj: &SpooledObject, dead_letter_dir: &Path) -> Result<()> {
  std::fs::create_dir_all(dead_letter_dir).map_err(io(dead_letter_dir))?;
  let dcm_name = obj.dcm_path.file_name().unwrap_or_default();
  let sc_name = obj.sidecar_path.file_name().unwrap_or_default();
  std::fs::rename(&obj.dcm_path, dead_letter_dir.join(dcm_name)).map_err(io(&obj.dcm_path))?;
  std::fs::rename(&obj.sidecar_path, dead_letter_dir.join(sc_name)).map_err(io(&obj.sidecar_path))?;
  fsync_dir(dead_letter_dir)
}

#[cfg(test)]
mod tests {
  use std::time::Duration;

  use dicom_core::{dicom_value, DataElement, VR};
  use dicom_dictionary_std::{tags, uids};
  use dicom_object::{FileMetaTableBuilder, InMemDicomObject};

  use super::*;

  fn set_mtime_old(path: &std::path::Path) {
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    let old = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    file.set_modified(old).unwrap();
  }

  fn test_object(sop_instance_uid: &str) -> dicom_object::FileDicomObject<InMemDicomObject> {
    let mut obj = InMemDicomObject::new_empty();
    obj.put(DataElement::new(
      tags::SOP_CLASS_UID,
      VR::UI,
      dicom_value!(Str, uids::CT_IMAGE_STORAGE),
    ));
    obj.put(DataElement::new(
      tags::SOP_INSTANCE_UID,
      VR::UI,
      dicom_value!(Str, sop_instance_uid),
    ));
    obj.put(DataElement::new(
      tags::PATIENT_NAME,
      VR::PN,
      dicom_value!(Str, "TEST^QUEUE"),
    ));
    let meta = FileMetaTableBuilder::new()
      .media_storage_sop_class_uid(uids::CT_IMAGE_STORAGE)
      .media_storage_sop_instance_uid(sop_instance_uid)
      .transfer_syntax(uids::EXPLICIT_VR_LITTLE_ENDIAN)
      .build()
      .unwrap();
    obj.with_exact_meta(meta)
  }

  #[test]
  fn enqueue_then_scan_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let obj = test_object("1.2.3.1");
    enqueue(dir.path(), &obj, 0).unwrap();
    let pending = scan(dir.path()).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].attempts, 0);
    assert!(pending[0].dcm_path.ends_with("1.2.3.1.dcm"));
  }

  #[test]
  fn stray_part_and_orphan_dcm_are_cleaned() {
    let dir = tempfile::tempdir().unwrap();
    let part = dir.path().join("2.2.2.part");
    let orphan = dir.path().join("3.3.3.dcm");
    std::fs::write(&part, b"junk").unwrap();
    std::fs::write(&orphan, b"orphan").unwrap();
    set_mtime_old(&part);
    set_mtime_old(&orphan);

    assert!(scan(dir.path()).unwrap().is_empty());
    assert!(part.exists());
    assert!(orphan.exists());

    let stats = cleanup_stale(dir.path(), Duration::from_secs(1)).unwrap();
    assert_eq!(stats.part_files_removed, 1);
    assert_eq!(stats.orphan_dcm_removed, 1);
    assert!(!part.exists());
    assert!(!orphan.exists());
  }

  #[test]
  fn scan_does_not_delete_part_files() {
    let dir = tempfile::tempdir().unwrap();
    let part = dir.path().join("inflight.dcm.part");
    std::fs::write(&part, b"writing").unwrap();
    set_mtime_old(&part);

    assert!(scan(dir.path()).unwrap().is_empty());
    assert!(part.exists());
  }

  #[test]
  fn scan_does_not_delete_dcm_without_yaml() {
    let dir = tempfile::tempdir().unwrap();
    let dcm = dir.path().join("inflight.dcm");
    std::fs::write(&dcm, b"partial").unwrap();
    set_mtime_old(&dcm);

    assert!(scan(dir.path()).unwrap().is_empty());
    assert!(dcm.exists());
  }

  #[test]
  fn cleanup_stale_keeps_young_part_and_orphan_dcm() {
    let dir = tempfile::tempdir().unwrap();
    let part = dir.path().join("fresh.part");
    let orphan = dir.path().join("fresh.dcm");
    std::fs::write(&part, b"new").unwrap();
    std::fs::write(&orphan, b"new").unwrap();

    let stats = cleanup_stale(dir.path(), Duration::from_secs(3600)).unwrap();
    assert_eq!(stats, CleanupStats::default());
    assert!(part.exists());
    assert!(orphan.exists());
  }

  #[test]
  fn scan_does_not_interfere_with_enqueue() {
    let dir = tempfile::tempdir().unwrap();
    let scan_dir = dir.path().to_path_buf();
    let scanner = std::thread::spawn(move || {
      for _ in 0..500 {
        let _ = scan(&scan_dir);
      }
    });

    for i in 0..20 {
      enqueue(dir.path(), &test_object(&format!("race.{i}")), 0).unwrap();
    }
    scanner.join().unwrap();

    assert_eq!(scan(dir.path()).unwrap().len(), 20);
  }

  #[test]
  fn record_failure_increments_attempts() {
    let dir = tempfile::tempdir().unwrap();
    enqueue(dir.path(), &test_object("4.4.4"), 0).unwrap();
    let pending = scan(dir.path()).unwrap();
    record_failure(&pending[0], "connection refused").unwrap();
    let pending = scan(dir.path()).unwrap();
    assert_eq!(pending[0].attempts, 1);
    assert_eq!(pending[0].last_error.as_deref(), Some("connection refused"));
  }

  #[test]
  fn rollback_removes_dcm_and_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let obj = test_object("7.7.7");
    let path = enqueue(dir.path(), &obj, 0).unwrap();
    assert!(path.exists());
    rollback(&path).unwrap();
    assert!(!path.exists());
    assert!(!path.with_extension("yaml").exists());
    assert!(scan(dir.path()).unwrap().is_empty());
  }

  #[test]
  fn enqueue_fanout_all_or_nothing_on_disk_full() {
    let root = tempfile::tempdir().unwrap();
    let ok_dir = root.path().join("dest-a");
    let full_dir = root.path().join("dest-b");
    std::fs::create_dir_all(&ok_dir).unwrap();
    std::fs::create_dir_all(&full_dir).unwrap();

    let obj = test_object("8.8.8");
    let dirs = [ok_dir.as_path(), full_dir.as_path()];

    let result = enqueue_fanout(&dirs, &obj, u64::MAX);
    assert!(result.is_err(), "second destination should fail disk check");

    assert!(scan(&ok_dir).unwrap().is_empty());
    assert!(scan(&full_dir).unwrap().is_empty());
  }

  #[test]
  fn enqueue_fanout_succeeds_for_all_destinations() {
    let root = tempfile::tempdir().unwrap();
    let dir_a = root.path().join("dest-a");
    let dir_b = root.path().join("dest-b");
    std::fs::create_dir_all(&dir_a).unwrap();
    std::fs::create_dir_all(&dir_b).unwrap();

    let obj = test_object("9.9.9");
    let dirs = [dir_a.as_path(), dir_b.as_path()];
    let paths = enqueue_fanout(&dirs, &obj, 0).unwrap();
    assert_eq!(paths.len(), 2);
    assert_eq!(scan(&dir_a).unwrap().len(), 1);
    assert_eq!(scan(&dir_b).unwrap().len(), 1);
  }

  #[test]
  fn mark_delivered_persists_without_removing_files() {
    let dir = tempfile::tempdir().unwrap();
    enqueue(dir.path(), &test_object("10.10.10"), 0).unwrap();
    let pending = scan(dir.path()).unwrap();
    assert!(!pending[0].delivered);
    mark_delivered(&pending[0]).unwrap();
    let pending = scan(dir.path()).unwrap();
    assert_eq!(pending.len(), 1);
    assert!(pending[0].delivered);
    assert!(pending[0].dcm_path.exists());
  }

  #[test]
  fn acknowledge_and_dead_letter_remove_from_queue() {
    let dir = tempfile::tempdir().unwrap();
    let dead = tempfile::tempdir().unwrap();
    enqueue(dir.path(), &test_object("5.5.5"), 0).unwrap();
    enqueue(dir.path(), &test_object("6.6.6"), 0).unwrap();
    let pending = scan(dir.path()).unwrap();
    acknowledge(&pending[0]).unwrap();
    move_to_dead_letter(&pending[1], dead.path()).unwrap();
    assert!(scan(dir.path()).unwrap().is_empty());
    assert_eq!(scan(dead.path()).unwrap().len(), 1);
  }
}
