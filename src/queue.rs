//! Durable per-destination spool with atomic writes (part-file + fsync + rename) and retry sidecars.

use dicom_object::{FileDicomObject, InMemDicomObject};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Sidecar {
    pub attempts: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SpooledObject {
    pub dcm_path: PathBuf,
    pub sidecar_path: PathBuf,
    pub attempts: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Snafu)]
pub enum QueueError {
    #[snafu(display("queue I/O error on {}: {source}", path.display()))]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("could not write DICOM object {}: {source}", path.display()))]
    WriteObject {
        path: PathBuf,
        source: dicom_object::WriteError,
    },
    #[snafu(display("could not serialize sidecar: {source}"))]
    Sidecar { source: serde_yaml::Error },
    #[snafu(display(
        "insufficient disk space in {}: {available} bytes free, {required} required",
        path.display()
    ))]
    DiskFull {
        path: PathBuf,
        available: u64,
        required: u64,
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
pub fn enqueue(
    dir: &Path,
    obj: &FileDicomObject<InMemDicomObject>,
    min_free_bytes: u64,
) -> Result<PathBuf> {
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

    obj.write_to_file(&dcm_part)
        .map_err(|e| QueueError::WriteObject {
            path: dcm_part.clone(),
            source: e,
        })?;
    fsync_file(&dcm_part)?;
    std::fs::rename(&dcm_part, &dcm_final).map_err(io(&dcm_final))?;

    let yaml = serde_yaml::to_string(&Sidecar::default())
        .map_err(|e| QueueError::Sidecar { source: e })?;
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
            path: dcm_path.to_path_buf(),
            source: e,
        }),
        (_, Some(e)) if e.kind() != std::io::ErrorKind::NotFound => Err(QueueError::Io {
            path: sidecar_path,
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

/// List committed objects. Cleans up `.part` files and orphan `.dcm` files.
pub fn scan(dir: &Path) -> Result<Vec<SpooledObject>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(dir).map_err(io(dir))? {
        let entry = entry.map_err(io(dir))?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".part") {
            let _ = std::fs::remove_file(&path);
            continue;
        }
        if !name.ends_with(".dcm") {
            continue;
        }
        let sidecar_path = path.with_extension("yaml");
        if !sidecar_path.exists() {
            let _ = std::fs::remove_file(&path);
            continue;
        }
        let text = std::fs::read_to_string(&sidecar_path).map_err(io(&sidecar_path))?;
        let sidecar: Sidecar = serde_yaml::from_str(&text).unwrap_or_default();
        out.push(SpooledObject {
            dcm_path: path,
            sidecar_path,
            attempts: sidecar.attempts,
            last_error: sidecar.last_error,
        });
    }
    out.sort_by(|a, b| a.dcm_path.cmp(&b.dcm_path));
    Ok(out)
}

/// Persist a failed attempt (increments counter, records error).
pub fn record_failure(obj: &SpooledObject, error: &str) -> Result<()> {
    let sidecar = Sidecar {
        attempts: obj.attempts + 1,
        last_error: Some(error.chars().take(500).collect()),
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
    std::fs::rename(&obj.sidecar_path, dead_letter_dir.join(sc_name))
        .map_err(io(&obj.sidecar_path))?;
    fsync_dir(dead_letter_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dicom_core::{dicom_value, DataElement, VR};
    use dicom_dictionary_std::{tags, uids};
    use dicom_object::{FileMetaTableBuilder, InMemDicomObject};

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
        std::fs::write(dir.path().join("2.2.2.part"), b"junk").unwrap();
        std::fs::write(dir.path().join("3.3.3.dcm"), b"orphan").unwrap();
        let pending = scan(dir.path()).unwrap();
        assert!(pending.is_empty());
        assert!(!dir.path().join("2.2.2.part").exists());
        assert!(!dir.path().join("3.3.3.dcm").exists());
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
