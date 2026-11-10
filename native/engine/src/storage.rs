//! Immutable, checksummed batches are authoritative; checkpoints are disposable.
use crate::{Result, RetainedDocument, State};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

const LOG_MAGIC: &[u8; 8] = b"FOLDLOG2";
const LEGACY_LOG_MAGIC: &[u8; 8] = b"FOLDLOG1";
const LOCK_MAGIC: &[u8; 9] = b"FOLDLOCK2";
const CACHE_MAGIC: &[u8; 8] = b"FOLDCAC1";
const FRAME_MAGIC: &[u8; 4] = b"BAT2";
const LEGACY_FRAME_MAGIC: &[u8; 4] = b"BAT1";
const HEADER_SIZE: u64 = 56;
const FRAME_HEADER: usize = 84;
const LEGACY_FRAME_HEADER: usize = 52;
const CACHE_HEADER: u64 = 48;
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const MAX_CACHE_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Batch {
    pub documents: Vec<RetainedDocument>,
}

pub(crate) struct VerifiedBatch {
    pub batch: Batch,
    pub end: u64,
    pub digest: [u8; 32],
}

pub(crate) struct Scan {
    pub batches: Vec<VerifiedBatch>,
    pub recovered: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Checkpoint<StateData = State> {
    pub format: u32,
    pub database_id: String,
    pub module_sha256: String,
    pub offset: u64,
    pub prefix: [u8; 32],
    pub batches: u64,
    pub state: StateData,
}

pub(crate) struct Disk {
    file: File,
    _lock: File,
    path: PathBuf,
    pub database_id: String,
    pub offset: u64,
    pub prefix: [u8; 32],
    pub batches: u64,
    pub poisoned: bool,
    legacy: bool,
}

pub(crate) enum Storage {
    Memory { batches: Vec<Batch> },
    Disk(Disk),
}

fn error(error: impl std::fmt::Display) -> String {
    format!("Native storage: {error}")
}

pub(crate) fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}

fn validate_primary_path(path: &Path) -> Result<()> {
    let name = path
        .file_name()
        .ok_or("A database path must name a file")?
        .to_string_lossy()
        .to_ascii_lowercase();
    if name.ends_with(".cache") || name.contains(".cache.") || name.ends_with(".lock") {
        return Err("Database filenames ending in .cache/.lock or containing .cache. are reserved for native sidecars".into());
    }
    Ok(())
}

fn validate_sidecar(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(
                    "Native cache path must be a regular file, not a symbolic link or directory"
                        .into(),
                );
            }
            let mut magic = [0; 8];
            let mut file = File::open(path).map_err(error)?;
            match file.read_exact(&mut magic) {
                Ok(()) if &magic == LOG_MAGIC || &magic == LEGACY_LOG_MAGIC => {
                    return Err("Native cache path contains an authoritative document log; refusing to replace it".into());
                }
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {}
                Err(e) => return Err(error(e)),
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(error(e)),
    }
}

fn preserve_unrecognized_sidecar(path: &Path) -> Result<()> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(error(e)),
    };
    let mut magic = [0; 8];
    match file.read_exact(&mut magic) {
        Ok(()) if &magic == CACHE_MAGIC => return Ok(()),
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {}
        Err(e) => return Err(error(e)),
    }
    drop(file);
    let preserved = sibling(path, &format!(".unrecognized.{}", Uuid::new_v4()));
    fs::rename(path, preserved).map_err(error)
}

// Test-only, per-thread failure injection runs the real file operations up to
// each durability boundary without exposing a mutable global production hook.
#[cfg(test)]
thread_local! {
    static FAILURE: std::cell::RefCell<Option<&'static str>> = const { std::cell::RefCell::new(None) };
}

fn take_failure(name: &str) -> bool {
    #[cfg(test)]
    return FAILURE.with(|failure| {
        if failure
            .borrow()
            .as_ref()
            .is_some_and(|value| *value == name)
        {
            failure.replace(None);
            true
        } else {
            false
        }
    });
    #[cfg(not(test))]
    {
        let _ = name;
        false
    }
}

fn io_boundary(name: &str) -> Result<()> {
    if take_failure(name) {
        Err(format!("Native storage: injected {name} failure"))
    } else {
        Ok(())
    }
}

fn append_frame(file: &mut File, frame: &[u8]) -> Result<()> {
    if take_failure("append-short-write") || take_failure("append-disk-full") {
        // Leave a real partial frame, so recovery must prove that it preserves
        // the acknowledged prefix rather than merely retrying a no-op failure.
        file.write_all(&frame[..frame.len() / 2]).map_err(error)?;
        return Err("Native storage: injected short write or disk exhaustion".into());
    }
    file.write_all(frame).map_err(error)
}

fn sync_directory(path: &Path) -> Result<()> {
    // Unix fsync of the containing directory makes file creation/rename durable.
    // Windows sync_all flushes the file; opening directories is not portable.
    #[cfg(unix)]
    File::open(
        path.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )
    .and_then(|file| file.sync_all())
    .map_err(error)?;
    Ok(())
}

fn header_version(id: Uuid, legacy: bool) -> Vec<u8> {
    let mut bytes = if legacy { LEGACY_LOG_MAGIC } else { LOG_MAGIC }.to_vec();
    bytes.extend(id.as_bytes());
    let checksum = Sha256::digest(&bytes);
    bytes.extend(checksum);
    bytes
}

fn chained(prefix: &[u8; 32], frame: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(prefix);
    digest.update(frame);
    digest.finalize().into()
}

fn resolve_primary_path(requested: &Path) -> Result<PathBuf> {
    if requested.exists() {
        return fs::canonicalize(requested).map_err(error);
    }
    let mut current = requested.to_path_buf();
    for _ in 0..40 {
        let Ok(metadata) = fs::symlink_metadata(&current) else {
            return Ok(current);
        };
        if !metadata.file_type().is_symlink() {
            return fs::canonicalize(&current).map_err(error);
        }
        let target = fs::read_link(&current).map_err(error)?;
        current = if target.is_absolute() {
            target
        } else {
            current
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(target)
        };
        if current.exists() {
            return fs::canonicalize(&current).map_err(error);
        }
    }
    Err("Too many levels of symbolic links".into())
}

impl Storage {
    pub fn open_with_options(path: &str, reset: bool, create_new: bool) -> Result<Self> {
        Self::open_mode(path, reset, create_new, false)
    }

    fn open_mode(
        path: &str,
        reset: bool,
        create_new: bool,
        migration_source: bool,
    ) -> Result<Self> {
        if path == ":memory:" {
            if create_new {
                return Err("Migration destination must be a persistent log path".into());
            }
            return Ok(Self::Memory {
                batches: Vec::new(),
            });
        }
        let requested = PathBuf::from(path);
        if !migration_source {
            validate_primary_path(&requested)?;
        }
        // A symbolic alias must use its target's sidecars and reservation policy.
        // Actual inode locking below additionally protects hard-link aliases.
        let path = resolve_primary_path(&requested)?;
        if !migration_source {
            validate_primary_path(&path)?;
        }
        if create_new
            && (fs::symlink_metadata(&requested).is_ok() || fs::symlink_metadata(&path).is_ok())
        {
            return Err("Migration destination already exists".into());
        }
        let lock_path = sibling(&path, ".lock");
        if fs::symlink_metadata(&lock_path).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err("Native lock path must not be a symbolic link".into());
        }
        let mut lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(error)?;
        lock.try_lock()
            .map_err(|e| format!("Native storage is already open or cannot be locked: {e}"))?;
        let lock_size = lock.metadata().map_err(error)?.len();
        if lock_size > LOCK_MAGIC.len() as u64 {
            return Err("Native lock path contains an unrelated file".into());
        }
        let mut lock_bytes = vec![0; lock_size as usize];
        lock.read_exact(&mut lock_bytes).map_err(error)?;
        if !LOCK_MAGIC.starts_with(&lock_bytes) {
            return Err("Native lock path contains an unrelated file".into());
        }
        // A legacy primary named `<other>.cache` must never be treated as a
        // disposable accelerator. Check even when that other primary is absent.
        validate_sidecar(&sibling(&path, ".cache"))?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).truncate(false);
        if create_new {
            options.create_new(true);
        } else {
            options.create(true);
        }
        let (mut file, read_only_handle) = if migration_source {
            (File::open(&path).map_err(error)?, true)
        } else {
            match options.open(&path) {
                Ok(handle) => (handle, false),
                Err(write_err) if !reset && !create_new => match File::open(&path) {
                    Ok(handle) => (handle, true),
                    Err(_) => return Err(error(write_err)),
                },
                Err(write_err) => return Err(error(write_err)),
            }
        };
        let initialized = (|| -> Result<(String, [u8; 32], bool)> {
            file.try_lock()
                .map_err(|e| format!("Native storage is already open or cannot be locked: {e}"))?;
            if !read_only_handle && (reset || file.metadata().map_err(error)?.len() == 0) {
                // Keep both coordination and data inode locks for the entire reset.
                // A reset is explicitly destructive; I/O failure is reported rather
                // than hidden, and the stable lock inode is never removed.
                let bytes = header_version(Uuid::new_v4(), false);
                file.seek(SeekFrom::Start(0)).map_err(error)?;
                if take_failure("new-log-header-short-write") {
                    file.write_all(&bytes[..bytes.len() / 2]).map_err(error)?;
                    return Err("Native storage: injected incomplete log header".into());
                }
                file.write_all(&bytes).map_err(error)?;
                file.set_len(HEADER_SIZE).map_err(error)?;
                file.sync_all().map_err(error)?;
                sync_directory(&path)?;
            }
            let mut bytes = [0u8; HEADER_SIZE as usize];
            file.seek(SeekFrom::Start(0)).map_err(error)?;
            file.read_exact(&mut bytes)
                .map_err(|_| "Native log has an incomplete header".to_string())?;
            let legacy = &bytes[..8] == LEGACY_LOG_MAGIC;
            if (&bytes[..8] != LOG_MAGIC && !legacy)
                || Sha256::digest(&bytes[..24])[..] != bytes[24..]
            {
                return Err("Native log header is corrupt or uses an unsupported format".into());
            }
            if read_only_handle && !legacy && !migration_source {
                return Err("Native log requires write access unless it is a read-only legacy FOLDLOG1 log".into());
            }
            let id = Uuid::from_slice(&bytes[8..24]).map_err(error)?.to_string();
            let prefix: [u8; 32] = Sha256::digest(bytes).into();
            if lock_bytes.len() != LOCK_MAGIC.len() {
                // Recover only a bounded known marker prefix, and only after
                // the primary file has been locked and its header validated.
                lock.seek(SeekFrom::Start(0)).map_err(error)?;
                if take_failure("lock-marker-short-write") {
                    lock.write_all(&LOCK_MAGIC[..4]).map_err(error)?;
                    return Err("Native storage: injected incomplete lock marker".into());
                }
                lock.write_all(LOCK_MAGIC).map_err(error)?;
                lock.set_len(LOCK_MAGIC.len() as u64).map_err(error)?;
                lock.sync_all().map_err(error)?;
                sync_directory(&path)?;
            }
            Ok((id, prefix, legacy))
        })();
        let (id, prefix, legacy) = match initialized {
            Ok(metadata) => metadata,
            Err(failure) => {
                if create_new {
                    // `create_new` proved this file belongs to this migration;
                    // retain coordination ownership while removing a failed init.
                    drop(file);
                    if let Err(cleanup) = fs::remove_file(&path)
                        .map_err(error)
                        .and_then(|()| sync_directory(&path))
                    {
                        return Err(format!("{failure}; could not remove incomplete migration destination: {cleanup}"));
                    }
                }
                return Err(failure);
            }
        };
        Ok(Self::Disk(Disk {
            file,
            _lock: lock,
            path,
            database_id: id,
            offset: HEADER_SIZE,
            prefix,
            batches: 0,
            poisoned: false,
            legacy,
        }))
    }

    pub fn migrate(source: &str, destination: &str) -> Result<()> {
        if source == ":memory:" || destination == ":memory:" {
            return Err("Migration requires persistent source and destination paths".into());
        }
        if !Path::new(source).is_file() {
            return Err("Migration source must be an existing document log".into());
        }
        let mut original = Self::open_mode(source, false, false, true)?;
        let scan = match &mut original {
            Self::Disk(disk) => disk.scan_with_recovery(false)?,
            Self::Memory { .. } => unreachable!(),
        };
        let mut identities = std::collections::BTreeSet::new();
        let mut checkpoints = std::collections::BTreeMap::new();
        for verified in &scan.batches {
            for record in &verified.batch.documents {
                crate::validate_record(record)?;
                if !identities.insert((record.source.clone(), record.id.clone()))
                    || checkpoints
                        .get(&record.source)
                        .is_some_and(|p| record.position <= *p)
                {
                    return Err(
                        "Native log has invalid immutable identities or source order".into(),
                    );
                }
                checkpoints.insert(record.source.clone(), record.position);
            }
        }
        let mut copied = Self::open_with_options(destination, false, true)?;
        for verified in scan.batches {
            if let Err(failure) = copied.append(verified.batch) {
                // Only this call's exclusively-created destination is removed,
                // while its stable coordination lock remains held. Never unlink
                // source files or an existing destination supplied by a caller.
                let cleanup = copied.discard_new_destination();
                return Err(match cleanup {
                    Ok(()) => failure,
                    Err(cleanup) => format!(
                        "{failure}; could not remove incomplete migration destination: {cleanup}"
                    ),
                });
            }
        }
        Ok(())
    }

    fn discard_new_destination(self) -> Result<()> {
        let Self::Disk(disk) = self else {
            return Ok(());
        };
        let Disk {
            file, _lock, path, ..
        } = disk;
        drop(file);
        fs::remove_file(&path).map_err(error)?;
        sync_directory(&path)?;
        drop(_lock);
        Ok(())
    }

    pub fn legacy_read_only(&self) -> bool {
        matches!(self, Self::Disk(disk) if disk.legacy)
    }

    pub fn scan(&mut self) -> Result<Scan> {
        match self {
            Self::Memory { batches } => Ok(Scan {
                batches: batches
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(index, batch)| VerifiedBatch {
                        batch,
                        end: index as u64 + 1,
                        digest: [0; 32],
                    })
                    .collect(),
                recovered: 0,
            }),
            Self::Disk(disk) => disk.scan(),
        }
    }

    pub fn append(&mut self, batch: Batch) -> Result<()> {
        match self {
            Self::Memory { batches } => {
                batches.push(batch);
                Ok(())
            }
            Self::Disk(disk) => disk.append(batch),
        }
    }

    pub fn load_checkpoint(&self) -> (Option<Checkpoint>, String, u64) {
        let Self::Disk(disk) = self else {
            return (None, "memory".into(), 0);
        };
        let path = sibling(&disk.path, ".cache");
        let result = (|| -> Result<(Checkpoint, u64)> {
            let mut file = File::open(&path).map_err(error)?;
            let size = file.metadata().map_err(error)?.len();
            if !(CACHE_HEADER..=MAX_CACHE_BYTES).contains(&size) {
                return Err("Invalid checkpoint size".into());
            }
            let mut head = [0u8; CACHE_HEADER as usize];
            file.read_exact(&mut head).map_err(error)?;
            if &head[..8] != CACHE_MAGIC
                || u64::from_le_bytes(head[8..16].try_into().unwrap()) != size - CACHE_HEADER
            {
                return Err("Invalid checkpoint header".into());
            }
            let mut digest = Sha256::new();
            let mut buffer = [0u8; 65536];
            loop {
                let read = file.read(&mut buffer).map_err(error)?;
                if read == 0 {
                    break;
                }
                digest.update(&buffer[..read]);
            }
            if digest.finalize()[..] != head[16..48] {
                return Err("Checkpoint checksum mismatch".into());
            }
            file.seek(SeekFrom::Start(CACHE_HEADER)).map_err(error)?;
            let checkpoint =
                serde_json::from_reader(std::io::BufReader::new(file)).map_err(error)?;
            Ok((checkpoint, size))
        })();
        match result {
            Ok((checkpoint, size)) => (Some(checkpoint), "candidate".into(), size),
            Err(_) if !path.exists() => (None, "missing".into(), 0),
            Err(_) => (
                None,
                "invalid".into(),
                fs::metadata(path).map(|m| m.len()).unwrap_or(0),
            ),
        }
    }

    pub fn checkpoint(&self, state: &State, module_sha256: &str) -> Result<u64> {
        let Self::Disk(disk) = self else { return Ok(0) };
        if disk.poisoned {
            return Err(
                "Native log writer is poisoned; close and reopen before checkpointing".into(),
            );
        }
        let checkpoint = Checkpoint {
            format: 1,
            database_id: disk.database_id.clone(),
            module_sha256: module_sha256.into(),
            offset: disk.offset,
            prefix: disk.prefix,
            batches: disk.batches,
            state,
        };
        let destination = sibling(&disk.path, ".cache");
        validate_sidecar(&destination)?;
        let temporary = sibling(&disk.path, &format!(".cache.{}.tmp", Uuid::new_v4()));
        let result = (|| -> Result<u64> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .map_err(error)?;
            file.write_all(&[0u8; CACHE_HEADER as usize])
                .map_err(error)?;
            let mut writer = DigestWriter {
                writer: std::io::BufWriter::new(&mut file),
                digest: Sha256::new(),
                bytes: 0,
            };
            serde_json::to_writer(&mut writer, &checkpoint).map_err(error)?;
            writer.flush().map_err(error)?;
            let bytes = writer.bytes;
            let digest = writer.digest.clone().finalize();
            drop(writer);
            if bytes + CACHE_HEADER > MAX_CACHE_BYTES {
                return Err("Checkpoint exceeds 1 GiB; log remains authoritative".into());
            }
            file.seek(SeekFrom::Start(0)).map_err(error)?;
            file.write_all(CACHE_MAGIC).map_err(error)?;
            file.write_all(&bytes.to_le_bytes()).map_err(error)?;
            file.write_all(&digest).map_err(error)?;
            io_boundary("checkpoint-file-sync")?;
            file.sync_all().map_err(error)?;
            drop(file);
            validate_sidecar(&destination)?;
            // Unrecognized bytes may be a corrupt cache or an unrelated file.
            // Preserve them rather than silently destroying the only copy.
            preserve_unrecognized_sidecar(&destination)?;
            io_boundary("checkpoint-rename")?;
            fs::rename(&temporary, &destination).map_err(error)?;
            io_boundary("checkpoint-directory-sync")?;
            sync_directory(&destination)?;
            Ok(bytes + CACHE_HEADER)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }

    pub fn poisoned(&self) -> bool {
        matches!(self, Self::Disk(disk) if disk.poisoned)
    }

    pub fn metadata(&self) -> (bool, u64, u64) {
        match self {
            Self::Memory { batches } => (false, 0, batches.len() as u64),
            Self::Disk(disk) => (true, disk.offset, disk.batches),
        }
    }

    pub fn initial_prefix(&self) -> (u64, [u8; 32]) {
        match self {
            Self::Memory { .. } => (0, [0; 32]),
            Self::Disk(disk) => {
                let bytes = header_version(
                    Uuid::parse_str(&disk.database_id).expect("validated UUID"),
                    disk.legacy,
                );
                (HEADER_SIZE, Sha256::digest(bytes).into())
            }
        }
    }
}

impl Disk {
    fn scan(&mut self) -> Result<Scan> {
        self.scan_with_recovery(true)
    }

    fn scan_with_recovery(&mut self, recover: bool) -> Result<Scan> {
        if self.poisoned {
            return Err("Native log writer is poisoned; close and reopen".into());
        }
        let length = self.file.metadata().map_err(error)?.len();
        let bytes = header_version(
            Uuid::parse_str(&self.database_id).expect("validated UUID"),
            self.legacy,
        );
        let mut offset = HEADER_SIZE;
        let mut prefix: [u8; 32] = Sha256::digest(bytes).into();
        let mut batches = Vec::new();
        self.file.seek(SeekFrom::Start(offset)).map_err(error)?;
        let frame_header = if self.legacy {
            LEGACY_FRAME_HEADER
        } else {
            FRAME_HEADER
        };
        let frame_magic = if self.legacy {
            LEGACY_FRAME_MAGIC
        } else {
            FRAME_MAGIC
        };
        while offset < length {
            if length - offset < frame_header as u64 {
                let mut fragment = vec![0u8; (length - offset) as usize];
                self.file.read_exact(&mut fragment).map_err(error)?;
                if fragment[..fragment.len().min(4)] != frame_magic[..fragment.len().min(4)] {
                    return Err(format!(
                        "Native log partial frame header corrupt at byte {offset}"
                    ));
                }
                if fragment.len() >= 12 {
                    let size = u64::from_le_bytes(fragment[4..12].try_into().unwrap());
                    let complement = (!size).to_le_bytes();
                    let available = fragment.len().saturating_sub(12).min(8);
                    if size == 0
                        || size > MAX_FRAME_BYTES as u64
                        || fragment[12..12 + available] != complement[..available]
                    {
                        return Err(format!(
                            "Native log partial frame length corrupt at byte {offset}"
                        ));
                    }
                }
                if !self.legacy
                    && fragment.len() > 52
                    && fragment[52..] != prefix[..fragment.len() - 52]
                {
                    return Err(format!(
                        "Native log partial frame predecessor mismatch at byte {offset}"
                    ));
                }
                break;
            }
            let mut head = vec![0u8; frame_header];
            self.file.read_exact(&mut head).map_err(error)?;
            let size = u64::from_le_bytes(head[4..12].try_into().unwrap());
            let inverse = u64::from_le_bytes(head[12..20].try_into().unwrap());
            if &head[..4] != frame_magic
                || inverse != !size
                || size == 0
                || size > MAX_FRAME_BYTES as u64
            {
                return Err(format!("Native log frame header corrupt at byte {offset}"));
            }
            if !self.legacy && head[52..84] != prefix {
                return Err(format!(
                    "Native log frame predecessor mismatch at byte {offset}"
                ));
            }
            if length - offset - (frame_header as u64) < size {
                break;
            }
            let mut payload = vec![0u8; size as usize];
            self.file.read_exact(&mut payload).map_err(error)?;
            if Sha256::digest(&payload)[..] != head[20..52] {
                return Err(format!("Native log checksum mismatch at byte {offset}"));
            }
            let batch: Batch = serde_json::from_slice(&payload)
                .map_err(|e| format!("Native log invalid batch at byte {offset}: {e}"))?;
            if batch.documents.is_empty() || batch.documents.len() > 1024 {
                return Err("Native log batch document count is corrupt".into());
            }
            let mut frame = head.to_vec();
            frame.extend(payload);
            prefix = chained(&prefix, &frame);
            offset += frame.len() as u64;
            batches.push(VerifiedBatch {
                batch,
                end: offset,
                digest: prefix,
            });
        }
        let recovered = length - offset;
        if recovered > 0 {
            if !recover {
                return Err("Migration source has an incomplete tail; open it normally to recover before migration".into());
            }
            if !self.legacy {
                self.file.set_len(offset).map_err(error)?;
                self.file.sync_all().map_err(error)?;
            }
        }
        self.file.seek(SeekFrom::Start(offset)).map_err(error)?;
        self.offset = offset;
        self.prefix = prefix;
        self.batches = batches.len() as u64;
        Ok(Scan { batches, recovered })
    }

    fn append(&mut self, batch: Batch) -> Result<()> {
        if self.poisoned {
            return Err("Native log writer is poisoned; close and reopen".into());
        }
        if self.legacy {
            return Err("Legacy FOLDLOG1 is read-only; copy it with Database.migrate(source, destination) before appending".into());
        }
        let payload = serde_json::to_vec(&batch).map_err(error)?;
        if payload.is_empty() || payload.len() > MAX_FRAME_BYTES {
            return Err("Document batch exceeds native log frame limit of 8 MiB".into());
        }
        let size = payload.len() as u64;
        let mut frame = FRAME_MAGIC.to_vec();
        frame.extend(size.to_le_bytes());
        frame.extend((!size).to_le_bytes());
        frame.extend(Sha256::digest(&payload));
        frame.extend(self.prefix);
        frame.extend(payload);
        let result = (|| -> Result<()> {
            if self.file.metadata().map_err(error)?.len() != self.offset {
                return Err("Native log changed outside the active writer".into());
            }
            self.file
                .seek(SeekFrom::Start(self.offset))
                .map_err(error)?;
            append_frame(&mut self.file, &frame)?;
            io_boundary("append-sync")?;
            self.file.sync_all().map_err(error)?;
            Ok(())
        })();
        if result.is_err() {
            self.poisoned = true;
            return result
                .map_err(|e| format!("{e}; commit outcome may be uncertain, close and reopen"));
        }
        self.offset += frame.len() as u64;
        self.prefix = chained(&self.prefix, &frame);
        self.batches += 1;
        Ok(())
    }
}

struct DigestWriter<W> {
    writer: W,
    digest: Sha256,
    bytes: u64,
}
impl<W: Write> Write for DigestWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.writer.write(bytes)?;
        self.digest.update(&bytes[..written]);
        self.bytes += written as u64;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod failure_tests {
    use super::FAILURE;
    use crate::Engine;
    use serde_json::{json, Value};
    use std::io::{Read, Seek, SeekFrom};

    const MODULE: &[u8] = include_bytes!("../../../src/foldlitedb/portable_assets/metrics.wasm");

    fn open(path: &std::path::Path) -> Engine {
        Engine::open(path.to_str().unwrap(), MODULE).unwrap()
    }

    fn request(id: &str, value: i64) -> Value {
        json!({"op":"ingest","documents":[{"id":id,"payload":{"metric":"x","value":value}}]})
    }

    fn bytes(engine: &Engine) -> Vec<u8> {
        let Some(super::Storage::Disk(disk)) = engine.storage.as_ref() else {
            panic!("disk required")
        };
        let mut file = &disk.file;
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut result = Vec::new();
        file.read_to_end(&mut result).unwrap();
        result
    }

    fn inject(name: &'static str) {
        FAILURE.with(|failure| assert!(failure.replace(Some(name)).is_none()));
    }

    #[test]
    fn failed_appends_preserve_acknowledged_prefix_poison_and_recover_named_retries() {
        for boundary in ["append-short-write", "append-disk-full", "append-sync"] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("documents.fold");
            let mut owner = open(&path);
            owner.execute(request("acknowledged", 1)).unwrap();
            let prefix = bytes(&owner);
            inject(boundary);
            assert!(owner
                .execute(request("uncertain", 2))
                .unwrap_err()
                .contains("uncertain"));
            assert!(owner.writer_poisoned());
            assert_eq!(
                owner.execute(json!({"op":"stats"})).unwrap()["documents"],
                1
            );
            assert_eq!(
                owner.execute(json!({"op":"query"})).unwrap()["rows"][0]["result"]["sum"],
                1
            );
            assert!(owner.execute(json!({"op":"checkpoint"})).is_err());
            assert!(owner.execute(request("refused", 3)).is_err());
            assert!(bytes(&owner).starts_with(&prefix));
            drop(owner);
            let mut recovered = open(&path);
            let expected_count = if boundary == "append-sync" { 2 } else { 1 };
            assert_eq!(
                recovered.execute(json!({"op":"stats"})).unwrap()["documents"],
                expected_count
            );
            recovered.execute(request("uncertain", 2)).unwrap();
            assert_eq!(
                recovered.execute(request("uncertain", 2)).unwrap()["replayed"],
                1
            );
            assert_eq!(
                recovered.execute(json!({"op":"stats"})).unwrap()["documents"],
                2
            );
            assert_eq!(
                recovered.execute(json!({"op":"query"})).unwrap()["rows"][0]["result"]["sum"],
                3
            );
            assert!(bytes(&recovered).starts_with(&prefix));
        }
    }

    #[test]
    fn failed_checkpoint_boundaries_never_change_source_or_published_results() {
        for boundary in [
            "checkpoint-file-sync",
            "checkpoint-rename",
            "checkpoint-directory-sync",
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("documents.fold");
            let mut owner = open(&path);
            owner.execute(request("one", 1)).unwrap();
            owner.execute(json!({"op":"checkpoint"})).unwrap();
            owner.execute(request("two", 2)).unwrap();
            let original = bytes(&owner);
            let rows = owner.execute(json!({"op":"query"})).unwrap();
            inject(boundary);
            assert!(owner
                .execute(json!({"op":"checkpoint"}))
                .unwrap_err()
                .contains(boundary));
            assert!(!owner.writer_poisoned());
            assert_eq!(bytes(&owner), original);
            assert_eq!(owner.execute(json!({"op":"query"})).unwrap(), rows);
            // Preserve precisely the cache state left by the failed boundary;
            // Drop must not hide the failure by retrying a successful save.
            owner.checkpoint_on_drop = false;
            drop(owner);
            let mut recovered = open(&path);
            assert_eq!(
                recovered.execute(json!({"op":"stats"})).unwrap()["documents"],
                2
            );
            assert_eq!(
                recovered.execute(json!({"op":"query"})).unwrap()["rows"][0]["result"]["sum"],
                3
            );
        }
    }

    #[test]
    fn failed_close_checkpoint_reports_the_error_and_releases_ownership() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("documents.fold");
        let mut owner = open(&path);
        owner.execute(request("one", 1)).unwrap();
        inject("checkpoint-rename");
        assert!(owner.execute(json!({"op":"close"})).is_err());
        let mut recovered = open(&path);
        assert_eq!(
            recovered.execute(json!({"op":"stats"})).unwrap()["documents"],
            1
        );
        recovered.execute(request("two", 2)).unwrap();
    }

    #[test]
    fn migration_failure_removes_only_its_new_destination_and_keeps_source_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.fold");
        let destination = directory.path().join("destination.fold");
        let mut owner = open(&source);
        owner.execute(request("one", 1)).unwrap();
        owner.execute(request("two", 2)).unwrap();
        drop(owner);
        let original = std::fs::read(&source).unwrap();
        for boundary in [
            "new-log-header-short-write",
            "append-short-write",
            "append-sync",
        ] {
            inject(boundary);
            assert!(
                Engine::migrate_log(source.to_str().unwrap(), destination.to_str().unwrap())
                    .is_err()
            );
            assert!(!destination.exists());
            assert_eq!(std::fs::read(&source).unwrap(), original);
        }
        Engine::migrate_log(source.to_str().unwrap(), destination.to_str().unwrap()).unwrap();
        let mut copied = open(&destination);
        assert_eq!(
            copied.execute(json!({"op":"stats"})).unwrap()["documents"],
            2
        );
    }

    #[test]
    fn interrupted_lock_marker_is_repaired_after_validating_existing_documents() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("documents.fold");
        let mut owner = open(&path);
        owner.execute(request("one", 7)).unwrap();
        drop(owner);
        let original = std::fs::read(&path).unwrap();
        let lock_path = super::sibling(&path, ".lock");
        std::fs::write(&lock_path, []).unwrap();
        inject("lock-marker-short-write");
        assert!(Engine::open(path.to_str().unwrap(), MODULE).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert_eq!(std::fs::read(&lock_path).unwrap(), b"FOLD");
        let mut recovered = open(&path);
        assert_eq!(
            recovered.execute(json!({"op":"stats"})).unwrap()["documents"],
            1
        );
        drop(recovered);
        assert_eq!(std::fs::read(&lock_path).unwrap(), super::LOCK_MAGIC);
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
}
