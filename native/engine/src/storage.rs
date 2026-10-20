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

const LOG_MAGIC: &[u8; 8] = b"FOLDLOG1";
const CACHE_MAGIC: &[u8; 8] = b"FOLDCAC1";
const FRAME_MAGIC: &[u8; 4] = b"BAT1";
const HEADER_SIZE: u64 = 56;
const FRAME_HEADER: usize = 52;
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

fn header(id: Uuid) -> Vec<u8> {
    let mut bytes = LOG_MAGIC.to_vec();
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

impl Storage {
    pub fn open(path: &str) -> Result<Self> {
        if path == ":memory:" {
            return Ok(Self::Memory {
                batches: Vec::new(),
            });
        }
        let path = PathBuf::from(path);
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(sibling(&path, ".lock"))
            .map_err(error)?;
        lock.try_lock()
            .map_err(|e| format!("Native storage is already open or cannot be locked: {e}"))?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(error)?;
        // Lock the actual inode/handle too: aliases must not create two writers
        // simply because their sibling lock paths differ.
        file.try_lock()
            .map_err(|e| format!("Native storage is already open or cannot be locked: {e}"))?;
        if file.metadata().map_err(error)?.len() == 0 {
            file.write_all(&header(Uuid::new_v4())).map_err(error)?;
            file.sync_all().map_err(error)?;
            sync_directory(&path)?;
        }
        let mut bytes = [0u8; HEADER_SIZE as usize];
        file.seek(SeekFrom::Start(0)).map_err(error)?;
        file.read_exact(&mut bytes)
            .map_err(|_| "Native log has an incomplete header".to_string())?;
        if &bytes[..8] != LOG_MAGIC || Sha256::digest(&bytes[..24])[..] != bytes[24..] {
            return Err("Native log header is corrupt or uses an unsupported format".into());
        }
        let id = Uuid::from_slice(&bytes[8..24]).map_err(error)?.to_string();
        let prefix = Sha256::digest(bytes).into();
        Ok(Self::Disk(Disk {
            file,
            _lock: lock,
            path,
            database_id: id,
            offset: HEADER_SIZE,
            prefix,
            batches: 0,
            poisoned: false,
        }))
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
            file.sync_all().map_err(error)?;
            drop(file);
            fs::rename(&temporary, &destination).map_err(error)?;
            sync_directory(&destination)?;
            Ok(bytes + CACHE_HEADER)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
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
                let bytes = header(Uuid::parse_str(&disk.database_id).expect("validated UUID"));
                (HEADER_SIZE, Sha256::digest(bytes).into())
            }
        }
    }
}

impl Disk {
    fn scan(&mut self) -> Result<Scan> {
        if self.poisoned {
            return Err("Native log writer is poisoned; close and reopen".into());
        }
        let length = self.file.metadata().map_err(error)?.len();
        let bytes = header(Uuid::parse_str(&self.database_id).expect("validated UUID"));
        let mut offset = HEADER_SIZE;
        let mut prefix: [u8; 32] = Sha256::digest(bytes).into();
        let mut batches = Vec::new();
        self.file.seek(SeekFrom::Start(offset)).map_err(error)?;
        while offset < length {
            if length - offset < FRAME_HEADER as u64 {
                let mut fragment = vec![0u8; (length - offset) as usize];
                self.file.read_exact(&mut fragment).map_err(error)?;
                if fragment[..fragment.len().min(4)] != FRAME_MAGIC[..fragment.len().min(4)] {
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
                break;
            }
            let mut head = [0u8; FRAME_HEADER];
            self.file.read_exact(&mut head).map_err(error)?;
            let size = u64::from_le_bytes(head[4..12].try_into().unwrap());
            let inverse = u64::from_le_bytes(head[12..20].try_into().unwrap());
            if &head[..4] != FRAME_MAGIC
                || inverse != !size
                || size == 0
                || size > MAX_FRAME_BYTES as u64
            {
                return Err(format!("Native log frame header corrupt at byte {offset}"));
            }
            if length - offset - (FRAME_HEADER as u64) < size {
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
            self.file.set_len(offset).map_err(error)?;
            self.file.sync_all().map_err(error)?;
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
        let payload = serde_json::to_vec(&batch).map_err(error)?;
        if payload.is_empty() || payload.len() > MAX_FRAME_BYTES {
            return Err("Document batch exceeds native log frame limit of 8 MiB".into());
        }
        let size = payload.len() as u64;
        let mut frame = FRAME_MAGIC.to_vec();
        frame.extend(size.to_le_bytes());
        frame.extend((!size).to_le_bytes());
        frame.extend(Sha256::digest(&payload));
        frame.extend(payload);
        let result = (|| -> Result<()> {
            if self.file.metadata().map_err(error)?.len() != self.offset {
                return Err("Native log changed outside the active writer".into());
            }
            self.file
                .seek(SeekFrom::Start(self.offset))
                .map_err(error)?;
            self.file.write_all(&frame).map_err(error)?;
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
