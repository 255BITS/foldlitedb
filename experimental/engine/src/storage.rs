//! Both backends persist exactly the same JSON records and SHA256 checksums.
use crate::Record;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"FLDLOG\x02\0";
const HEADER: usize = 80;
const MAX_FRAME: usize = 64 * 1024 * 1024;

pub(crate) trait Backend: Send {
    fn scan(&mut self, visit: &mut dyn FnMut(Record) -> Result<(), String>) -> Result<(), String>;
    fn append(&mut self, record: &Record) -> Result<(), String>;
    fn bytes(&self) -> u64;
}

fn encode(record: &Record) -> Result<(Vec<u8>, Vec<u8>), String> {
    let data = serde_json::to_vec(record).map_err(|e| e.to_string())?;
    if data.len() > MAX_FRAME {
        return Err("Transaction exceeds 64 MiB frame budget".into());
    }
    let digest = Sha256::digest(&data).to_vec();
    Ok((data, digest))
}

fn decode(data: &[u8], digest: &[u8]) -> Result<Record, String> {
    if data.len() > MAX_FRAME || Sha256::digest(data).as_slice() != digest {
        return Err("Corrupt committed transaction checksum".into());
    }
    serde_json::from_slice(data).map_err(|e| format!("Corrupt committed transaction: {e}"))
}

pub(crate) fn open(path: &str, backend: &str) -> Result<Box<dyn Backend>, String> {
    match backend {
        "sqlite" => Ok(Box::new(Sqlite::open(path)?)),
        "log" => Ok(Box::new(Log::open(path)?)),
        _ => Err("backend must be sqlite or log".into()),
    }
}

struct Sqlite {
    connection: Connection,
    path: PathBuf,
    failed: bool,
}

impl Sqlite {
    fn open(path: &str) -> Result<Self, String> {
        let connection = Connection::open(path).map_err(|e| e.to_string())?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; \
             CREATE TABLE IF NOT EXISTS engine_records(ordinal INTEGER PRIMARY KEY, \
             data BLOB NOT NULL, digest BLOB NOT NULL);",
            )
            .map_err(|e| e.to_string())?;
        Ok(Self {
            connection,
            path: path.into(),
            failed: false,
        })
    }
}

impl Backend for Sqlite {
    fn scan(&mut self, visit: &mut dyn FnMut(Record) -> Result<(), String>) -> Result<(), String> {
        let mut statement = self
            .connection
            .prepare("SELECT data,digest FROM engine_records ORDER BY ordinal")
            .map_err(|e| e.to_string())?;
        let mut rows = statement.query([]).map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let data: Vec<u8> = row.get(0).map_err(|e| e.to_string())?;
            let digest: Vec<u8> = row.get(1).map_err(|e| e.to_string())?;
            visit(decode(&data, &digest)?)?;
        }
        Ok(())
    }

    fn append(&mut self, record: &Record) -> Result<(), String> {
        if self.failed {
            return Err("Storage write failed; reopen the engine".into());
        }
        let (data, digest) = encode(record)?;
        let result = (|| {
            let tx = self.connection.transaction().map_err(|e| e.to_string())?;
            tx.execute(
                "INSERT INTO engine_records(data,digest) VALUES(?,?)",
                params![data, digest],
            )
            .map_err(|e| e.to_string())?;
            tx.commit().map_err(|e| e.to_string())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn bytes(&self) -> u64 {
        let main = self.path.metadata().map(|m| m.len()).unwrap_or(0);
        let wal = PathBuf::from(format!("{}-wal", self.path.display()))
            .metadata()
            .map(|m| m.len())
            .unwrap_or(0);
        let shm = PathBuf::from(format!("{}-shm", self.path.display()))
            .metadata()
            .map(|m| m.len())
            .unwrap_or(0);
        main + wal + shm
    }
}

struct Log {
    file: File,
    path: PathBuf,
    failed: bool,
}

impl Log {
    fn open(path: &str) -> Result<Self, String> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            file,
            path: path.into(),
            failed: false,
        })
    }

    fn repair(&mut self, boundary: u64) -> Result<(), String> {
        self.file.set_len(boundary).map_err(|e| e.to_string())?;
        self.file.sync_all().map_err(|e| e.to_string())?;
        Ok(())
    }
}

impl Backend for Log {
    fn scan(&mut self, visit: &mut dyn FnMut(Record) -> Result<(), String>) -> Result<(), String> {
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|e| e.to_string())?;
        let size = self.file.metadata().map_err(|e| e.to_string())?.len();
        let mut offset = 0;
        while offset < size {
            if size - offset < HEADER as u64 {
                self.repair(offset)?;
                break;
            }
            let mut header = [0; HEADER];
            self.file
                .read_exact(&mut header)
                .map_err(|e| e.to_string())?;
            if &header[..8] != MAGIC {
                return Err("Corrupt transaction frame magic".into());
            }
            // Authenticate every header field before trusting length to decide
            // whether EOF is a torn write. Otherwise an increased corrupt
            // length could silently discard an acknowledged complete record.
            if Sha256::digest(&header[..48]).as_slice() != &header[48..] {
                return Err("Corrupt transaction header checksum".into());
            }
            let length = u64::from_le_bytes(header[8..16].try_into().unwrap());
            if length == 0 || length > MAX_FRAME as u64 {
                return Err("Corrupt transaction frame length".into());
            }
            if size - offset - (HEADER as u64) < length {
                self.repair(offset)?;
                break;
            }
            let mut data = vec![0; length as usize];
            self.file.read_exact(&mut data).map_err(|e| e.to_string())?;
            visit(decode(&data, &header[16..48])?)?;
            offset += HEADER as u64 + length;
        }
        self.file
            .seek(SeekFrom::End(0))
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn append(&mut self, record: &Record) -> Result<(), String> {
        if self.failed {
            return Err("Storage write failed; reopen the engine".into());
        }
        let (data, digest) = encode(record)?;
        let mut header = [0; HEADER];
        header[..8].copy_from_slice(MAGIC);
        header[8..16].copy_from_slice(&(data.len() as u64).to_le_bytes());
        header[16..48].copy_from_slice(&digest);
        let checksum = Sha256::digest(&header[..48]);
        header[48..].copy_from_slice(&checksum);
        let result = (|| {
            self.file.write_all(&header).map_err(|e| e.to_string())?;
            self.file.write_all(&data).map_err(|e| e.to_string())?;
            self.file.sync_all().map_err(|e| e.to_string())?;
            // Include directory-entry durability in acknowledgements, including
            // creation of a new log. Both stores retain the same commit payload.
            let parent = self
                .path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|e| e.to_string())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn bytes(&self) -> u64 {
        self.file.metadata().map(|m| m.len()).unwrap_or(0)
    }
}
