//! Experimental single-view native engine. The Python/WASM database remains default.
//! Both durable stores replay identical records into the same in-memory indexes.
mod storage;

use foldlitedb_reducer::kernel::{self, Contribution, Position, State};
use fs2::FileExt;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::Path;
use std::time::Instant;

const MAX_POSITION: u64 = (1 << 53) - 1;
const MAX_BATCH: usize = 1024;
const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_VALUE_BYTES: usize = 1024 * 1024;
const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;

fn identity(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > 4096 {
        Err(format!("{label} must contain 1..4096 UTF-8 bytes"))
    } else {
        Ok(())
    }
}
fn text(value: &Value) -> Result<String, String> {
    serde_json::to_string(value).map_err(|e| e.to_string())
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn default_source() -> String {
    "default".into()
}
fn one() -> usize {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct ViewSpec {
    name: String,
    version: String,
    key_field: String,
    value_field: String,
    reducer: String,
    #[serde(default)]
    work: u32,
    #[serde(default = "one")]
    emissions: usize,
}

impl ViewSpec {
    fn validate(&self) -> Result<(), String> {
        for (value, label) in [
            (&self.name, "view name"),
            (&self.version, "view version"),
            (&self.key_field, "key field"),
            (&self.value_field, "value field"),
        ] {
            identity(value, label)?;
        }
        kernel::identity(&self.reducer)?;
        if self.work > 1_000_000 || self.emissions > 1024 {
            return Err("work/emissions exceed bounded native map limits".into());
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InputDocument {
    #[serde(default)]
    id: Option<String>,
    payload: Value,
    #[serde(default)]
    position: Option<u64>,
    #[serde(default = "default_source")]
    source: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredDocument {
    id: String,
    payload: Value,
    position: u64,
    source: String,
    sequence: u64,
}

impl StoredDocument {
    fn public(&self) -> Value {
        json!({"id":self.id,"payload":self.payload,"position":self.position,"source":self.source})
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredState {
    key: Value,
    state: State,
    sequence: u64,
}

impl StoredState {
    fn public(&self) -> Result<Value, String> {
        Ok(
            json!({"key": self.key, "result": kernel::finalize(self.state.clone())?,
                  "state": self.state, "sequence": self.sequence}),
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub(crate) enum Record {
    Header {
        format: u32,
        epoch: String,
        spec: ViewSpec,
    },
    Commit {
        sequence: u64,
        documents: Vec<StoredDocument>,
        states: Vec<StoredState>,
        checkpoints: BTreeMap<String, u64>,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    epoch: String,
    revision: String,
    sequence: u64,
    key: Option<String>,
}

#[derive(Default)]
struct Indexes {
    epoch: Option<String>,
    head: u64,
    documents: BTreeMap<(String, String), StoredDocument>,
    positions: BTreeMap<(String, u64), String>,
    checkpoints: BTreeMap<String, u64>,
    states: BTreeMap<String, StoredState>,
    history: BTreeMap<String, Vec<StoredState>>,
    history_order: BTreeSet<(u64, String)>,
}

impl Indexes {
    fn apply(&mut self, record: Record, expected: &ViewSpec) -> Result<(), String> {
        match record {
            Record::Header {
                format,
                epoch,
                spec,
            } => {
                if self.epoch.is_some() || format != 1 {
                    return Err("Invalid engine header/schema".into());
                }
                if &spec != expected {
                    return Err(
                        "View specification/version changed; rebuild into a new engine path".into(),
                    );
                }
                identity(&epoch, "engine epoch")?;
                self.epoch = Some(epoch);
            }
            Record::Commit {
                sequence,
                documents,
                states,
                checkpoints,
            } => {
                if self.epoch.is_none()
                    || documents.is_empty()
                    || documents.len() > MAX_BATCH
                    || sequence != self.head + documents.len() as u64
                    || sequence > MAX_POSITION
                {
                    return Err("Corrupt transaction sequence".into());
                }
                let mut changed_sources = BTreeMap::new();
                for document in documents {
                    identity(&document.id, "document ID")?;
                    identity(&document.source, "document source")?;
                    if document.sequence != self.head + 1
                        || document.position > MAX_POSITION
                        || self
                            .documents
                            .contains_key(&(document.source.clone(), document.id.clone()))
                        || self
                            .positions
                            .contains_key(&(document.source.clone(), document.position))
                        || self
                            .checkpoints
                            .get(&document.source)
                            .is_some_and(|position| document.position <= *position)
                    {
                        return Err("Corrupt document identity/source order".into());
                    }
                    self.head = document.sequence;
                    self.checkpoints
                        .insert(document.source.clone(), document.position);
                    changed_sources.insert(document.source.clone(), document.position);
                    self.positions.insert(
                        (document.source.clone(), document.position),
                        document.id.clone(),
                    );
                    self.documents
                        .insert((document.source.clone(), document.id.clone()), document);
                }
                if changed_sources != checkpoints {
                    return Err("Corrupt source checkpoint metadata".into());
                }
                let mut seen = BTreeSet::new();
                for state in states {
                    kernel::validate(&state.state)?;
                    if serde_json::to_value(&state.state).map_err(|e| e.to_string())?["reducer"]
                        != expected.reducer
                        || state.sequence != sequence
                    {
                        return Err("Corrupt view state".into());
                    }
                    let key = text(&state.key)?;
                    if !seen.insert(key.clone()) {
                        return Err("Duplicate transaction state".into());
                    }
                    self.history_order.insert((sequence, key.clone()));
                    self.history
                        .entry(key.clone())
                        .or_default()
                        .push(state.clone());
                    self.states.insert(key, state);
                }
            }
        }
        Ok(())
    }
}

/// A single named numeric view with one exclusive process-owned writer.
///
/// Python callbacks, ordered folds, multiple views and history compaction are
/// intentionally outside this prototype's supported contract.
pub struct Engine {
    storage: Box<dyn storage::Backend>,
    _writer_lock: File,
    indexes: Indexes,
    spec: ViewSpec,
    revision: String,
    backend: String,
    pool: rayon::ThreadPool,
    last_ingest: Value,
}

impl Engine {
    pub fn open(path: &str, backend: &str, spec: Value, workers: usize) -> Result<Self, String> {
        if !(1..=64).contains(&workers) {
            return Err("workers must be in 1..64".into());
        }
        if !["sqlite", "log"].contains(&backend) {
            return Err("backend must be sqlite or log".into());
        }
        let spec: ViewSpec = serde_json::from_value(spec).map_err(|e| e.to_string())?;
        spec.validate()?;
        let parent = Path::new(path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        let writer_lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(format!("{path}.lock"))
            .map_err(|e| e.to_string())?;
        writer_lock
            .try_lock_exclusive()
            .map_err(|e| format!("Engine is already locked by another writer: {e}"))?;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .map_err(|e| e.to_string())?;
        let mut storage = storage::open(path, backend)?;
        let mut indexes = Indexes::default();
        storage.scan(&mut |record| indexes.apply(record, &spec))?;
        if indexes.epoch.is_none() {
            let header = Record::Header {
                format: 1,
                epoch: uuid::Uuid::new_v4().simple().to_string(),
                spec: spec.clone(),
            };
            storage.append(&header)?;
            indexes.apply(header, &spec)?;
        }
        let revision = digest(&serde_json::to_vec(&spec).map_err(|e| e.to_string())?);
        Ok(Self {
            storage,
            _writer_lock: writer_lock,
            indexes,
            spec,
            revision,
            backend: backend.into(),
            pool,
            last_ingest: Value::Null,
        })
    }

    pub fn execute(&mut self, mut request: Value) -> Result<Value, String> {
        match request.get("op").and_then(Value::as_str).unwrap_or("") {
            "ingest" => {
                let raw = request
                    .get_mut("documents")
                    .map(Value::take)
                    .ok_or("Missing documents")?;
                let count = raw.as_array().ok_or("documents must be an array")?.len();
                if count > MAX_BATCH {
                    return Err("Batch exceeds max_batch (1024)".into());
                }
                let documents = serde_json::from_value(raw).map_err(|e| e.to_string())?;
                self.ingest(documents)
            }
            "query" => self.query(&request),
            "changes" => self.changes(&request),
            "checkpoint" => {
                let source = request
                    .get("source")
                    .map(|v| v.as_str().ok_or("source must be a string"))
                    .transpose()?
                    .unwrap_or("default");
                identity(source, "document source")?;
                Ok(json!({"position": self.indexes.checkpoints.get(source)}))
            }
            "get" => {
                let identifier = request
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or("Missing document ID")?;
                let source = request
                    .get("source")
                    .map(|v| v.as_str().ok_or("source must be a string"))
                    .transpose()?
                    .unwrap_or("default");
                identity(identifier, "document ID")?;
                identity(source, "document source")?;
                Ok(
                    json!({"document":self.indexes.documents.get(&(source.into(),identifier.into())).map(StoredDocument::public)}),
                )
            }
            "stats" => Ok(
                json!({"sequence":self.indexes.head,"documents":self.indexes.documents.len(),
                "groups":self.indexes.states.len(),"history_rows":self.indexes.history_order.len(),
                "workers":self.pool.current_num_threads(),"backend":self.backend,"durable":true,"schema":1,
                "storage_bytes":self.storage.bytes(),"sqlite_version":rusqlite::version(),"last_ingest":self.last_ingest}),
            ),
            _ => Err("Unknown engine operation".into()),
        }
    }

    fn ingest(&mut self, input: Vec<InputDocument>) -> Result<Value, String> {
        let mut pending = Vec::<StoredDocument>::new();
        let mut by_id = BTreeMap::<(String, String), usize>::new();
        let mut by_position = BTreeMap::<(String, u64), usize>::new();
        let mut checkpoints = BTreeMap::<String, u64>::new();
        let mut resolved = Vec::new();
        let mut byte_count = 0;
        let mut replayed = 0;
        for document in input {
            identity(&document.source, "document source")?;
            if let Some(id) = &document.id {
                identity(id, "document ID")?;
            }
            if document.position.is_some_and(|p| p > MAX_POSITION) {
                return Err("Document position must be a nonnegative safe integer".into());
            }
            let payload = text(&document.payload)?;
            if payload.len() > MAX_VALUE_BYTES {
                return Err("document payload exceeds max_value_bytes".into());
            }
            let existing = if let Some(id) = &document.id {
                let key = (document.source.clone(), id.clone());
                by_id
                    .get(&key)
                    .map(|index| &pending[*index])
                    .or_else(|| self.indexes.documents.get(&key))
            } else if let Some(position) = document.position {
                let key = (document.source.clone(), position);
                by_position
                    .get(&key)
                    .map(|index| &pending[*index])
                    .or_else(|| {
                        self.indexes.positions.get(&key).and_then(|id| {
                            self.indexes
                                .documents
                                .get(&(document.source.clone(), id.clone()))
                        })
                    })
            } else {
                None
            };
            let stored = if let Some(previous) = existing {
                if text(&previous.payload)? != payload
                    || document.position.is_some_and(|p| p != previous.position)
                {
                    return Err("DocumentConflict: immutable document changed".into());
                }
                replayed += 1;
                previous.clone()
            } else {
                let id = document
                    .id
                    .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
                let checkpoint = checkpoints
                    .get(&document.source)
                    .or_else(|| self.indexes.checkpoints.get(&document.source));
                let position = document
                    .position
                    .unwrap_or_else(|| checkpoint.map_or(0, |p| p + 1));
                if position > MAX_POSITION {
                    return Err("Document position must be a nonnegative safe integer".into());
                }
                let position_key = (document.source.clone(), position);
                if by_position.contains_key(&position_key)
                    || self.indexes.positions.contains_key(&position_key)
                {
                    return Err(
                        "DocumentConflict: source position already has a different document".into(),
                    );
                }
                if checkpoint.is_some_and(|p| position <= *p) {
                    return Err("SourceOrderError: new document is behind checkpoint".into());
                }
                let stored = StoredDocument {
                    id,
                    payload: document.payload,
                    position,
                    source: document.source,
                    sequence: self.indexes.head + pending.len() as u64 + 1,
                };
                if stored.sequence > MAX_POSITION {
                    return Err("Document sequence exceeds safe integer bound".into());
                }
                by_id.insert((stored.source.clone(), stored.id.clone()), pending.len());
                by_position.insert(position_key, pending.len());
                checkpoints.insert(stored.source.clone(), position);
                pending.push(stored.clone());
                stored
            };
            byte_count += payload.len() + stored.id.len() + stored.source.len();
            if byte_count > MAX_BATCH_BYTES {
                return Err("Batch exceeds max_batch_bytes".into());
            }
            resolved.push(stored.public());
        }

        let started = Instant::now();
        let mapped: Vec<_> = self.pool.install(|| {
            pending
                .par_iter()
                .map(|document| map_document(&self.spec, document))
                .collect()
        });
        let map_seconds = started.elapsed().as_secs_f64();
        let started = Instant::now();
        let mut groups = BTreeMap::<String, (Value, Vec<Contribution>)>::new();
        let mut workers = BTreeSet::new();
        // Indexed parallel collection retains document order. Each map result
        // retains emission order; grouping therefore never reorders a key.
        for mapped in mapped {
            let (emissions, worker) = mapped?;
            workers.insert(worker);
            for (encoded_key, key, value) in emissions {
                groups
                    .entry(encoded_key)
                    .or_insert_with(|| (key, Vec::new()))
                    .1
                    .push(value);
            }
        }
        let group_seconds = started.elapsed().as_secs_f64();
        let started = Instant::now();
        let sequence = self.indexes.head + pending.len() as u64;
        let mut states = Vec::new();
        for (key, (decoded_key, values)) in groups {
            let mut state = match self.indexes.states.get(&key) {
                Some(row) => row.state.clone(),
                None => kernel::identity(&self.spec.reducer)?,
            };
            let mut batch = Vec::with_capacity(kernel::MAX_VALUES);
            for value in values {
                batch.push(value);
                if batch.len() == kernel::MAX_VALUES {
                    state = kernel::add(state, std::mem::take(&mut batch))?;
                }
            }
            if !batch.is_empty() {
                state = kernel::add(state, batch)?;
            }
            states.push(StoredState {
                key: decoded_key,
                state,
                sequence,
            });
        }
        let reduce_seconds = started.elapsed().as_secs_f64();
        let inserted = pending.len();
        let started = Instant::now();
        if !pending.is_empty() {
            let record = Record::Commit {
                sequence,
                documents: pending,
                states,
                checkpoints,
            };
            self.storage.append(&record)?;
            self.indexes.apply(record, &self.spec)?;
        }
        let commit_seconds = started.elapsed().as_secs_f64();
        self.last_ingest = json!({"map_seconds":map_seconds,"group_seconds":group_seconds,
            "reduce_seconds":reduce_seconds,"commit_seconds":commit_seconds,
            "map_workers_observed":workers.len(),"inserted":inserted,"replayed":replayed});
        Ok(
            json!({"documents":resolved,"inserted":inserted,"replayed":replayed,"sequence":sequence}),
        )
    }

    fn cursor(&self, sequence: u64, key: Option<String>) -> String {
        serde_json::to_string(&Cursor {
            epoch: self.indexes.epoch.clone().unwrap(),
            revision: self.revision.clone(),
            sequence,
            key,
        })
        .unwrap()
    }

    fn parse_cursor(&self, value: &Value, snapshot: bool) -> Result<Cursor, String> {
        let cursor: Cursor = serde_json::from_str(value.as_str().ok_or("Invalid cursor")?)
            .map_err(|_| "Invalid cursor")?;
        if Some(&cursor.epoch) != self.indexes.epoch.as_ref()
            || cursor.revision != self.revision
            || cursor.sequence > self.indexes.head
            || (snapshot && cursor.key.is_some())
        {
            return Err("CursorExpired: cursor belongs to another engine/view/prefix".into());
        }
        Ok(cursor)
    }

    fn limit(request: &Value) -> Result<usize, String> {
        let limit = request
            .get("limit")
            .map(|v| v.as_u64().ok_or("Invalid page limit"))
            .transpose()?
            .unwrap_or(100);
        if !(1..=4096).contains(&limit) {
            return Err("Page limit must be in 1..4096".into());
        }
        Ok(limit as usize)
    }

    fn query(&self, request: &Value) -> Result<Value, String> {
        let limit = Self::limit(request)?;
        let cursor = match request.get("cursor").filter(|v| !v.is_null()) {
            Some(value) => self.parse_cursor(value, true)?,
            None => Cursor {
                epoch: self.indexes.epoch.clone().unwrap(),
                revision: self.revision.clone(),
                sequence: self.indexes.head,
                key: None,
            },
        };
        let cursor_token = self.cursor(cursor.sequence, None);
        let filter = request.get("key").map(text).transpose()?;
        if filter.as_ref().is_some_and(|key| key.len() > 4096) {
            return Err("Key exceeds max_key_bytes".into());
        }
        let mut after = String::new();
        if let Some(token) = request.get("after").filter(|v| !v.is_null()) {
            if request.get("cursor").is_none_or(Value::is_null) {
                return Err("Snapshot pagination requires both cursor and after".into());
            }
            let decoded: (String, String, Option<String>) =
                serde_json::from_str(token.as_str().ok_or("Invalid after token")?)
                    .map_err(|_| "Invalid after token")?;
            if decoded.0 != cursor_token || decoded.2 != filter {
                return Err("Invalid snapshot/filter continuation".into());
            }
            after = decoded.1;
        }
        let mut rows = Vec::new();
        let mut used = 0;
        let mut last = None;
        let mut complete = true;
        let bounds = match &filter {
            Some(key) if key > &after => (Included(key.clone()), Included(key.clone())),
            Some(_) => {
                return Ok(json!({"rows":[],"cursor":cursor_token,"after":null,"complete":true}))
            }
            None => (Excluded(after), Unbounded),
        };
        for (key, history) in self.indexes.history.range(bounds) {
            let boundary = history.partition_point(|row| row.sequence <= cursor.sequence);
            if boundary == 0 {
                continue;
            }
            let row = history[boundary - 1].public()?;
            let bytes = text(&row)?.len();
            if rows.len() >= limit || used + bytes > MAX_PAGE_BYTES {
                if rows.is_empty() {
                    return Err("Result row exceeds max_page_bytes".into());
                }
                complete = false;
                break;
            }
            used += bytes;
            rows.push(row);
            last = Some(key.clone());
        }
        let after = if complete {
            None
        } else {
            Some(serde_json::to_string(&(cursor_token.clone(), last.unwrap(), filter)).unwrap())
        };
        Ok(json!({"rows":rows,"cursor":cursor_token,"after":after,"complete":complete}))
    }

    fn changes(&self, request: &Value) -> Result<Value, String> {
        let limit = Self::limit(request)?;
        let cursor =
            self.parse_cursor(request.get("since").ok_or("Missing since cursor")?, false)?;
        let start = (cursor.sequence, cursor.key.clone().unwrap_or_default());
        let mut rows = Vec::new();
        let mut used = 0;
        let mut last = None;
        let mut complete = true;
        for (sequence, key) in self
            .indexes
            .history_order
            .range((Excluded(start), Unbounded))
        {
            if cursor.key.is_none() && *sequence == cursor.sequence {
                continue;
            }
            let history = &self.indexes.history[key];
            let row = &history[history
                .binary_search_by_key(sequence, |row| row.sequence)
                .unwrap()];
            let row = row.public()?;
            let bytes = text(&row)?.len();
            if rows.len() >= limit || used + bytes > MAX_PAGE_BYTES {
                if rows.is_empty() {
                    return Err("Result row exceeds max_page_bytes".into());
                }
                complete = false;
                break;
            }
            used += bytes;
            rows.push(row);
            last = Some((*sequence, key.clone()));
        }
        let token = if complete {
            self.cursor(self.indexes.head, None)
        } else {
            let (sequence, key) = last.unwrap();
            self.cursor(sequence, Some(key))
        };
        Ok(json!({"rows":rows,"cursor":token,"complete":complete}))
    }
}

type Emission = (String, Value, Contribution);

fn map_document(
    spec: &ViewSpec,
    document: &StoredDocument,
) -> Result<(Vec<Emission>, usize), String> {
    let worker = rayon::current_thread_index().unwrap_or(0);
    if spec.emissions == 0 {
        return Ok((Vec::new(), worker));
    }
    let key = document
        .payload
        .get(&spec.key_field)
        .ok_or("Native map key field is missing")?;
    let raw = document
        .payload
        .get(&spec.value_field)
        .ok_or("Native map value field is missing")?;
    let mut value = if raw.is_null() {
        None
    } else {
        Some(
            raw.as_f64()
                .filter(|value| value.is_finite())
                .ok_or("Native map expects a finite number or null")?,
        )
    };
    let encoded_key = text(key)?;
    if encoded_key.len() > 4096 {
        return Err("Key exceeds max_key_bytes".into());
    }
    if spec.work > 0 {
        let mut x = (document.position as u32) ^ 0x9e3779b9;
        for round in 0..spec.work {
            x = x.wrapping_mul(1664525).wrapping_add(1013904223);
            x = x.rotate_left(13) ^ round;
        }
        std::hint::black_box(x);
        value = value.map(|value| value + f64::from(x & 0xffff));
    }
    let mut emissions = Vec::with_capacity(spec.emissions);
    for ordinal in 0..spec.emissions {
        let tie = digest(
            &serde_json::to_vec(&(document.source.as_str(), document.id.as_str(), ordinal))
                .map_err(|e| e.to_string())?,
        );
        emissions.push((
            encoded_key.clone(),
            key.clone(),
            Contribution {
                value,
                position: Position(document.position, tie),
            },
        ));
    }
    Ok((emissions, worker))
}

#[cfg(test)]
mod tests;
