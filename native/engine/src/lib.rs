//! An immutable native document log with disposable portable Wasm view caches.
mod callback;
mod json;
mod query;
pub mod reducer;
mod runtime;
mod state;
mod storage;
mod wasm;

pub use json::canonical_json;
pub use query::compare_json_keys;
pub use reducer::ReducerInstance;

use callback::{apply_callback_updates, callback_checkpoints, callback_updates};
use json::{
    next_sequence, normalize_canonical_numbers, normalize_json_numbers, optional_string_remove,
    page_limit, parse_after, safe_integer, validate_and_normalize_payload, validate_exact_integers,
    validate_json,
};
use query::{QueryKey, Selector};
use runtime::Runtime;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use state::{
    document_value, get_identity, get_position, version_value, version_value_with_key, Document,
    IdentityKey, PositionKey, RetainedDocument, Stage, State, Version,
};
use std::collections::{btree_map::Entry, BTreeMap, HashMap};
use storage::{Batch, Storage};
use uuid::Uuid;

/// Largest integer `n` such that every integer in `0..=n` is exactly
/// representable as an IEEE-754 double (JavaScript `Number.MAX_SAFE_INTEGER`).
///
/// Source positions and engine sequences never exceed it. Payload integers
/// above it are accepted only when exactly representable as a double, and they
/// are returned with floating-point JSON spelling (for example
/// `9007199254740992.0`). Applications that read numbers back into integer
/// types should keep domain integers within `-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER`
/// or encode them as strings.
pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Byte limit of one ABI v1 request, and separately of one response, including
/// its JSON envelope. Every `map`, `identity`, `add`, `finalize` and
/// `descriptor` call is bounded by it; a batch's single `map` request contains
/// every newly inserted payload, and a per-group `add` request contains the
/// previous state and all of that group's new values.
pub const ABI_BUFFER_BYTES: usize = runtime::BUFFER_SIZE;

/// Maximum documents in one atomic ingestion batch.
pub const MAX_BATCH_DOCUMENTS: usize = 1024;

/// Maximum map emissions (key/value contributions) produced by one batch.
pub const MAX_BATCH_EMISSIONS: usize = 4096;

/// Maximum encoded size of one committed log frame (one batch of documents).
pub const MAX_LOG_FRAME_BYTES: usize = storage::MAX_FRAME_BYTES;

/// Maximum encoded size of one disposable `.cache` checkpoint file.
pub const MAX_CACHE_BYTES: u64 = storage::MAX_CACHE_BYTES;

/// Maximum JSON nesting depth of payloads, keys, states and results.
pub const MAX_JSON_DEPTH: usize = 64;

const MAX_SAFE_SEQUENCE: u64 = MAX_SAFE_INTEGER;
const MAX_PAGE: usize = 1024;
const CURSOR_RECOVERY: &str = "Obtain a fresh snapshot with db.query() and replace the previous results before resuming changes.";
type Result<T> = std::result::Result<T, String>;
type TypedResult<T> = std::result::Result<T, EngineError>;

/// Structured error category matching public Python and browser exception types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    DatabaseError,
    DocumentConflict,
    SourceOrderError,
    CursorExpired,
}

impl ErrorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DatabaseError => "DatabaseError",
            Self::DocumentConflict => "DocumentConflict",
            Self::SourceOrderError => "SourceOrderError",
            Self::CursorExpired => "CursorExpired",
        }
    }
}

/// Structured engine error pairing an [`ErrorKind`] with a human-readable message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineError {
    pub kind: ErrorKind,
    pub message: String,
}

impl EngineError {
    pub fn database(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::DatabaseError,
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::DocumentConflict,
            message: message.into(),
        }
    }

    pub fn source_order(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::SourceOrderError,
            message: message.into(),
        }
    }

    pub fn cursor_expired(message: &str) -> Self {
        Self {
            kind: ErrorKind::CursorExpired,
            message: cursor_error(message),
        }
    }
}

impl From<String> for EngineError {
    fn from(message: String) -> Self {
        Self::database(message)
    }
}

impl From<&str> for EngineError {
    fn from(message: &str) -> Self {
        Self::database(message)
    }
}

struct Prepared {
    token: String,
    stage: Stage,
}

/// Stream-verifies cached prefix batches against `checkpoint.state.documents`
/// without retaining all prefix batches in memory simultaneously.
struct PrefixVerifier {
    checkpoint: storage::Checkpoint,
    doc_cursor: usize,
}

impl PrefixVerifier {
    fn verify_batch(
        &mut self,
        batch_index: u64,
        batch: &Batch,
        end: u64,
        digest: [u8; 32],
    ) -> bool {
        let Some(boundary) = self.checkpoint.state.batches.get(batch_index as usize) else {
            return false;
        };
        let new_doc_count = self.doc_cursor + batch.documents.len();
        if boundary.documents != new_doc_count {
            return false;
        }
        let Some(slice) = self
            .checkpoint
            .state
            .documents
            .get(self.doc_cursor..new_doc_count)
        else {
            return false;
        };
        let mut previous = None;
        for (record, document) in batch.documents.iter().zip(slice) {
            if document.record != *record
                || previous.is_some_and(|sequence| document.sequence != sequence + 1)
            {
                return false;
            }
            previous = Some(document.sequence);
        }
        self.doc_cursor = new_doc_count;
        if batch_index + 1 == self.checkpoint.batches
            && (end != self.checkpoint.offset || digest != self.checkpoint.prefix)
        {
            return false;
        }
        true
    }
}

/// One open document log (or `:memory:` database) with its Wasm view.
///
/// Requests are JSON objects passed to [`Engine::execute`]. Limits for
/// pre-validation are the crate-level constants [`ABI_BUFFER_BYTES`],
/// [`MAX_BATCH_DOCUMENTS`], [`MAX_BATCH_EMISSIONS`], [`MAX_LOG_FRAME_BYTES`],
/// [`MAX_JSON_DEPTH`] and [`MAX_SAFE_INTEGER`]. After an I/O error the writer
/// is poisoned; check [`Engine::writer_poisoned`] and reopen to recover.
pub struct Engine {
    storage: Option<Storage>,
    runtime: Runtime,
    module_sha256: String,
    state: State,
    cache_status: String,
    cache_bytes: u64,
    cache_offset: u64,
    replay_batches: u64,
    replay_documents: u64,
    recovered_tail_bytes: u64,
    checkpoint_on_drop: bool,
    prepared: Option<Prepared>,
}

impl Engine {
    pub fn open(path: &str, module_bytes: &[u8]) -> Result<Self> {
        Self::open_with_options(path, module_bytes, false)
    }

    /// Open or explicitly reset a log while holding its stable ownership locks.
    /// The module is validated before any destructive reset operation.
    pub fn open_with_options(path: &str, module_bytes: &[u8], reset: bool) -> Result<Self> {
        let runtime = Runtime::new(module_bytes)?;
        let module_sha256 = format!("{:x}", Sha256::digest(module_bytes));
        let mut storage = Storage::open_with_options(path, reset, false)?;
        let (candidate, mut cache_status, cache_bytes) = if reset {
            match &storage {
                Storage::Disk(_) => (None, "missing".into(), 0),
                Storage::Memory { .. } => (None, "memory".into(), 0),
            }
        } else {
            storage.load_checkpoint()
        };
        let initial_prefix = storage.initial_prefix();
        let mut state = State::empty();
        let mut cache_offset = 0;
        let mut replay_batches = 0u64;
        let mut replay_documents = 0u64;
        let mut active_cache: Option<PrefixVerifier> = None;

        if let Some(mut cache) = candidate {
            let identity = match &storage {
                Storage::Disk(disk) => disk.database_id.as_str(),
                _ => "",
            };
            if cache.module_sha256 != module_sha256 {
                cache_status = "module-mismatch".into();
            } else if cache.format == 1
                && cache.database_id == identity
                && cache.state.batches.len() as u64 == cache.batches
                && cache.state.validate_and_index().is_ok()
            {
                if cache.batches == 0 {
                    if (cache.offset, cache.prefix) == initial_prefix
                        && cache.state.documents.is_empty()
                    {
                        cache_offset = cache.offset;
                        state = cache.state;
                        cache_status = "loaded".into();
                    } else {
                        cache_status = "invalid".into();
                    }
                } else {
                    active_cache = Some(PrefixVerifier {
                        checkpoint: cache,
                        doc_cursor: 0,
                    });
                }
            } else {
                cache_status = "invalid".into();
            }
        }

        let mut cache_mismatched = false;
        let recovered_tail_bytes = storage.scan_each(|batch_index, batch, end, digest| {
            if let Some(verifier) = active_cache.as_mut() {
                if verifier.verify_batch(batch_index, &batch, end, digest) {
                    if batch_index + 1 == verifier.checkpoint.batches {
                        let done = active_cache.take().unwrap();
                        if done.doc_cursor == done.checkpoint.state.documents.len() {
                            cache_offset = done.checkpoint.offset;
                            state = done.checkpoint.state;
                            cache_status = "loaded".into();
                        } else {
                            cache_status = "invalid".into();
                            cache_mismatched = true;
                        }
                    }
                    return Ok(());
                }
                // Drop the mismatched cache immediately and finish verifying the
                // complete log before replaying from batch 0 in a streaming pass.
                active_cache = None;
                cache_status = "invalid".into();
                cache_mismatched = true;
                return Ok(());
            }
            if cache_mismatched {
                return Ok(());
            }
            let count = batch.documents.len() as u64;
            replay_batch_into(&runtime, &mut state, batch)?;
            replay_batches += 1;
            replay_documents += count;
            Ok(())
        })?;

        if active_cache.take().is_some() {
            // Log had fewer batches than the checkpoint claimed.
            cache_status = "invalid".into();
            cache_mismatched = true;
        }
        if cache_mismatched {
            storage.scan_each_no_recovery(|_, batch, _, _| {
                let count = batch.documents.len() as u64;
                replay_batch_into(&runtime, &mut state, batch)?;
                replay_batches += 1;
                replay_documents += count;
                Ok(())
            })?;
        }

        Ok(Self {
            storage: Some(storage),
            runtime,
            module_sha256,
            state,
            cache_status,
            cache_bytes,
            cache_offset,
            replay_batches,
            replay_documents,
            recovered_tail_bytes,
            checkpoint_on_drop: true,
            prepared: None,
        })
    }

    /// Copy a verified log into a new v2 log, preserving original batch order.
    /// The source remains unchanged; an existing destination is never replaced.
    pub fn migrate_log(source: &str, destination: &str) -> Result<()> {
        Storage::migrate(source, destination)
    }

    /// Payloads and keys have JavaScript/IEEE-754 numeric semantics. Source
    /// positions and cursors retain exact JavaScript-safe integer checks.
    ///
    /// As in the Python host, a payload or query-key integer must be exactly
    /// representable as a double (`9007199254740993` and `u64::MAX` fail;
    /// `2^53 + 2` and `i64::MIN` are accepted). The check runs before mapping
    /// and before any durable write, so a rejected batch commits nothing.
    /// Accepted integers above [`MAX_SAFE_INTEGER`] are returned with floating
    /// JSON spelling. Integer literals beyond the 64-bit range are parsed as
    /// doubles by `serde_json` before the engine sees them, so callers that
    /// build requests from text should validate such literals themselves.
    pub fn execute(&mut self, request: Value) -> Result<Value> {
        self.execute_typed(request).map_err(|error| error.message)
    }

    /// Execute a portable engine request and preserve structured error categories
    /// so host bindings never classify exceptions by English substring matching.
    pub fn execute_typed(&mut self, request: Value) -> TypedResult<Value> {
        if self.storage.is_none() {
            return Err(EngineError::database("Database is closed"));
        }
        if !request.is_object() {
            return Err(EngineError::database("Request must be a JSON object"));
        }
        let op = request
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| EngineError::database("Request requires an op string"))?;
        match op {
            "ingest" => self.ingest(request),
            "prepare_ingest" => self.prepare_ingest(request),
            "commit_prepared" => self.commit_prepared(&request).map_err(Into::into),
            "cancel_prepared" => {
                self.prepared = None;
                Ok(json!({"cancelled":true}))
            }
            "document_batches" => self.document_batches(&request),
            "callback_checkpoints" => Ok(json!(self.state.callback_checkpoints)),
            "set_callback_checkpoints" => {
                self.prepared = None;
                let checkpoints = callback_checkpoints(&request, self.state.sequence)?;
                let updates = callback_updates(&request, self.state.sequence)?;
                self.state.callback_checkpoints.extend(checkpoints);
                apply_callback_updates(&mut self.state.callback_checkpoints, updates);
                Ok(json!({"updated":true}))
            }
            "preview" => self.preview(request).map_err(Into::into),
            "canonical_key" => {
                let key = request
                    .get("key")
                    .ok_or_else(|| EngineError::database("canonical_key requires a key"))?;
                validate_json(key, 0)?;
                validate_exact_integers(key)?;
                Ok(json!(canonical_json(key)?))
            }
            "canonical_keys" => {
                let keys = request
                    .get("keys")
                    .and_then(Value::as_array)
                    .ok_or_else(|| EngineError::database("canonical_keys requires a keys array"))?;
                if keys.len() > MAX_BATCH_EMISSIONS {
                    return Err(EngineError::database(format!(
                        "canonical_keys accepts at most {MAX_BATCH_EMISSIONS} keys per batch"
                    )));
                }
                let mut encoded = Vec::with_capacity(keys.len());
                for key in keys {
                    validate_json(key, 0)?;
                    validate_exact_integers(key)?;
                    encoded.push(canonical_json(key)?);
                }
                Ok(json!(encoded))
            }
            "query" => self.query(&request),
            "changes" => self.changes(&request),
            "documents" => self.documents(&request),
            "get" => self.get(&request).map_err(Into::into),
            "stats" => self.stats().map_err(Into::into),
            "descriptor" => Ok(self.runtime.descriptor.clone()),
            "checkpoint" => {
                self.prepared = None;
                self.checkpoint()?;
                let mut stats = self.stats()?;
                stats["checkpointed"] = json!(true);
                Ok(stats)
            }
            "rebuild" => self.rebuild(&request),
            "close" => {
                self.prepared = None;
                let write_checkpoint = match request.get("checkpoint") {
                    None => true,
                    Some(Value::Bool(flag)) => *flag,
                    Some(_) => {
                        return Err(EngineError::database("close checkpoint must be a boolean"))
                    }
                };
                if !write_checkpoint {
                    self.checkpoint_on_drop = false;
                    self.storage.take();
                    return Ok(json!({"closed":true}));
                }
                // A failed final checkpoint is reported, but the handle and its
                // ownership lock are released either way so the log can be reopened.
                let checkpointed = self.checkpoint();
                self.storage.take();
                checkpointed
                    .map(|()| json!({"closed":true}))
                    .map_err(Into::into)
            }
            _ => Err(EngineError::database("Unknown portable engine operation")),
        }
    }

    /// Whether a persistent log writer is poisoned by an earlier I/O error.
    ///
    /// A failed append or sync leaves the commit outcome uncertain, so the
    /// writer refuses further ingestion and checkpoints. Reads keep serving the
    /// last acknowledged state. Recover by closing the engine (or dropping it)
    /// and calling [`Engine::open`] again: opening verifies the log and discards
    /// an interrupted tail. Then retry with explicit document ids or source
    /// positions, which makes an append that did reach the log an identical
    /// replay rather than a duplicate. This check is cheap and does no I/O.
    /// It is `false` for `:memory:` and for a closed engine.
    pub fn writer_poisoned(&self) -> bool {
        self.storage.as_ref().is_some_and(Storage::poisoned)
    }

    /// Whether the engine is open and its writer can accept ingestion: not
    /// closed, not a legacy read-only log, and not poisoned. See
    /// [`Engine::writer_poisoned`] for uncertain-write recovery.
    pub fn is_writable(&self) -> bool {
        self.storage.as_ref().is_some_and(|s| !s.legacy_read_only()) && !self.writer_poisoned()
    }

    fn cursor(&self, sequence: u64) -> String {
        format!(
            "{}:{}:{sequence}",
            self.state.generation, self.module_sha256
        )
    }

    fn parse_cursor(
        &self,
        value: Option<&Value>,
        current: u64,
        allow_start: bool,
    ) -> TypedResult<u64> {
        match value {
            None | Some(Value::Null) => Ok(if allow_start { 0 } else { current }),
            Some(Value::Number(n)) if allow_start && n.as_u64() == Some(0) => Ok(0),
            Some(Value::String(cursor)) => {
                let prefix = format!("{}:{}:", self.state.generation, self.module_sha256);
                let sequence = cursor
                    .strip_prefix(&prefix)
                    .ok_or_else(|| {
                        EngineError::cursor_expired(
                            "Cursor belongs to a different database, view module, or cache generation",
                        )
                    })?
                    .parse::<u64>()
                    .map_err(|_| EngineError::database("Invalid cursor sequence"))?;
                if sequence > current {
                    return Err(EngineError::cursor_expired(
                        "Cursor is ahead of this database",
                    ));
                }
                Ok(sequence)
            }
            _ => Err(EngineError::database(
                "Cursor must be an opaque cursor string",
            )),
        }
    }

    fn preview(&self, mut request: Value) -> Result<Value> {
        let mut payload = request
            .as_object_mut()
            .and_then(|object| object.remove("payload"))
            .ok_or("preview requires a payload")?;
        validate_and_normalize_payload(&mut payload)?;
        let mut mapped = map_documents(&self.runtime, &[&payload])?;
        let mut emissions = Value::Array(
            mapped
                .remove(0)
                .into_iter()
                .map(|(key, value)| json!({"key": key, "value": value}))
                .collect(),
        );
        normalize_json_numbers(&mut emissions);
        Ok(emissions)
    }

    fn ingest(&mut self, mut request: Value) -> TypedResult<Value> {
        self.prepared = None;
        let documents = match request
            .as_object_mut()
            .and_then(|object| object.remove("documents"))
        {
            Some(Value::Array(documents)) => documents,
            _ => return Err(EngineError::database("ingest requires a documents array")),
        };
        let stage = stage(&self.runtime, &self.state, documents)?;
        let response = json!({"documents":stage.resolved,"inserted":stage.documents.len(),
            "replayed":stage.replayed,"sequence":stage.sequence,"cursor":self.cursor(stage.sequence)});
        if !stage.documents.is_empty() {
            let records: Vec<&RetainedDocument> =
                stage.documents.iter().map(|d| &d.record).collect();
            self.storage.as_mut().unwrap().append_records(&records)?;
            // All fallible validation and Wasm work precedes the durable append.
            self.state.publish(stage);
        }
        Ok(response)
    }

    fn prepare_ingest(&mut self, mut request: Value) -> TypedResult<Value> {
        self.prepared = None;
        let documents = match request
            .as_object_mut()
            .and_then(|object| object.remove("documents"))
        {
            Some(Value::Array(documents)) => documents,
            _ => {
                return Err(EngineError::database(
                    "prepare_ingest requires a documents array",
                ))
            }
        };
        let stage = stage(&self.runtime, &self.state, documents)?;
        let token = Uuid::new_v4().to_string();
        let response = json!({"token":token,"documents":stage.resolved,
            "new_documents":stage.documents.iter().map(document_value).collect::<Vec<_>>(),
            "inserted":stage.documents.len(),"replayed":stage.replayed,
            "sequence":stage.sequence,"cursor":self.cursor(stage.sequence)});
        self.prepared = Some(Prepared { token, stage });
        Ok(response)
    }

    fn commit_prepared(&mut self, request: &Value) -> Result<Value> {
        let prepared = self
            .prepared
            .take()
            .ok_or("No prepared ingestion; prepare the batch again")?;
        if request.get("token").and_then(Value::as_str) != Some(prepared.token.as_str()) {
            return Err("Prepared ingestion token is stale; prepare the batch again".into());
        }
        let stage = prepared.stage;
        let checkpoints = callback_checkpoints(request, stage.sequence)?;
        let updates = callback_updates(request, stage.sequence)?;
        let response = json!({"documents":stage.resolved,"inserted":stage.documents.len(),
            "replayed":stage.replayed,"sequence":stage.sequence,"cursor":self.cursor(stage.sequence)});
        if !stage.documents.is_empty() {
            let records: Vec<&RetainedDocument> =
                stage.documents.iter().map(|doc| &doc.record).collect();
            self.storage.as_mut().unwrap().append_records(&records)?;
            self.state.publish(stage);
        }
        self.state.callback_checkpoints.extend(checkpoints);
        apply_callback_updates(&mut self.state.callback_checkpoints, updates);
        Ok(response)
    }

    fn document_batches(&self, request: &Value) -> TypedResult<Value> {
        let since = request
            .get("since")
            .map(|v| safe_integer(v, "batch coverage"))
            .transpose()?
            .unwrap_or(0);
        let snapshot = self.parse_cursor(request.get("cursor"), self.state.sequence, false)?;
        if snapshot != 0
            && !self
                .state
                .batches
                .iter()
                .any(|batch| batch.sequence == snapshot)
        {
            return Err(EngineError::database(
                "Callback snapshot must identify a committed batch boundary",
            ));
        }
        if since > snapshot
            || (since != 0 && !self.state.batches.iter().any(|b| b.sequence == since))
        {
            return Err(EngineError::database(
                "Callback coverage must identify a committed batch boundary",
            ));
        }
        let index = self
            .state
            .batches
            .partition_point(|batch| batch.sequence <= since);
        let Some(batch) = self
            .state
            .batches
            .get(index)
            .filter(|batch| batch.sequence <= snapshot)
        else {
            return Ok(json!({"documents":[],"sequence":since,"complete":true}));
        };
        let start = index
            .checked_sub(1)
            .map(|i| self.state.batches[i].documents)
            .unwrap_or(0);
        Ok(
            json!({"documents":self.state.documents[start..batch.documents].iter().map(document_value).collect::<Vec<_>>(),
            "sequence":batch.sequence,"complete":self.state.batches.get(index+1).is_none_or(|next| next.sequence > snapshot)}),
        )
    }

    fn checkpoint(&mut self) -> Result<()> {
        self.cache_bytes = self
            .storage
            .as_ref()
            .unwrap()
            .checkpoint(&self.state, &self.module_sha256)?;
        let (disk, offset, _) = self.storage.as_ref().unwrap().metadata();
        self.cache_offset = offset;
        self.cache_status = if disk { "checkpointed" } else { "memory" }.into();
        Ok(())
    }

    fn rebuild(&mut self, request: &Value) -> TypedResult<Value> {
        self.prepared = None;
        let mut rebuilt = State::empty();
        let mut batch_count = 0u64;
        let mut document_count = 0u64;
        let runtime = &self.runtime;
        let recovered = self.storage.as_mut().unwrap().scan_each(|_, batch, _, _| {
            let count = batch.documents.len() as u64;
            replay_batch_into(runtime, &mut rebuilt, batch)?;
            batch_count += 1;
            document_count += count;
            Ok(())
        })?;
        if request.get("checkpoints").is_some() {
            rebuilt.callback_checkpoints = callback_checkpoints(request, rebuilt.sequence)?;
        }
        // A checkpoint failure leaves the current generation untouched.
        let bytes = self
            .storage
            .as_ref()
            .unwrap()
            .checkpoint(&rebuilt, &self.module_sha256)?;
        self.state = rebuilt;
        self.cache_bytes = bytes;
        let (disk, offset, _) = self.storage.as_ref().unwrap().metadata();
        self.cache_offset = offset;
        self.cache_status = if disk { "checkpointed" } else { "memory" }.into();
        self.replay_batches = batch_count;
        self.replay_documents = document_count;
        self.recovered_tail_bytes += recovered;
        let mut stats = self.stats()?;
        stats["rebuilt"] = json!(true);
        Ok(stats)
    }

    fn query(&self, request: &Value) -> TypedResult<Value> {
        let snapshot = self.parse_cursor(request.get("cursor"), self.state.sequence, false)?;
        let cursor = self.cursor(snapshot);
        let limit = page_limit(request)?;
        let selector = Selector::parse(request)?;
        let after = selector.after(request, &cursor)?;
        let mut rows = Vec::new();
        let mut complete = true;
        let mut last_canonical_key = None;
        if let Some(bounds) = selector.bounds(after) {
            for (ordered_key, canonical_key) in self.state.query_keys.range(bounds) {
                // Prefix matches form one contiguous interval after their lower
                // bound. Stop at its first nonmatch rather than scanning other keys.
                if !selector.matches(ordered_key) {
                    break;
                }
                let indices = &self.state.versions[canonical_key];
                let count = indices
                    .partition_point(|&index| self.state.history[index].sequence <= snapshot);
                if count > 0 {
                    if rows.len() == limit {
                        complete = false;
                        break;
                    }
                    rows.push(version_value_with_key(
                        &ordered_key.0,
                        &self.state.history[indices[count - 1]],
                    ));
                    last_canonical_key = Some(canonical_key.as_str());
                }
            }
        }
        let after = if complete {
            None
        } else {
            Some(selector.token(&cursor, last_canonical_key.unwrap()))
        };
        Ok(json!({"rows":rows,"cursor":cursor,"after":after,"complete":complete}))
    }

    fn changes(&self, request: &Value) -> TypedResult<Value> {
        let current = self.state.sequence;
        let since = self.parse_cursor(request.get("since"), current, true)?;
        let limit = page_limit(request)?;
        let start = self.state.history.partition_point(|v| v.sequence <= since);
        let mut rows = self.state.history[start..]
            .iter()
            .take(limit + 1)
            .map(version_value)
            .collect::<Result<Vec<_>>>()?;
        let complete = rows.len() <= limit;
        rows.truncate(limit);
        let sequence = if complete {
            current
        } else {
            rows.last().unwrap()["sequence"].as_u64().unwrap()
        };
        Ok(json!({"rows":rows,"cursor":self.cursor(sequence),"complete":complete}))
    }

    fn documents(&self, request: &Value) -> TypedResult<Value> {
        let snapshot = self.parse_cursor(request.get("cursor"), self.state.sequence, false)?;
        let cursor = self.cursor(snapshot);
        let limit = page_limit(request)?;
        let after = parse_after(request, &cursor)?;
        let after_sequence = after
            .as_ref()
            .map(|v| safe_integer(v, "document after sequence"))
            .transpose()?
            .unwrap_or(0);
        let start = self
            .state
            .documents
            .partition_point(|doc| doc.sequence <= after_sequence);
        let mut documents = self.state.documents[start..]
            .iter()
            .take_while(|doc| doc.sequence <= snapshot)
            .take(limit + 1)
            .map(document_value)
            .collect::<Vec<_>>();
        let complete = documents.len() <= limit;
        documents.truncate(limit);
        let after = if complete {
            None
        } else {
            Some(
                serde_json::to_string(&json!([cursor, documents.last().unwrap()["sequence"]]))
                    .unwrap(),
            )
        };
        Ok(json!({"documents":documents,"cursor":cursor,"after":after,"complete":complete}))
    }

    fn get(&self, request: &Value) -> Result<Value> {
        let object = request.as_object().ok_or("Request must be a JSON object")?;
        let source = match object.get("source") {
            None | Some(Value::Null) => "default",
            Some(Value::String(s)) if !s.is_empty() && s.len() <= 4096 => s.as_str(),
            _ => return Err("source must be a nonempty string of at most 4096 bytes".into()),
        };
        let id = match object.get("id") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) if !s.is_empty() && s.len() <= 4096 => Some(s.as_str()),
            _ => return Err("id must be a nonempty string of at most 4096 bytes".into()),
        };
        let position = match object.get("position") {
            None | Some(Value::Null) => None,
            Some(value) => Some(safe_integer(value, "position")?),
        };
        match (id, position) {
            (Some(id), None) => Ok(get_identity(&self.state.identities, source, id)
                .map(|index| document_value(&self.state.documents[index]))
                .unwrap_or(Value::Null)),
            (None, Some(position)) => Ok(get_position(&self.state.positions, source, position)
                .map(|index| document_value(&self.state.documents[index]))
                .unwrap_or(Value::Null)),
            (None, None) => Err("get requires either id or position".into()),
            (Some(_), Some(_)) => Err("get accepts either id or position, not both".into()),
        }
    }

    fn stats(&self) -> Result<Value> {
        let (disk, bytes, batches) = self
            .storage
            .as_ref()
            .ok_or("Database is closed")?
            .metadata();
        Ok(
            json!({"documents":self.state.documents.len(),"sequence":self.state.sequence,
            "cursor":self.cursor(self.state.sequence),"module_sha256":self.module_sha256,
            "descriptor":self.runtime.descriptor,"checkpoints":self.state.checkpoints,
            "generation":self.state.generation,"groups":self.state.versions.len(),
            "storage":if disk {"append-only-log"} else {"memory"},
            "cache":{"status":self.cache_status,"bytes":self.cache_bytes,"log_offset":self.cache_offset},
            "replay":{"batches":self.replay_batches,"documents":self.replay_documents},
            "replay_batches":self.replay_batches,"replay_documents":self.replay_documents,
            "log":{"bytes":bytes,"batches":batches,"recovered_tail_bytes":self.recovered_tail_bytes,
                "writer_poisoned":self.writer_poisoned(),"read_only":self.storage.as_ref().unwrap().legacy_read_only(),
                "format":if self.storage.as_ref().unwrap().legacy_read_only() {1} else {2}}}),
        )
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if self.checkpoint_on_drop {
            if let Some(storage) = &self.storage {
                let _ = storage.checkpoint(&self.state, &self.module_sha256);
            }
        }
    }
}

fn replay_batch_into(runtime: &Runtime, state: &mut State, batch: Batch) -> Result<()> {
    let staged = stage_retained(runtime, state, batch.documents).map_err(|e| e.message)?;
    state.publish(staged);
    Ok(())
}

fn stage(runtime: &Runtime, state: &State, documents: Vec<Value>) -> TypedResult<Stage> {
    if documents.len() > MAX_BATCH_DOCUMENTS {
        return Err(EngineError::database(format!(
            "An ingest batch may contain at most {MAX_BATCH_DOCUMENTS} documents \
             (MAX_BATCH_DOCUMENTS); this batch has {}. Split it into smaller batches.",
            documents.len()
        )));
    }
    let mut staged = Stage {
        documents: Vec::with_capacity(documents.len()),
        versions: Vec::new(),
        sequence: state.sequence,
        resolved: Vec::with_capacity(documents.len()),
        replayed: 0,
        query_keys: BTreeMap::new(),
    };
    // Only this batch's identities/checkpoints are staged; previously retained
    // documents, history, and indexes are never copied on the ingest path.
    let mut identities: HashMap<IdentityKey, usize> = HashMap::with_capacity(documents.len());
    let mut positions: HashMap<PositionKey, usize> = HashMap::with_capacity(documents.len());
    let mut checkpoints: HashMap<String, u64> = HashMap::new();
    for document in documents {
        let Value::Object(mut object) = document else {
            return Err(EngineError::database(
                "Each document must be an object with a payload",
            ));
        };
        let mut payload = object
            .remove("payload")
            .ok_or_else(|| EngineError::database("Each document requires a payload"))?;
        validate_and_normalize_payload(&mut payload)?;
        let requested_id = optional_string_remove(&mut object, "id")?;
        let source =
            optional_string_remove(&mut object, "source")?.unwrap_or_else(|| "default".into());
        let requested_position = object
            .get("position")
            .map(|v| safe_integer(v, "position"))
            .transpose()?;
        let existing_id = requested_id.as_deref().and_then(|id| {
            get_identity(&identities, &source, id)
                .map(|i| &staged.documents[i])
                .or_else(|| {
                    get_identity(&state.identities, &source, id).map(|i| &state.documents[i])
                })
        });
        let last = checkpoints
            .get(&source)
            .or_else(|| state.checkpoints.get(&source))
            .copied();
        let position = match requested_position {
            Some(p) => p,
            None => match existing_id {
                Some(doc) => doc.record.position,
                None => last.map(next_sequence).transpose()?.unwrap_or(0),
            },
        };
        let existing_position = get_position(&positions, &source, position)
            .map(|i| &staged.documents[i])
            .or_else(|| {
                get_position(&state.positions, &source, position).map(|i| &state.documents[i])
            });
        if let Some(existing) = existing_id.or(existing_position) {
            let record = &existing.record;
            if record.source != source
                || record.position != position
                || requested_id.as_ref().is_some_and(|id| record.id != *id)
                || canonical_json(&record.payload)? != canonical_json(&payload)?
            {
                return Err(EngineError::conflict(
                    "Document replay conflicts with an immutable id or source position",
                ));
            }
            staged.replayed += 1;
            staged.resolved.push(document_value(existing));
            continue;
        }
        if last.is_some_and(|last| position <= last) {
            return Err(EngineError::source_order(
                "New source positions must advance the retained checkpoint",
            ));
        }
        let id = requested_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        staged.sequence = next_sequence(staged.sequence)?;
        let document = Document {
            record: RetainedDocument {
                id: id.clone(),
                source: source.clone(),
                position,
                payload,
            },
            sequence: staged.sequence,
        };
        let doc_index = staged.documents.len();
        identities.insert(
            IdentityKey {
                source: source.clone(),
                id,
            },
            doc_index,
        );
        positions.insert(
            PositionKey {
                source: source.clone(),
                position,
            },
            doc_index,
        );
        checkpoints.insert(source, position);
        staged.resolved.push(document_value(&document));
        staged.documents.push(document);
    }
    stage_views(runtime, state, &mut staged)?;
    Ok(staged)
}

fn stage_retained(
    runtime: &Runtime,
    state: &State,
    records: Vec<RetainedDocument>,
) -> TypedResult<Stage> {
    if records.len() > MAX_BATCH_DOCUMENTS {
        return Err(EngineError::database(format!(
            "An ingest batch may contain at most {MAX_BATCH_DOCUMENTS} documents \
             (MAX_BATCH_DOCUMENTS); this batch has {}. Split it into smaller batches.",
            records.len()
        )));
    }
    let mut staged = Stage {
        documents: Vec::with_capacity(records.len()),
        versions: Vec::new(),
        sequence: state.sequence,
        resolved: Vec::new(),
        replayed: 0,
        query_keys: BTreeMap::new(),
    };
    let mut identities: HashMap<IdentityKey, usize> = HashMap::with_capacity(records.len());
    let mut positions: HashMap<PositionKey, usize> = HashMap::with_capacity(records.len());
    let mut checkpoints: HashMap<String, u64> = HashMap::new();
    for mut record in records {
        if record.id.is_empty()
            || record.id.len() > 4096
            || record.source.is_empty()
            || record.source.len() > 4096
            || record.position > MAX_SAFE_SEQUENCE
        {
            return Err(EngineError::database(
                "Invalid retained document identity or source position",
            ));
        }
        validate_and_normalize_payload(&mut record.payload)?;
        let existing_id = get_identity(&identities, &record.source, &record.id)
            .map(|i| &staged.documents[i])
            .or_else(|| {
                get_identity(&state.identities, &record.source, &record.id)
                    .map(|i| &state.documents[i])
            });
        let existing_position = get_position(&positions, &record.source, record.position)
            .map(|i| &staged.documents[i])
            .or_else(|| {
                get_position(&state.positions, &record.source, record.position)
                    .map(|i| &state.documents[i])
            });
        if let Some(existing) = existing_id.or(existing_position) {
            let existing_record = &existing.record;
            if existing_record.source != record.source
                || existing_record.position != record.position
                || existing_record.id != record.id
                || canonical_json(&existing_record.payload)? != canonical_json(&record.payload)?
            {
                return Err(EngineError::conflict(
                    "Document replay conflicts with an immutable id or source position",
                ));
            }
            return Err(EngineError::database(
                "Native log contains duplicate immutable documents",
            ));
        }
        let last = checkpoints
            .get(&record.source)
            .or_else(|| state.checkpoints.get(&record.source))
            .copied();
        if last.is_some_and(|last| record.position <= last) {
            return Err(EngineError::source_order(
                "New source positions must advance the retained checkpoint",
            ));
        }
        staged.sequence = next_sequence(staged.sequence)?;
        let doc_index = staged.documents.len();
        identities.insert(
            IdentityKey {
                source: record.source.clone(),
                id: record.id.clone(),
            },
            doc_index,
        );
        positions.insert(
            PositionKey {
                source: record.source.clone(),
                position: record.position,
            },
            doc_index,
        );
        checkpoints.insert(record.source.clone(), record.position);
        staged.documents.push(Document {
            record,
            sequence: staged.sequence,
        });
    }
    stage_views(runtime, state, &mut staged)?;
    Ok(staged)
}

fn stage_views(runtime: &Runtime, state: &State, staged: &mut Stage) -> TypedResult<()> {
    let payloads: Vec<&Value> = staged
        .documents
        .iter()
        .filter(|doc| !doc.record.source.starts_with('_'))
        .map(|doc| &doc.record.payload)
        .collect();
    if payloads.is_empty() {
        return Ok(());
    }
    let mapped = map_documents(runtime, &payloads)?;
    let mut grouped: BTreeMap<String, (Value, Vec<Value>)> = BTreeMap::new();
    for emissions in mapped {
        for (key, value) in emissions {
            let canonical = canonical_json(&key)?;
            match grouped.entry(canonical) {
                Entry::Occupied(mut entry) => entry.get_mut().1.push(value),
                Entry::Vacant(entry) => {
                    entry.insert((key, vec![value]));
                }
            }
        }
    }
    let mut keys = Vec::with_capacity(grouped.len());
    let mut inputs = Vec::with_capacity(grouped.len());
    for (canonical_key, (mut key_value, values)) in grouped {
        let previous = state
            .versions
            .get(&canonical_key)
            .and_then(|indices| indices.last())
            .map(|&i| &state.history[i].state);
        let mut input = json!({"values":values});
        if let Some(previous) = previous {
            input["state"] = previous.clone();
        } else {
            normalize_canonical_numbers(&mut key_value);
            staged
                .query_keys
                .insert(QueryKey(key_value), canonical_key.clone());
        }
        keys.push(canonical_key);
        inputs.push(input);
    }
    for (key, (mut new_state, mut result)) in keys.into_iter().zip(runtime.reduce_groups(&inputs)?)
    {
        normalize_json_numbers(&mut new_state);
        normalize_json_numbers(&mut result);
        staged.sequence = next_sequence(staged.sequence)?;
        staged.versions.push(Version {
            key,
            state: new_state,
            result,
            sequence: staged.sequence,
        });
    }
    Ok(())
}

fn cursor_error(message: &str) -> String {
    format!("{message}. {CURSOR_RECOVERY}")
}

// Keep struct field order strictly alphabetical so serialized JSON matches
// `serde_json::Map` key order byte-for-byte.
#[derive(Serialize)]
struct MapRequest<'a> {
    documents: &'a [&'a Value],
    op: &'static str,
}

// Mapping diagnostics and ingestion deliberately share the complete contract.
// Reducers run only after this helper returns during normal ingestion.
fn map_documents(runtime: &Runtime, payloads: &[&Value]) -> Result<Vec<Vec<(Value, Value)>>> {
    let count = payloads.len();
    let mapped = runtime.call_serialized(
        "map",
        &MapRequest {
            documents: payloads,
            op: "map",
        },
    )?;
    let Value::Array(mapped) = mapped else {
        return Err("map must return one contribution array per document".into());
    };
    if mapped.len() != count {
        return Err("map returned the wrong number of document results".into());
    }
    let mut output = Vec::with_capacity(count);
    let mut contributions = 0;
    for emissions in mapped {
        let Value::Array(emissions) = emissions else {
            return Err("Each map result must be an array".into());
        };
        let mut normalized = Vec::with_capacity(emissions.len());
        for emission in emissions {
            let Value::Object(mut obj) = emission else {
                return Err("A map contribution requires key".into());
            };
            let key = obj.remove("key").ok_or("A map contribution requires key")?;
            let value = obj
                .remove("value")
                .ok_or("A map contribution requires value")?;
            contributions += 1;
            if contributions > MAX_BATCH_EMISSIONS {
                return Err(format!(
                    "A batch may emit at most {MAX_BATCH_EMISSIONS} contributions \
                     (MAX_BATCH_EMISSIONS). Split it into smaller batches."
                ));
            }
            validate_json(&key, 0)?;
            validate_json(&value, 0)?;
            // Preserve the guest's contribution values, including signed zero.
            // Re-encoding through canonical JSON would change reducer inputs.
            normalized.push((key, value));
        }
        output.push(normalized);
    }
    Ok(output)
}
