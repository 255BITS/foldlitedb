//! An immutable native document log with disposable portable Wasm view caches.
mod runtime;
mod storage;
pub mod reducer;

pub use reducer::ReducerInstance;
use runtime::Runtime;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use storage::{Batch, Storage};
use uuid::Uuid;

const MAX_SAFE_SEQUENCE: u64 = 9_007_199_254_740_991;
const MAX_BATCH: usize = 1024;
const MAX_PAGE: usize = 1024;
const MAX_CONTRIBUTIONS: usize = 4096;
const CURSOR_RECOVERY: &str = "Obtain a fresh snapshot with db.query() and replace the previous results before resuming changes.";
type Result<T> = std::result::Result<T, String>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedDocument {
    id: String,
    source: String,
    position: u64,
    payload: Value,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    #[serde(flatten)]
    record: RetainedDocument,
    sequence: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Version {
    key: String,
    state: Value,
    result: Value,
    sequence: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct State {
    generation: String,
    sequence: u64,
    documents: Vec<Document>,
    history: Vec<Version>,
    #[serde(skip)]
    identities: BTreeMap<(String, String), usize>,
    #[serde(skip)]
    positions: BTreeMap<(String, u64), usize>,
    #[serde(skip)]
    checkpoints: BTreeMap<String, u64>,
    #[serde(skip)]
    versions: BTreeMap<String, Vec<usize>>,
}

impl State {
    fn empty() -> Self {
        Self {
            generation: Uuid::new_v4().to_string(),
            sequence: 0,
            documents: Vec::new(),
            history: Vec::new(),
            identities: BTreeMap::new(),
            positions: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            versions: BTreeMap::new(),
        }
    }

    // Checkpoint data is never authoritative. Validate every structural invariant
    // before rebuilding its indexes; the verified log separately proves provenance.
    fn validate_and_index(&mut self) -> Result<()> {
        if Uuid::parse_str(&self.generation).is_err()
            || self.sequence > MAX_SAFE_SEQUENCE
            || self.sequence != (self.documents.len() + self.history.len()) as u64
        {
            return Err("Invalid cached generation or sequence".into());
        }
        let mut document_sequence = 0;
        for (index, document) in self.documents.iter().enumerate() {
            validate_record(&document.record)?;
            if document.sequence <= document_sequence || document.sequence > self.sequence {
                return Err("Invalid cached document sequence".into());
            }
            document_sequence = document.sequence;
            let record = &document.record;
            if self
                .identities
                .insert((record.source.clone(), record.id.clone()), index)
                .is_some()
                || self
                    .positions
                    .insert((record.source.clone(), record.position), index)
                    .is_some()
                || self
                    .checkpoints
                    .get(&record.source)
                    .is_some_and(|p| record.position <= *p)
            {
                return Err("Invalid cached immutable document identity".into());
            }
            self.checkpoints
                .insert(record.source.clone(), record.position);
        }
        let mut version_sequence = 0;
        for (index, version) in self.history.iter().enumerate() {
            let key: Value =
                serde_json::from_str(&version.key).map_err(|_| "Invalid cached key")?;
            validate_json(&key, 0)?;
            validate_json(&version.state, 0)?;
            validate_json(&version.result, 0)?;
            if version.sequence <= version_sequence
                || version.sequence > self.sequence
                || canonical_json(&key)? != version.key
            {
                return Err("Invalid cached version sequence or key".into());
            }
            version_sequence = version.sequence;
            self.versions
                .entry(version.key.clone())
                .or_default()
                .push(index);
        }
        // The two sorted streams must partition the complete sequence domain.
        let mut documents = self.documents.iter().peekable();
        let mut versions = self.history.iter().peekable();
        for expected in 1..=self.sequence {
            if documents.peek().is_some_and(|doc| doc.sequence == expected) {
                documents.next();
            } else if versions.peek().is_some_and(|row| row.sequence == expected) {
                versions.next();
            } else {
                return Err("Cached history has a duplicate or missing sequence".into());
            }
        }
        if documents.next().is_some() || versions.next().is_some() {
            return Err("Cached history has duplicate sequences".into());
        }
        Ok(())
    }

    fn publish(&mut self, stage: Stage) {
        for document in stage.documents {
            let record = &document.record;
            let index = self.documents.len();
            self.identities
                .insert((record.source.clone(), record.id.clone()), index);
            self.positions
                .insert((record.source.clone(), record.position), index);
            self.checkpoints
                .insert(record.source.clone(), record.position);
            self.documents.push(document);
        }
        for version in stage.versions {
            self.versions
                .entry(version.key.clone())
                .or_default()
                .push(self.history.len());
            self.history.push(version);
        }
        self.sequence = stage.sequence;
    }
}

struct Stage {
    documents: Vec<Document>,
    versions: Vec<Version>,
    sequence: u64,
    resolved: Vec<Value>,
    replayed: usize,
}

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
}

impl Engine {
    pub fn open(path: &str, module_bytes: &[u8]) -> Result<Self> {
        let runtime = Runtime::new(module_bytes)?;
        let module_sha256 = format!("{:x}", Sha256::digest(module_bytes));
        let mut storage = Storage::open(path)?;
        let (candidate, mut cache_status, cache_bytes) = storage.load_checkpoint();
        let initial_prefix = storage.initial_prefix();
        let scan = storage.scan()?;
        let mut state = State::empty();
        let mut skip = 0;
        let mut cache_offset = 0;
        if let Some(mut cache) = candidate {
            let identity = match &storage {
                Storage::Disk(disk) => disk.database_id.as_str(),
                _ => "",
            };
            let boundary = if cache.batches == 0 {
                Some(initial_prefix)
            } else {
                scan.batches
                    .get(cache.batches.saturating_sub(1) as usize)
                    .map(|batch| (batch.end, batch.digest))
            };
            if cache.module_sha256 != module_sha256 {
                cache_status = "module-mismatch".into();
            } else if cache.format == 1
                && cache.database_id == identity
                && boundary == Some((cache.offset, cache.prefix))
                && cache.state.validate_and_index().is_ok()
                && checkpoint_matches_log(
                    &cache.state,
                    &scan.batches[..cache.batches.min(scan.batches.len() as u64) as usize],
                )
            {
                skip = cache.batches as usize;
                cache_offset = cache.offset;
                state = cache.state;
                cache_status = "loaded".into();
            } else {
                cache_status = "invalid".into();
            }
        }
        let mut engine = Self {
            storage: Some(storage),
            runtime,
            module_sha256,
            state,
            cache_status,
            cache_bytes,
            cache_offset,
            replay_batches: 0,
            replay_documents: 0,
            recovered_tail_bytes: scan.recovered,
            checkpoint_on_drop: false,
        };
        for verified in scan.batches.into_iter().skip(skip) {
            let count = verified.batch.documents.len() as u64;
            engine.replay_batch(verified.batch)?;
            engine.replay_batches += 1;
            engine.replay_documents += count;
        }
        engine.checkpoint_on_drop = true;
        Ok(engine)
    }

    /// Payloads and keys have JavaScript/IEEE-754 numeric semantics. Source
    /// positions and cursors retain exact JavaScript-safe integer checks.
    pub fn execute(&mut self, request: Value) -> Result<Value> {
        if self.storage.is_none() {
            return Err("Database is closed".into());
        }
        if !request.is_object() {
            return Err("Request must be a JSON object".into());
        }
        match request
            .get("op")
            .and_then(Value::as_str)
            .ok_or("Request requires an op string")?
        {
            "ingest" => self.ingest(&request),
            "preview" => self.preview(&request),
            "query" => self.query(&request),
            "changes" => self.changes(&request),
            "documents" => self.documents(&request),
            "get" => self.get(&request),
            "stats" => self.stats(),
            "descriptor" => Ok(self.runtime.descriptor.clone()),
            "checkpoint" => {
                self.checkpoint()?;
                let mut stats = self.stats()?;
                stats["checkpointed"] = json!(true);
                Ok(stats)
            }
            "rebuild" => self.rebuild(),
            "close" => {
                self.checkpoint()?;
                self.storage.take();
                Ok(json!({"closed":true}))
            }
            _ => Err("Unknown portable engine operation".into()),
        }
    }

    fn cursor(&self, sequence: u64) -> String {
        format!(
            "{}:{}:{sequence}",
            self.state.generation, self.module_sha256
        )
    }

    fn parse_cursor(&self, value: Option<&Value>, current: u64, allow_start: bool) -> Result<u64> {
        match value {
            None | Some(Value::Null) => Ok(if allow_start { 0 } else { current }),
            Some(Value::Number(n)) if allow_start && n.as_u64() == Some(0) => Ok(0),
            Some(Value::String(cursor)) => {
                let prefix = format!("{}:{}:", self.state.generation, self.module_sha256);
                let sequence = cursor
                    .strip_prefix(&prefix)
                    .ok_or_else(|| {
                        cursor_error(
                        "Cursor belongs to a different database, view module, or cache generation",
                    )
                    })?
                    .parse::<u64>()
                    .map_err(|_| cursor_error("Invalid cursor sequence"))?;
                if sequence > current {
                    return Err(cursor_error("Cursor is ahead of this database"));
                }
                Ok(sequence)
            }
            _ => Err(cursor_error("Cursor must be an opaque cursor string")),
        }
    }

    fn preview(&self, request: &Value) -> Result<Value> {
        let payload = request.get("payload").ok_or("preview requires a payload")?;
        let (_, payload) = canonical_payload(payload)?;
        let mut mapped = map_documents(&self.runtime, vec![payload])?;
        let mut emissions = Value::Array(mapped.remove(0));
        normalize_json_numbers(&mut emissions);
        Ok(emissions)
    }

    fn ingest(&mut self, request: &Value) -> Result<Value> {
        let documents = request
            .get("documents")
            .and_then(Value::as_array)
            .ok_or("ingest requires a documents array")?;
        let stage = stage(&mut self.runtime, &self.state, documents)?;
        let response = json!({"documents":stage.resolved,"inserted":stage.documents.len(),
            "replayed":stage.replayed,"sequence":stage.sequence,"cursor":self.cursor(stage.sequence)});
        if !stage.documents.is_empty() {
            let batch = Batch {
                documents: stage.documents.iter().map(|d| d.record.clone()).collect(),
            };
            self.storage.as_mut().unwrap().append(batch)?;
            // All fallible validation and Wasm work precedes the durable append.
            self.state.publish(stage);
        }
        Ok(response)
    }

    fn replay_batch(&mut self, batch: Batch) -> Result<()> {
        let documents = batch
            .documents
            .iter()
            .map(|d| serde_json::to_value(d).expect("serializable record"))
            .collect::<Vec<_>>();
        let stage = stage(&mut self.runtime, &self.state, &documents)?;
        if stage.replayed != 0 || stage.documents.len() != documents.len() {
            return Err("Native log contains duplicate immutable documents".into());
        }
        self.state.publish(stage);
        Ok(())
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

    fn rebuild(&mut self) -> Result<Value> {
        let scan = self.storage.as_mut().unwrap().scan()?;
        let mut rebuilt = State::empty();
        let mut document_count = 0;
        for batch in &scan.batches {
            let documents: Vec<Value> = batch
                .batch
                .documents
                .iter()
                .map(|d| serde_json::to_value(d).expect("serializable record"))
                .collect();
            let staged = stage(&mut self.runtime, &rebuilt, &documents)?;
            if staged.replayed != 0 {
                return Err("Native log contains duplicate immutable documents".into());
            }
            document_count += documents.len() as u64;
            rebuilt.publish(staged);
        }
        // A checkpoint failure leaves the current generation untouched.
        let bytes = self
            .storage
            .as_ref()
            .unwrap()
            .checkpoint(&rebuilt, &self.module_sha256)?;
        self.state = rebuilt;
        self.cache_bytes = bytes;
        self.cache_offset = self.storage.as_ref().unwrap().metadata().1;
        self.cache_status = if self.storage.as_ref().unwrap().metadata().0 {
            "checkpointed"
        } else {
            "memory"
        }
        .into();
        self.replay_batches = scan.batches.len() as u64;
        self.replay_documents = document_count;
        self.recovered_tail_bytes += scan.recovered;
        let mut stats = self.stats()?;
        stats["rebuilt"] = json!(true);
        Ok(stats)
    }

    fn query(&self, request: &Value) -> Result<Value> {
        let snapshot = self.parse_cursor(request.get("cursor"), self.state.sequence, false)?;
        let cursor = self.cursor(snapshot);
        let limit = page_limit(request)?;
        let key = request
            .get("key")
            .map(|key| {
                validate_json(key, 0)?;
                canonical_json(key)
            })
            .transpose()?;
        let after = parse_after(request, &cursor)?;
        let after_key = after
            .as_ref()
            .map(|v| v.as_str().ok_or("Invalid query after token"))
            .transpose()?;
        let mut rows = Vec::new();
        use std::ops::Bound::{Excluded, Unbounded};
        let selected: Box<dyn Iterator<Item = (&String, &Vec<usize>)> + '_> = match key.as_ref() {
            Some(key) => Box::new(
                self.state
                    .versions
                    .get_key_value(key)
                    .into_iter()
                    .filter(|(key, _)| after_key.is_none_or(|after| key.as_str() > after)),
            ),
            None => match after_key {
                Some(after) => Box::new(
                    self.state
                        .versions
                        .range::<str, _>((Excluded(after), Unbounded)),
                ),
                None => Box::new(self.state.versions.iter()),
            },
        };
        for (_, indices) in selected {
            let count =
                indices.partition_point(|&index| self.state.history[index].sequence <= snapshot);
            if count > 0 {
                rows.push(version_value(&self.state.history[indices[count - 1]])?);
                if rows.len() > limit {
                    break;
                }
            }
        }
        let complete = rows.len() <= limit;
        rows.truncate(limit);
        let after = if complete {
            None
        } else {
            Some(
                serde_json::to_string(&json!([
                    cursor,
                    canonical_json(&rows.last().unwrap()["key"])?
                ]))
                .unwrap(),
            )
        };
        Ok(json!({"rows":rows,"cursor":cursor,"after":after,"complete":complete}))
    }

    fn changes(&self, request: &Value) -> Result<Value> {
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

    fn documents(&self, request: &Value) -> Result<Value> {
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
        let source = request
            .get("source")
            .and_then(Value::as_str)
            .unwrap_or("default");
        let id_str = match request.get("id") {
            Some(Value::String(s)) => Some(s.clone()),
            Some(Value::Number(n)) => Some(n.to_string()),
            _ => None,
        };
        if let Some(id) = id_str {
            if let Some(&index) = self.state.identities.get(&(source.to_string(), id)) {
                return Ok(document_value(&self.state.documents[index]));
            }
        }
        if let Some(position) = request.get("position").and_then(Value::as_u64) {
            if let Some(&index) = self.state.positions.get(&(source.to_string(), position)) {
                return Ok(document_value(&self.state.documents[index]));
            }
        }
        Ok(Value::Null)
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
            "log":{"bytes":bytes,"batches":batches,"recovered_tail_bytes":self.recovered_tail_bytes}}),
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

fn stage(runtime: &mut Runtime, state: &State, documents: &[Value]) -> Result<Stage> {
    if documents.len() > MAX_BATCH {
        return Err(
            "An ingest batch may contain at most 1024 documents. Use smaller batches or iterate \
             db.ingest_many(..., batch_size=...) with a smaller batch_size."
                .into(),
        );
    }
    let mut staged = Stage {
        documents: Vec::new(),
        versions: Vec::new(),
        sequence: state.sequence,
        resolved: Vec::new(),
        replayed: 0,
    };
    // Only this batch's identities/checkpoints are staged; previously retained
    // documents, history, and indexes are never copied on the ingest path.
    let mut identities = BTreeMap::new();
    let mut positions = BTreeMap::new();
    let mut checkpoints = BTreeMap::new();
    for document in documents {
        let object = document
            .as_object()
            .ok_or("Each document must be an object with a payload")?;
        let payload = object
            .get("payload")
            .ok_or("Each document requires a payload")?;
        let (payload_text, payload) = canonical_payload(payload)?;
        let requested_id = optional_string(object, "id")?;
        let source = optional_string(object, "source")?.unwrap_or_else(|| "default".into());
        let requested_position = object
            .get("position")
            .map(|v| safe_integer(v, "position"))
            .transpose()?;
        let get_identity = |id: &str| -> Option<&Document> {
            identities
                .get(&(source.clone(), id.to_owned()))
                .map(|&i: &usize| &staged.documents[i])
                .or_else(|| {
                    state
                        .identities
                        .get(&(source.clone(), id.to_owned()))
                        .map(|&i| &state.documents[i])
                })
        };
        let existing_id = requested_id.as_deref().and_then(get_identity);
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
        let existing_position = positions
            .get(&(source.clone(), position))
            .map(|&i: &usize| &staged.documents[i])
            .or_else(|| {
                state
                    .positions
                    .get(&(source.clone(), position))
                    .map(|&i| &state.documents[i])
            });
        if let Some(existing) = existing_id.or(existing_position) {
            let record = &existing.record;
            if record.source != source
                || record.position != position
                || canonical_json(&record.payload)? != payload_text
                || requested_id.as_ref().is_some_and(|id| record.id != *id)
            {
                return Err(
                    "Document replay conflicts with an immutable id or source position".into(),
                );
            }
            staged.replayed += 1;
            staged.resolved.push(document_value(existing));
            continue;
        }
        if last.is_some_and(|last| position <= last) {
            return Err("New source positions must advance the retained checkpoint".into());
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
        identities.insert((source.clone(), id), staged.documents.len());
        positions.insert((source.clone(), position), staged.documents.len());
        checkpoints.insert(source, position);
        staged.resolved.push(document_value(&document));
        staged.documents.push(document);
    }
    if !staged.documents.is_empty() {
        let payloads: Vec<_> = staged
            .documents
            .iter()
            .map(|doc| doc.record.payload.clone())
            .collect();
        let mapped = map_documents(runtime, payloads)?;
        let mut grouped: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for emissions in mapped {
            for emission in emissions {
                grouped
                    .entry(canonical_json(&emission["key"])?)
                    .or_default()
                    .push(emission["value"].clone());
            }
        }
        for (key, values) in grouped {
            let previous = state
                .versions
                .get(&key)
                .and_then(|indices| indices.last())
                .map(|&i| state.history[i].state.clone());
            let previous = match previous {
                Some(value) => value,
                None => runtime.call(json!({"op":"identity"}))?,
            };
            let mut new_state =
                runtime.call(json!({"op":"add","state":previous,"values":values}))?;
            let mut result = runtime.call(json!({"op":"finalize","state":new_state}))?;
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
    }
    Ok(staged)
}

fn cursor_error(message: &str) -> String {
    format!("{message}. {CURSOR_RECOVERY}")
}

fn canonical_payload(payload: &Value) -> Result<(String, Value)> {
    validate_json(payload, 0)?;
    let text = canonical_json(payload)?;
    let mut payload = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    normalize_json_numbers(&mut payload);
    Ok((text, payload))
}

// Mapping diagnostics and ingestion deliberately share the complete contract.
// Reducers run only after this helper returns during normal ingestion.
fn map_documents(runtime: &Runtime, payloads: Vec<Value>) -> Result<Vec<Vec<Value>>> {
    let count = payloads.len();
    let mapped = runtime.call(json!({"op":"map","documents":payloads}))?;
    let mapped = mapped
        .as_array()
        .ok_or("map must return one contribution array per document")?;
    if mapped.len() != count {
        return Err("map returned the wrong number of document results".into());
    }
    let mut output = Vec::with_capacity(count);
    let mut contributions = 0;
    for emissions in mapped {
        let emissions = emissions
            .as_array()
            .ok_or("Each map result must be an array")?;
        let mut normalized = Vec::with_capacity(emissions.len());
        for emission in emissions {
            let key = emission
                .get("key")
                .ok_or("A map contribution requires key")?;
            let value = emission
                .get("value")
                .ok_or("A map contribution requires value")?;
            contributions += 1;
            if contributions > MAX_CONTRIBUTIONS {
                return Err("A batch may emit at most 4096 contributions".into());
            }
            validate_json(key, 0)?;
            validate_json(value, 0)?;
            // Preserve the guest's contribution values, including signed zero.
            // Re-encoding through canonical JSON would change reducer inputs.
            normalized.push(json!({"key":key.clone(),"value":value.clone()}));
        }
        output.push(normalized);
    }
    Ok(output)
}

fn checkpoint_matches_log(state: &State, batches: &[storage::VerifiedBatch]) -> bool {
    let mut documents = state.documents.iter();
    for batch in batches {
        let mut previous = None;
        for record in &batch.batch.documents {
            let Some(document) = documents.next() else {
                return false;
            };
            if document.record != *record
                || previous.is_some_and(|sequence| document.sequence != sequence + 1)
            {
                return false;
            }
            previous = Some(document.sequence);
        }
    }
    documents.next().is_none()
}

fn document_value(document: &Document) -> Value {
    let record = &document.record;
    json!({"id":record.id,"source":record.source,"position":record.position,"payload":record.payload,"sequence":document.sequence})
}
fn version_value(version: &Version) -> Result<Value> {
    let mut key: Value = serde_json::from_str(&version.key).map_err(|_| "Invalid retained key")?;
    normalize_json_numbers(&mut key);
    Ok(json!({"key":key,"state":version.state,"result":version.result,"sequence":version.sequence}))
}
fn validate_record(record: &RetainedDocument) -> Result<()> {
    if record.id.is_empty()
        || record.id.len() > 4096
        || record.source.is_empty()
        || record.source.len() > 4096
        || record.position > MAX_SAFE_SEQUENCE
    {
        return Err("Invalid retained document identity or source position".into());
    }
    validate_json(&record.payload, 0)
}
fn validate_json(value: &Value, depth: usize) -> Result<()> {
    if depth > 64 {
        return Err("JSON nesting exceeds 64".into());
    }
    match value {
        Value::Array(values) => {
            for value in values {
                validate_json(value, depth + 1)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_json(value, depth + 1)?;
            }
        }
        Value::Number(n) if n.as_f64().is_none_or(|v| !v.is_finite()) => {
            return Err("JSON numbers must be finite".into())
        }
        _ => {}
    }
    Ok(())
}
fn next_sequence(sequence: u64) -> Result<u64> {
    sequence
        .checked_add(1)
        .filter(|n| *n <= MAX_SAFE_SEQUENCE)
        .ok_or_else(|| "JavaScript-safe sequence limit reached".into())
}
fn optional_string(object: &Map<String, Value>, key: &str) -> Result<Option<String>> {
    match object.get(key) {
        None => Ok(None),
        Some(Value::String(value)) if !value.is_empty() && value.len() <= 4096 => {
            Ok(Some(value.clone()))
        }
        _ => Err(format!(
            "{key} must be a nonempty string of at most 4096 bytes"
        )),
    }
}
fn safe_integer(value: &Value, field: &str) -> Result<u64> {
    value
        .as_u64()
        .filter(|v| *v <= MAX_SAFE_SEQUENCE)
        .ok_or_else(|| format!("{field} must be a nonnegative JavaScript-safe integer"))
}
fn page_limit(request: &Value) -> Result<usize> {
    match request.get("limit") {
        None => Ok(100),
        Some(value) => value
            .as_u64()
            .filter(|n| *n > 0 && *n <= MAX_PAGE as u64)
            .map(|n| n as usize)
            .ok_or_else(|| "limit must be between 1 and 1024".into()),
    }
}
fn parse_after(request: &Value, cursor: &str) -> Result<Option<Value>> {
    match request.get("after") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(token)) => {
            let parsed: Value = serde_json::from_str(token).map_err(|_| "Invalid after token")?;
            let array = parsed
                .as_array()
                .filter(|v| v.len() == 2)
                .ok_or("Invalid after token")?;
            if array[0].as_str() != Some(cursor) {
                return Err("After token belongs to a different snapshot".into());
            }
            Ok(Some(array[1].clone()))
        }
        _ => Err("after must be an opaque pagination string".into()),
    }
}
/// Canonical JSON keys use JavaScript numeric formatting and UTF-16 property order.
/// This makes compound keys equal across Rust, Python and browser hosts.
pub fn canonical_json(value: &Value) -> Result<String> {
    fn write(value: &Value, output: &mut String) -> Result<()> {
        match value {
            Value::Null => output.push_str("null"),
            Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
            Value::Number(value) => {
                let number = value
                    .as_f64()
                    .filter(|v| v.is_finite())
                    .ok_or("JSON numbers must be finite")?;
                output.push_str(ryu_js::Buffer::new().format_finite(number));
            }
            Value::String(value) => {
                output.push_str(&serde_json::to_string(value).map_err(|e| e.to_string())?)
            }
            Value::Array(values) => {
                output.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write(value, output)?;
                }
                output.push(']');
            }
            Value::Object(values) => {
                let mut keys: Vec<_> = values.keys().collect();
                keys.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
                output.push('{');
                for (index, key) in keys.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    output.push_str(&serde_json::to_string(key).map_err(|e| e.to_string())?);
                    output.push(':');
                    write(&values[*key], output)?;
                }
                output.push('}');
            }
        }
        Ok(())
    }
    let mut output = String::new();
    write(value, &mut output)?;
    Ok(output)
}

// Portable JSON wire numbers have JavaScript/IEEE-754 semantics. Preserve small
// integer variants for convenient API replies; use floating variants for large
// numbers so a Python JSON decoder cannot mistake a shortest decimal spelling
// (e.g.1000000000000000100) for an exact arbitrary-precision integer.
// Source positions and engine sequences never pass through this function.
fn normalize_json_numbers(value: &mut Value) {
    match value {
        Value::Number(number) => {
            if let Some(float) = number.as_f64() {
                if float.abs() > MAX_SAFE_SEQUENCE as f64 {
                    *number = serde_json::Number::from_f64(float).expect("JSON numbers are finite");
                }
            }
        }
        Value::Array(values) => values.iter_mut().for_each(normalize_json_numbers),
        Value::Object(values) => values.values_mut().for_each(normalize_json_numbers),
        _ => {}
    }
}
