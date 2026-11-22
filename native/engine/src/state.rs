//! In-memory derived engine state, indexes, and borrowed hash-map lookup keys.
use crate::{
    callback::{validate_callback_checkpoints, CallbackCheckpoint},
    json::{canonical_json, normalize_json_numbers, validate_json},
    query::QueryKey,
    Result, MAX_BATCH_DOCUMENTS, MAX_SAFE_SEQUENCE,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap},
    hash::{Hash, Hasher},
};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IdentityKey {
    pub source: String,
    pub id: String,
}

pub(crate) trait IdentityLookup {
    fn parts(&self) -> (&str, &str);
}

impl IdentityLookup for IdentityKey {
    fn parts(&self) -> (&str, &str) {
        (&self.source, &self.id)
    }
}

impl IdentityLookup for (&str, &str) {
    fn parts(&self) -> (&str, &str) {
        *self
    }
}

impl Hash for IdentityKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

impl<'a> Borrow<dyn IdentityLookup + 'a> for IdentityKey {
    fn borrow(&self) -> &(dyn IdentityLookup + 'a) {
        self
    }
}

impl PartialEq for dyn IdentityLookup + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}

impl Eq for dyn IdentityLookup + '_ {}

impl Hash for dyn IdentityLookup + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

#[inline]
pub(crate) fn get_identity(
    map: &HashMap<IdentityKey, usize>,
    source: &str,
    id: &str,
) -> Option<usize> {
    map.get(&(source, id) as &dyn IdentityLookup).copied()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PositionKey {
    pub source: String,
    pub position: u64,
}

pub(crate) trait PositionLookup {
    fn parts(&self) -> (&str, u64);
}

impl PositionLookup for PositionKey {
    fn parts(&self) -> (&str, u64) {
        (&self.source, self.position)
    }
}

impl PositionLookup for (&str, u64) {
    fn parts(&self) -> (&str, u64) {
        *self
    }
}

impl Hash for PositionKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

impl<'a> Borrow<dyn PositionLookup + 'a> for PositionKey {
    fn borrow(&self) -> &(dyn PositionLookup + 'a) {
        self
    }
}

impl PartialEq for dyn PositionLookup + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}

impl Eq for dyn PositionLookup + '_ {}

impl Hash for dyn PositionLookup + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

#[inline]
pub(crate) fn get_position(
    map: &HashMap<PositionKey, usize>,
    source: &str,
    position: u64,
) -> Option<usize> {
    map.get(&(source, position) as &dyn PositionLookup).copied()
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedDocument {
    pub id: String,
    pub source: String,
    pub position: u64,
    pub payload: Value,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Document {
    #[serde(flatten)]
    pub record: RetainedDocument,
    pub sequence: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Version {
    pub key: String,
    pub state: Value,
    pub result: Value,
    pub sequence: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BatchBoundary {
    pub documents: usize,
    pub sequence: u64,
}

pub(crate) struct Stage {
    pub documents: Vec<Document>,
    pub versions: Vec<Version>,
    pub sequence: u64,
    pub resolved: Vec<Value>,
    pub replayed: usize,
    pub query_keys: BTreeMap<QueryKey, String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct State {
    pub generation: String,
    pub sequence: u64,
    pub documents: Vec<Document>,
    pub history: Vec<Version>,
    #[serde(default)]
    pub batches: Vec<BatchBoundary>,
    #[serde(default)]
    pub callback_checkpoints: BTreeMap<String, CallbackCheckpoint>,
    #[serde(skip)]
    pub identities: HashMap<IdentityKey, usize>,
    #[serde(skip)]
    pub positions: HashMap<PositionKey, usize>,
    #[serde(skip)]
    pub checkpoints: BTreeMap<String, u64>,
    #[serde(skip)]
    pub versions: HashMap<String, Vec<usize>>,
    #[serde(skip)]
    pub query_keys: BTreeMap<QueryKey, String>,
}

impl State {
    pub(crate) fn empty() -> Self {
        Self {
            generation: Uuid::new_v4().to_string(),
            sequence: 0,
            documents: Vec::new(),
            history: Vec::new(),
            batches: Vec::new(),
            callback_checkpoints: BTreeMap::new(),
            identities: HashMap::new(),
            positions: HashMap::new(),
            checkpoints: BTreeMap::new(),
            versions: HashMap::new(),
            query_keys: BTreeMap::new(),
        }
    }

    // Checkpoint data is never authoritative. Validate every structural invariant
    // before rebuilding its indexes; the verified log separately proves provenance.
    pub(crate) fn validate_and_index(&mut self) -> Result<()> {
        if Uuid::parse_str(&self.generation).is_err()
            || self.sequence > MAX_SAFE_SEQUENCE
            || self.sequence != (self.documents.len() + self.history.len()) as u64
        {
            return Err("Invalid cached generation or sequence".into());
        }
        self.identities.reserve(self.documents.len());
        self.positions.reserve(self.documents.len());
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
                .insert(
                    IdentityKey {
                        source: record.source.clone(),
                        id: record.id.clone(),
                    },
                    index,
                )
                .is_some()
                || self
                    .positions
                    .insert(
                        PositionKey {
                            source: record.source.clone(),
                            position: record.position,
                        },
                        index,
                    )
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
            if version.sequence <= version_sequence || version.sequence > self.sequence {
                return Err("Invalid cached version sequence or key".into());
            }
            validate_json(&version.state, 0)?;
            validate_json(&version.result, 0)?;
            version_sequence = version.sequence;
            if let Some(indices) = self.versions.get_mut(&version.key) {
                indices.push(index);
            } else {
                let mut key: Value =
                    serde_json::from_str(&version.key).map_err(|_| "Invalid cached key")?;
                validate_json(&key, 0)?;
                if canonical_json(&key)? != version.key {
                    return Err("Invalid cached version sequence or key".into());
                }
                normalize_json_numbers(&mut key);
                self.versions.insert(version.key.clone(), vec![index]);
                self.query_keys.insert(QueryKey(key), version.key.clone());
            }
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
        let mut documents = 0;
        let mut sequence = 0;
        for batch in &self.batches {
            if batch.documents <= documents
                || batch.documents > self.documents.len()
                || batch.documents - documents > MAX_BATCH_DOCUMENTS
                || batch.sequence <= sequence
                || batch.sequence > self.sequence
                || self.documents[batch.documents - 1].sequence > batch.sequence
            {
                return Err("Invalid cached document batch boundaries".into());
            }
            documents = batch.documents;
            sequence = batch.sequence;
        }
        if documents != self.documents.len() || sequence != self.sequence {
            return Err("Cached document batches do not cover the log".into());
        }
        validate_callback_checkpoints(&self.callback_checkpoints, self.sequence)?;
        Ok(())
    }

    pub(crate) fn publish(&mut self, stage: Stage) {
        let appended = !stage.documents.is_empty();
        self.query_keys.extend(stage.query_keys);
        for document in stage.documents {
            let record = &document.record;
            let index = self.documents.len();
            self.identities.insert(
                IdentityKey {
                    source: record.source.clone(),
                    id: record.id.clone(),
                },
                index,
            );
            self.positions.insert(
                PositionKey {
                    source: record.source.clone(),
                    position: record.position,
                },
                index,
            );
            self.checkpoints
                .insert(record.source.clone(), record.position);
            self.documents.push(document);
        }
        for version in stage.versions {
            if let Some(indices) = self.versions.get_mut(&version.key) {
                indices.push(self.history.len());
            } else {
                self.versions
                    .insert(version.key.clone(), vec![self.history.len()]);
            }
            self.history.push(version);
        }
        self.sequence = stage.sequence;
        if appended {
            self.batches.push(BatchBoundary {
                documents: self.documents.len(),
                sequence: self.sequence,
            });
        }
    }
}

pub(crate) fn document_value(document: &Document) -> Value {
    let record = &document.record;
    json!({"id":record.id,"source":record.source,"position":record.position,"payload":record.payload,"sequence":document.sequence})
}

pub(crate) fn version_value(version: &Version) -> Result<Value> {
    let mut key: Value = serde_json::from_str(&version.key).map_err(|_| "Invalid retained key")?;
    normalize_json_numbers(&mut key);
    Ok(version_value_with_key(&key, version))
}

pub(crate) fn version_value_with_key(key: &Value, version: &Version) -> Value {
    json!({"key":key,"state":version.state,"result":version.result,"sequence":version.sequence})
}

pub(crate) fn validate_record(record: &RetainedDocument) -> Result<()> {
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
