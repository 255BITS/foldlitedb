//! Descriptor-bound disposable checkpoints for trusted Python callback views.
use crate::{
    json::{canonical_json, safe_integer, validate_exact_integers, validate_json},
    Result, MAX_SAFE_SEQUENCE,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CallbackGroup {
    pub key: Value,
    pub state: Value,
    pub result: Value,
    pub sequence: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCallbackCheckpoint {
    format: u64,
    descriptor: Value,
    sequence: u64,
    groups: Vec<CallbackGroup>,
}

#[derive(Clone, Debug)]
pub(crate) struct CallbackCheckpoint {
    format: u64,
    descriptor: Value,
    sequence: u64,
    groups: BTreeMap<String, CallbackGroup>,
}

impl Serialize for CallbackCheckpoint {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Encoded<'a> {
            format: u64,
            descriptor: &'a Value,
            sequence: u64,
            groups: Vec<&'a CallbackGroup>,
        }
        Encoded {
            format: self.format,
            descriptor: &self.descriptor,
            sequence: self.sequence,
            groups: self.groups.values().collect(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CallbackCheckpoint {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw = RawCallbackCheckpoint::deserialize(deserializer)?;
        let mut groups = BTreeMap::new();
        for group in raw.groups {
            validate_json(&group.key, 0).map_err(serde::de::Error::custom)?;
            validate_exact_integers(&group.key).map_err(serde::de::Error::custom)?;
            let encoded = canonical_json(&group.key).map_err(serde::de::Error::custom)?;
            if groups.insert(encoded, group).is_some() {
                return Err(serde::de::Error::custom(
                    "Duplicate canonical key in callback checkpoint",
                ));
            }
        }
        Ok(Self {
            format: raw.format,
            descriptor: raw.descriptor,
            sequence: raw.sequence,
            groups,
        })
    }
}

pub(crate) fn callback_checkpoints(
    request: &Value,
    sequence: u64,
) -> Result<BTreeMap<String, CallbackCheckpoint>> {
    parse_callback_checkpoint_map(request, "checkpoints", sequence)
}

pub(crate) fn callback_updates(
    request: &Value,
    sequence: u64,
) -> Result<BTreeMap<String, CallbackCheckpoint>> {
    parse_callback_checkpoint_map(request, "callback_updates", sequence)
}

pub(crate) fn apply_callback_updates(
    target: &mut BTreeMap<String, CallbackCheckpoint>,
    updates: BTreeMap<String, CallbackCheckpoint>,
) {
    for (name, delta) in updates {
        match target.get_mut(&name) {
            Some(existing)
                if existing.format == delta.format && existing.descriptor == delta.descriptor =>
            {
                existing.sequence = delta.sequence;
                existing.groups.extend(delta.groups);
            }
            _ => {
                target.insert(name, delta);
            }
        }
    }
}

fn parse_callback_checkpoint_map(
    request: &Value,
    field: &str,
    sequence: u64,
) -> Result<BTreeMap<String, CallbackCheckpoint>> {
    let Some(raw) = request.get(field) else {
        return Ok(BTreeMap::new());
    };
    let entries = raw
        .as_object()
        .ok_or("Callback checkpoints must be an object")?;
    let mut parsed = BTreeMap::new();
    for (name, checkpoint) in entries {
        let checkpoint_obj = checkpoint
            .as_object()
            .ok_or("Invalid callback checkpoint name or format")?;
        if name.is_empty()
            || name.len() > 4096
            || checkpoint_obj.get("format").and_then(Value::as_u64) != Some(1)
            || !checkpoint_obj
                .keys()
                .all(|k| matches!(k.as_str(), "format" | "descriptor" | "sequence" | "groups"))
        {
            return Err("Invalid callback checkpoint name or format".into());
        }
        let coverage = safe_integer(
            checkpoint_obj
                .get("sequence")
                .ok_or("Callback checkpoint requires coverage")?,
            "callback coverage",
        )?;
        if coverage > sequence {
            return Err("Callback checkpoint is ahead of the committed log".into());
        }
        let descriptor = checkpoint_obj
            .get("descriptor")
            .ok_or("Callback checkpoint requires descriptor")?;
        validate_json(descriptor, 0)?;
        let raw_groups = checkpoint_obj
            .get("groups")
            .and_then(Value::as_array)
            .ok_or("Callback checkpoint requires groups")?;
        let mut groups = BTreeMap::new();
        for group in raw_groups {
            let group_obj = group
                .as_object()
                .ok_or("Callback group requires key, state and result")?;
            if !group_obj
                .keys()
                .all(|k| matches!(k.as_str(), "key" | "state" | "result" | "sequence"))
            {
                return Err("Callback group contains unknown fields".into());
            }
            let key = group_obj
                .get("key")
                .ok_or("Callback group requires key, state and result")?;
            let state = group_obj
                .get("state")
                .ok_or("Callback group requires key, state and result")?;
            let result = group_obj
                .get("result")
                .ok_or("Callback group requires key, state and result")?;
            for value in [key, state, result] {
                validate_json(value, 0)?;
                validate_exact_integers(value)?;
            }
            let group_sequence = safe_integer(
                group_obj
                    .get("sequence")
                    .ok_or("Callback group requires sequence")?,
                "callback group sequence",
            )?;
            if group_sequence > coverage {
                return Err("Callback group is ahead of its checkpoint".into());
            }
            let canonical_key = canonical_json(key)?;
            if groups
                .insert(
                    canonical_key,
                    CallbackGroup {
                        key: key.clone(),
                        state: state.clone(),
                        result: result.clone(),
                        sequence: group_sequence,
                    },
                )
                .is_some()
            {
                return Err("Duplicate canonical key in callback checkpoint".into());
            }
        }
        parsed.insert(
            name.clone(),
            CallbackCheckpoint {
                format: 1,
                descriptor: descriptor.clone(),
                sequence: coverage,
                groups,
            },
        );
    }
    Ok(parsed)
}

pub(crate) fn validate_callback_checkpoints(
    checkpoints: &BTreeMap<String, CallbackCheckpoint>,
    sequence: u64,
) -> Result<()> {
    for (name, checkpoint) in checkpoints {
        if name.is_empty() || name.len() > 4096 || checkpoint.format != 1 {
            return Err("Invalid callback checkpoint name or format".into());
        }
        if checkpoint.sequence > MAX_SAFE_SEQUENCE || checkpoint.sequence > sequence {
            return Err("Callback checkpoint is ahead of the committed log".into());
        }
        validate_json(&checkpoint.descriptor, 0)?;
        for (canonical_key, group) in &checkpoint.groups {
            for value in [&group.key, &group.state, &group.result] {
                validate_json(value, 0)?;
                validate_exact_integers(value)?;
            }
            if canonical_json(&group.key)? != *canonical_key {
                return Err("Invalid canonical key in callback checkpoint".into());
            }
            if group.sequence > MAX_SAFE_SEQUENCE || group.sequence > checkpoint.sequence {
                return Err("Callback group is ahead of its checkpoint".into());
            }
        }
    }
    Ok(())
}
