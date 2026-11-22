//! Portable JSON validation, numeric normalization, and canonical encoding.
use crate::{Result, MAX_JSON_DEPTH, MAX_PAGE, MAX_SAFE_SEQUENCE};
use serde_json::{Map, Number, Value};

pub(crate) fn validate_json(value: &Value, depth: usize) -> Result<()> {
    if depth > MAX_JSON_DEPTH {
        return Err(format!("JSON nesting exceeds {MAX_JSON_DEPTH}"));
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

/// The Python host's rule, `int(float(n)) == n`: an integer is accepted only
/// when converting it to the nearest IEEE-754 double loses nothing. Floating
/// JSON numbers are doubles by definition. Integer literals outside the 64-bit
/// range are already parsed as doubles by `serde_json`.
pub(crate) fn integer_is_exact_double(number: &Number) -> bool {
    if let Some(value) = number.as_u64() {
        (value as f64) as u128 == u128::from(value)
    } else if let Some(value) = number.as_i64() {
        (value as f64) as i128 == i128::from(value)
    } else {
        true
    }
}

/// Rejects host-supplied integers that a double cannot represent exactly,
/// before any mapping or durable write, rather than silently rounding them.
pub(crate) fn validate_exact_integers(value: &Value) -> Result<()> {
    match value {
        Value::Number(number) if !integer_is_exact_double(number) => Err(inexact_integer_error(
            number,
            number.as_f64().unwrap_or(0.0),
        )),
        Value::Array(values) => values.iter().try_for_each(validate_exact_integers),
        Value::Object(values) => values.values().try_for_each(validate_exact_integers),
        _ => Ok(()),
    }
}

fn inexact_integer_error(number: &Number, nearest: f64) -> String {
    format!(
        "JSON integer {number} is not exactly representable as an IEEE-754 double \
         (nearest is {}); encode larger identifiers as strings",
        ryu_js::Buffer::new().format_finite(nearest)
    )
}

/// Normalize a finite `serde_json::Number` to match canonical JS/IEEE-754 wire
/// semantics: `-0.0` and `0.0` become integer `0`, integral doubles within
/// `-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER` become integer variants, and numbers
/// with magnitude above `MAX_SAFE_INTEGER` become floating variants so arbitrary-
/// precision decoders (such as Python's `json`) keep double semantics.
#[inline]
fn normalize_canonical_number(number: &mut Number, float: f64) {
    if float == 0.0 {
        if !number.is_u64() {
            *number = Number::from(0u64);
        }
    } else if float.abs() > MAX_SAFE_SEQUENCE as f64 {
        if !number.is_f64() {
            *number = Number::from_f64(float).expect("JSON numbers are finite");
        }
    } else if number.is_f64() && float.fract() == 0.0 {
        if float > 0.0 {
            *number = Number::from(float as u64);
        } else {
            *number = Number::from(float as i64);
        }
    }
}

/// Single-pass validation and normalization for host-supplied payloads:
/// checks nesting depth (`<= MAX_JSON_DEPTH`), finiteness, exact-double integer
/// representation, and normalizes numbers in place without serializing and
/// re-parsing through canonical JSON.
pub(crate) fn validate_and_normalize_payload(value: &mut Value) -> Result<()> {
    fn visit(value: &mut Value, depth: usize) -> Result<()> {
        if depth > MAX_JSON_DEPTH {
            return Err(format!("JSON nesting exceeds {MAX_JSON_DEPTH}"));
        }
        match value {
            Value::Array(values) => {
                for item in values {
                    visit(item, depth + 1)?;
                }
            }
            Value::Object(values) => {
                for item in values.values_mut() {
                    visit(item, depth + 1)?;
                }
            }
            Value::Number(number) => {
                let Some(float) = number.as_f64().filter(|v| v.is_finite()) else {
                    return Err("JSON numbers must be finite".into());
                };
                if !integer_is_exact_double(number) {
                    return Err(inexact_integer_error(number, float));
                }
                normalize_canonical_number(number, float);
            }
            _ => {}
        }
        Ok(())
    }
    visit(value, 0)
}

/// Normalize numbers in a validated JSON key so it matches the value obtained by
/// parsing `canonical_json(key)` and running `normalize_json_numbers`.
pub(crate) fn normalize_canonical_numbers(value: &mut Value) {
    match value {
        Value::Number(number) => {
            if let Some(float) = number.as_f64() {
                normalize_canonical_number(number, float);
            }
        }
        Value::Array(values) => values.iter_mut().for_each(normalize_canonical_numbers),
        Value::Object(values) => values.values_mut().for_each(normalize_canonical_numbers),
        _ => {}
    }
}

// Portable JSON wire numbers have JavaScript/IEEE-754 semantics. Preserve small
// integer variants for convenient API replies; use floating variants for large
// numbers so a Python JSON decoder cannot mistake a shortest decimal spelling
// (e.g. 1000000000000000100) for an exact arbitrary-precision integer.
// Source positions and engine sequences never pass through this function.
pub(crate) fn normalize_json_numbers(value: &mut Value) {
    match value {
        Value::Number(number) => {
            if let Some(float) = number.as_f64() {
                if float.abs() > MAX_SAFE_SEQUENCE as f64 {
                    *number = Number::from_f64(float).expect("JSON numbers are finite");
                }
            }
        }
        Value::Array(values) => values.iter_mut().for_each(normalize_json_numbers),
        Value::Object(values) => values.values_mut().for_each(normalize_json_numbers),
        _ => {}
    }
}

/// Write a JSON-escaped string directly into `output`, matching `serde_json::to_string`
/// byte-for-byte without intermediate `String` allocations.
fn write_escaped_json_string(value: &str, output: &mut String) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push('"');
    let bytes = value.as_bytes();
    let mut start = 0;
    for (index, &byte) in bytes.iter().enumerate() {
        let escape = match byte {
            b'"' => Some("\\\""),
            b'\\' => Some("\\\\"),
            0x08 => Some("\\b"),
            0x0C => Some("\\f"),
            b'\n' => Some("\\n"),
            b'\r' => Some("\\r"),
            b'\t' => Some("\\t"),
            0x00..=0x1F => None,
            _ => continue,
        };
        if start < index {
            output.push_str(&value[start..index]);
        }
        if let Some(seq) = escape {
            output.push_str(seq);
        } else {
            let buf = [
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[(byte >> 4) as usize],
                HEX[(byte & 0x0f) as usize],
            ];
            output.push_str(std::str::from_utf8(&buf).expect("ASCII hex escape"));
        }
        start = index + 1;
    }
    if start < bytes.len() {
        output.push_str(&value[start..]);
    }
    output.push('"');
}

/// Canonical JSON keys use JavaScript numeric formatting and UTF-16 property order.
/// This makes compound keys equal across Rust, Python and browser hosts.
pub fn canonical_json(value: &Value) -> Result<String> {
    fn write(value: &Value, output: &mut String, ryu_buf: &mut ryu_js::Buffer) -> Result<()> {
        match value {
            Value::Null => output.push_str("null"),
            Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
            Value::Number(value) => {
                let number = value
                    .as_f64()
                    .filter(|v| v.is_finite())
                    .ok_or("JSON numbers must be finite")?;
                output.push_str(ryu_buf.format_finite(number));
            }
            Value::String(value) => write_escaped_json_string(value, output),
            Value::Array(values) => {
                output.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write(value, output, ryu_buf)?;
                }
                output.push(']');
            }
            Value::Object(values) => {
                // `serde_json::Map` without `preserve_order` is a `BTreeMap`
                // sorted by UTF-8 / Unicode scalar order. UTF-8 order equals
                // UTF-16 code-unit order for all characters < U+E000 (whose
                // UTF-8 bytes are strictly < 0xEE). Only objects with > 1 key
                // containing a byte >= 0xEE need a UTF-16 re-sort.
                let needs_utf16_sort = values.len() > 1
                    && values
                        .keys()
                        .any(|key| key.as_bytes().iter().any(|&b| b >= 0xEE));
                output.push('{');
                if !needs_utf16_sort {
                    for (index, (key, value)) in values.iter().enumerate() {
                        if index > 0 {
                            output.push(',');
                        }
                        write_escaped_json_string(key, output);
                        output.push(':');
                        write(value, output, ryu_buf)?;
                    }
                } else {
                    let mut entries: Vec<(&String, &Value)> = values.iter().collect();
                    entries.sort_by(|a, b| a.0.encode_utf16().cmp(b.0.encode_utf16()));
                    for (index, (key, value)) in entries.into_iter().enumerate() {
                        if index > 0 {
                            output.push(',');
                        }
                        write_escaped_json_string(key, output);
                        output.push(':');
                        write(value, output, ryu_buf)?;
                    }
                }
                output.push('}');
            }
        }
        Ok(())
    }
    let mut output = String::new();
    let mut ryu_buf = ryu_js::Buffer::new();
    write(value, &mut output, &mut ryu_buf)?;
    Ok(output)
}

pub(crate) fn next_sequence(sequence: u64) -> Result<u64> {
    sequence
        .checked_add(1)
        .filter(|n| *n <= MAX_SAFE_SEQUENCE)
        .ok_or_else(|| "JavaScript-safe sequence limit reached".into())
}

pub(crate) fn optional_string_remove(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<String>> {
    match object.remove(key) {
        None => Ok(None),
        Some(Value::String(value)) if !value.is_empty() && value.len() <= 4096 => Ok(Some(value)),
        _ => Err(format!(
            "{key} must be a nonempty string of at most 4096 bytes"
        )),
    }
}

pub(crate) fn safe_integer(value: &Value, field: &str) -> Result<u64> {
    value
        .as_u64()
        .filter(|v| *v <= MAX_SAFE_SEQUENCE)
        .ok_or_else(|| format!("{field} must be a nonnegative JavaScript-safe integer"))
}

pub(crate) fn page_limit(request: &Value) -> Result<usize> {
    match request.get("limit") {
        None => Ok(100),
        Some(value) => value
            .as_u64()
            .filter(|n| *n > 0 && *n <= MAX_PAGE as u64)
            .map(|n| n as usize)
            .ok_or_else(|| "limit must be between 1 and 1024".into()),
    }
}

pub(crate) fn parse_after(request: &Value, cursor: &str) -> Result<Option<Value>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn direct_string_escaping_matches_serde_json_byte_for_byte() {
        let mut all_ascii = String::new();
        for byte in 0u8..=127 {
            all_ascii.push(byte as char);
        }
        let samples = [
            "",
            "plain ascii key",
            "quotes \" and backslash \\ and slash /",
            "\x00\x01\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x1f\x7f",
            &all_ascii,
            "unicode: café 日本語 \u{2028}\u{2029} \u{d7ff} \u{e000} \u{ffff} \u{10000} 🚀 \u{10ffff}",
        ];
        for sample in samples {
            let mut actual = String::new();
            write_escaped_json_string(sample, &mut actual);
            let expected = serde_json::to_string(sample).unwrap();
            assert_eq!(actual, expected, "mismatch for {sample:?}");
        }
    }

    #[test]
    fn canonical_json_utf16_key_order_handles_bmp_pua_and_supplementary_planes() {
        // U+D7FF (0xD7FF) < U+10000 (0xD800 0xDC00) < U+10FFFF (0xDBFF 0xDFFF)
        // < U+E000 (0xE000) < U+FFFF (0xFFFF) in UTF-16 code unit order.
        let value = json!({
            "\u{ffff}": 5,
            "\u{e000}": 4,
            "\u{10ffff}": 3,
            "\u{10000}": 2,
            "\u{d7ff}": 1,
            "ascii": 0,
        });
        assert_eq!(
            canonical_json(&value).unwrap(),
            "{\"ascii\":0,\"\u{d7ff}\":1,\"\u{10000}\":2,\"\u{10ffff}\":3,\"\u{e000}\":4,\"\u{ffff}\":5}"
        );

        // Nested objects mix fast-path (< U+E000) and slow-path (>= U+E000) keys.
        let nested = json!({
            "b": {"\u{e000}": "pua", "\u{1f600}": "emoji"},
            "a": {"\u{d7ff}": "high-bmp", "z": "ascii"},
        });
        assert_eq!(
            canonical_json(&nested).unwrap(),
            "{\"a\":{\"z\":\"ascii\",\"\u{d7ff}\":\"high-bmp\"},\"b\":{\"\u{1f600}\":\"emoji\",\"\u{e000}\":\"pua\"}}"
        );
    }

    #[test]
    fn single_pass_payload_normalization_matches_round_trip_canonical_normalization() {
        let cases = vec![
            json!(null),
            json!(true),
            json!(0),
            json!(0.0),
            json!(-0.0),
            json!(12.0),
            json!(-12.0),
            json!(1.5),
            json!(5e-324_f64),
            json!(-9.281557522556183e-188_f64),
            json!(9_007_199_254_740_991_u64),
            json!(-9_007_199_254_740_991_i64),
            json!(9_007_199_254_740_992_u64),
            json!(-9_007_199_254_740_992_i64),
            json!(i64::MIN),
            json!(1_u64 << 63),
            json!(1e18_f64 + 128.0),
            json!({"a": -0.0, "b": [12.0, 9_007_199_254_740_992_u64, -9_007_199_254_740_994_i64]}),
        ];
        for original in cases {
            let mut single_pass = original.clone();
            validate_and_normalize_payload(&mut single_pass).unwrap();

            validate_json(&original, 0).unwrap();
            validate_exact_integers(&original).unwrap();
            let text = canonical_json(&original).unwrap();
            let mut round_trip: Value = serde_json::from_str(&text).unwrap();
            normalize_json_numbers(&mut round_trip);

            assert_eq!(single_pass, round_trip, "value mismatch for {original}");
            assert_eq!(
                serde_json::to_string(&single_pass).unwrap(),
                serde_json::to_string(&round_trip).unwrap(),
                "wire mismatch for {original}"
            );
        }
    }
}
