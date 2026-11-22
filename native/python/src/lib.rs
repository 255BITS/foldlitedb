//! Python owns ergonomic objects; the engine owns transactions and Wasm execution.
use foldlitedb_portable_engine::{
    Engine as NativeEngine, EngineError, ReducerInstance, MAX_CACHE_BYTES, MAX_JSON_DEPTH,
    MAX_SAFE_INTEGER,
};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple},
};
use serde_json::{Map, Number, Value};
use std::sync::Mutex;

// Transport envelopes wrap user data (e.g. callback checkpoint groups add 5
// levels). Allowing a small envelope margin above MAX_JSON_DEPTH guarantees
// valid depth-64 user values reach the engine's exact per-field depth checks
// while stopping cyclic Python containers before stack overflow.
const MAX_TRANSPORT_DEPTH: usize = MAX_JSON_DEPTH + 8;
const MAX_EXACT_SAFE_INT: i64 = (MAX_SAFE_INTEGER + 1) as i64; // 2^53

fn database_error(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(("DatabaseError", message.into()))
}

fn inexact_int_error() -> PyErr {
    database_error(
        "Portable JSON integers must be exactly representable as IEEE-754 doubles; \
         encode larger identifiers as strings",
    )
}

fn py_int_to_json(py: Python<'_>, obj: &Bound<'_, PyInt>) -> PyResult<Value> {
    if let Ok(n) = obj.extract::<i64>() {
        if (-MAX_EXACT_SAFE_INT..=MAX_EXACT_SAFE_INT).contains(&n) {
            return Ok(if n >= 0 {
                Value::Number(Number::from(n as u64))
            } else {
                Value::Number(Number::from(n))
            });
        }
        if n >= 0 {
            let u = n as u64;
            if (u as f64) as u128 != u128::from(u) {
                return Err(inexact_int_error());
            }
            return Ok(Value::Number(Number::from(u)));
        }
        if (n as f64) as i128 != i128::from(n) {
            return Err(inexact_int_error());
        }
        return Ok(Value::Number(Number::from(n)));
    }
    if let Ok(u) = obj.extract::<u64>() {
        if (u as f64) as u128 != u128::from(u) {
            return Err(inexact_int_error());
        }
        return Ok(Value::Number(Number::from(u)));
    }
    // Arbitrary-precision Python integers outside i64/u64: enforce exact
    // Python rule `int(float(n)) == n` and encode as IEEE-754 double.
    let float = obj.extract::<f64>().map_err(|_| inexact_int_error())?;
    if !float.is_finite() {
        return Err(inexact_int_error());
    }
    let roundtrip = py
        .get_type::<PyInt>()
        .call1((float,))
        .map_err(|_| inexact_int_error())?;
    if !roundtrip.eq(obj).unwrap_or(false) {
        return Err(inexact_int_error());
    }
    let number = Number::from_f64(float).ok_or_else(inexact_int_error)?;
    Ok(Value::Number(number))
}

fn py_to_json(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    depth: usize,
    max_depth: usize,
) -> PyResult<Value> {
    if depth > max_depth {
        return Err(database_error(format!(
            "Portable JSON nesting exceeds {MAX_JSON_DEPTH}"
        )));
    }
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(s) = obj.cast::<PyString>() {
        let text = s.to_str().map_err(|_| {
            database_error("Portable JSON strings must contain valid Unicode scalar values")
        })?;
        return Ok(Value::String(text.to_owned()));
    }
    // Check `bool` before `int` because `bool` subclasses `int` in Python.
    if let Ok(b) = obj.cast::<PyBool>() {
        return Ok(Value::Bool(b.is_true()));
    }
    if let Ok(n) = obj.cast::<PyInt>() {
        return py_int_to_json(py, n);
    }
    if let Ok(f) = obj.cast::<PyFloat>() {
        let value = f.value();
        if !value.is_finite() {
            return Err(database_error(
                "Expected finite JSON values: Out of range float values are not JSON compliant",
            ));
        }
        let number = Number::from_f64(value).ok_or_else(|| {
            database_error(
                "Expected finite JSON values: Out of range float values are not JSON compliant",
            )
        })?;
        return Ok(Value::Number(number));
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let next_depth = depth + 1;
        let mut map = Map::new();
        for (key, value) in dict.iter() {
            if next_depth > max_depth {
                return Err(database_error(format!(
                    "Portable JSON nesting exceeds {MAX_JSON_DEPTH}"
                )));
            }
            let Ok(key_str) = key.cast::<PyString>() else {
                return Err(database_error("JSON object keys must be strings"));
            };
            let key_text = key_str.to_str().map_err(|_| {
                database_error("Portable JSON strings must contain valid Unicode scalar values")
            })?;
            let converted = py_to_json(py, &value, next_depth, max_depth)?;
            map.insert(key_text.to_owned(), converted);
        }
        return Ok(Value::Object(map));
    }
    if let Ok(list) = obj.cast::<PyList>() {
        let next_depth = depth + 1;
        let mut items = Vec::with_capacity(list.len());
        for item in list.iter() {
            items.push(py_to_json(py, &item, next_depth, max_depth)?);
        }
        return Ok(Value::Array(items));
    }
    if let Ok(tuple) = obj.cast::<PyTuple>() {
        let next_depth = depth + 1;
        let mut items = Vec::with_capacity(tuple.len());
        for item in tuple.iter() {
            items.push(py_to_json(py, &item, next_depth, max_depth)?);
        }
        return Ok(Value::Array(items));
    }
    let type_name = obj
        .get_type()
        .name()
        .map(|name| name.to_string())
        .unwrap_or_else(|_| "unknown".into());
    Err(database_error(format!(
        "Expected finite JSON values: Object of type {type_name} is not JSON serializable"
    )))
}

fn json_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(PyBool::new(py, *b).to_owned().into_any().unbind()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any().unbind())
            } else if let Some(u) = n.as_u64() {
                Ok(u.into_pyobject(py)?.into_any().unbind())
            } else {
                let f = n.as_f64().ok_or_else(|| {
                    database_error("Native engine returned a non-finite JSON number")
                })?;
                Ok(PyFloat::new(py, f).into_any().unbind())
            }
        }
        Value::String(s) => Ok(PyString::new(py, s).into_any().unbind()),
        Value::Array(items) => {
            let mut py_items = Vec::with_capacity(items.len());
            for item in items {
                py_items.push(json_to_py(py, item)?);
            }
            Ok(PyList::new(py, py_items)?.into_any().unbind())
        }
        Value::Object(map) => {
            let dict = PyDict::new(py);
            for (key, val) in map {
                dict.set_item(key, json_to_py(py, val)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

#[pyclass]
struct NativeBatch {
    documents: Vec<Value>,
}

#[pymethods]
impl NativeBatch {
    #[new]
    fn new() -> Self {
        Self {
            documents: Vec::new(),
        }
    }

    fn push(&mut self, py: Python<'_>, record: &Bound<'_, PyAny>) -> PyResult<()> {
        // Snapshot at each yield so caller mutations cannot affect queued records.
        // Record envelope is depth 0; payload starts at depth 1 (so max payload
        // depth 64 reaches depth 65).
        let value = py_to_json(py, record, 0, MAX_JSON_DEPTH + 1)?;
        self.documents.push(value);
        Ok(())
    }

    fn __len__(&self) -> usize {
        self.documents.len()
    }
}

#[pyclass]
struct Engine {
    inner: Mutex<NativeEngine>,
}

#[pymethods]
impl Engine {
    #[new]
    #[pyo3(signature = (path, module_bytes, reset = false))]
    fn new(py: Python<'_>, path: String, module_bytes: Vec<u8>, reset: bool) -> PyResult<Self> {
        let inner = py
            .detach(move || NativeEngine::open_with_options(&path, &module_bytes, reset))
            .map_err(|err| PyValueError::new_err(("DatabaseError", err)))?;
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }

    #[staticmethod]
    fn migrate_log(py: Python<'_>, source: String, destination: String) -> PyResult<()> {
        py.detach(move || NativeEngine::migrate_log(&source, &destination))
            .map_err(|err| PyValueError::new_err(("DatabaseError", err)))
    }

    fn execute(&self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let request_value = py_to_json(py, request, 0, MAX_TRANSPORT_DEPTH)?;
        let response = py
            .detach(|| {
                let mut engine = self
                    .inner
                    .lock()
                    .map_err(|_| EngineError::database("Native engine lock was poisoned"))?;
                engine.execute_typed(request_value)
            })
            .map_err(|err| PyValueError::new_err((err.kind.as_str(), err.message)))?;
        json_to_py(py, &response)
    }

    fn execute_batch(
        &self,
        py: Python<'_>,
        op: &str,
        batch: &mut NativeBatch,
    ) -> PyResult<Py<PyAny>> {
        let documents = std::mem::take(&mut batch.documents);
        let mut request = Map::new();
        request.insert("op".into(), Value::String(op.into()));
        request.insert("documents".into(), Value::Array(documents));
        let response = py
            .detach(|| {
                let mut engine = self
                    .inner
                    .lock()
                    .map_err(|_| EngineError::database("Native engine lock was poisoned"))?;
                engine.execute_typed(Value::Object(request))
            })
            .map_err(|err| PyValueError::new_err((err.kind.as_str(), err.message)))?;
        json_to_py(py, &response)
    }

    fn cursor(&self, py: Python<'_>) -> PyResult<String> {
        py.detach(|| {
            let engine = self
                .inner
                .lock()
                .map_err(|_| EngineError::database("Native engine lock was poisoned"))?;
            engine.current_cursor().map_err(EngineError::database)
        })
        .map_err(|err| PyValueError::new_err((err.kind.as_str(), err.message)))
    }

    fn execute_json(&self, py: Python<'_>, request: &str) -> PyResult<String> {
        py.detach(|| {
            if request.len() as u64 > MAX_CACHE_BYTES {
                return Err(EngineError::database("Native request exceeds 1 GiB"));
            }
            let request =
                serde_json::from_str(request).map_err(|e| EngineError::database(e.to_string()))?;
            let mut engine = self
                .inner
                .lock()
                .map_err(|_| EngineError::database("Native engine lock was poisoned"))?;
            let response = engine.execute_typed(request)?;
            serde_json::to_string(&response).map_err(|e| EngineError::database(e.to_string()))
        })
        .map_err(|err| PyValueError::new_err((err.kind.as_str(), err.message)))
    }
}

#[pyclass]
struct NativeReducer {
    inner: Mutex<ReducerInstance>,
}

#[pymethods]
impl NativeReducer {
    #[new]
    #[pyo3(signature = (module_bytes, fuel = None))]
    fn new(py: Python<'_>, module_bytes: Vec<u8>, fuel: Option<u64>) -> PyResult<Self> {
        let fuel = fuel.unwrap_or(50_000_000);
        let inner = py
            .detach(move || ReducerInstance::new(&module_bytes, fuel))
            .map_err(PyValueError::new_err)?;
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }

    fn execute_json(&self, py: Python<'_>, request: &str) -> PyResult<String> {
        py.detach(|| {
            let mut reducer = self
                .inner
                .lock()
                .map_err(|_| "Reducer lock was poisoned".to_owned())?;
            let output = reducer.execute_json(request.as_bytes())?;
            String::from_utf8(output).map_err(|e| e.to_string())
        })
        .map_err(PyValueError::new_err)
    }

    #[getter]
    fn memory_bytes(&self) -> PyResult<usize> {
        let reducer = self
            .inner
            .lock()
            .map_err(|_| PyValueError::new_err("Reducer lock was poisoned"))?;
        Ok(reducer.memory_bytes())
    }

    #[getter]
    fn module_sha256(&self) -> PyResult<String> {
        let reducer = self
            .inner
            .lock()
            .map_err(|_| PyValueError::new_err("Reducer lock was poisoned"))?;
        Ok(reducer.module_sha256().to_string())
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Engine>()?;
    module.add_class::<NativeBatch>()?;
    module.add_class::<NativeReducer>()?;
    module.add("PORTABLE_ABI_VERSION", 1)?;
    Ok(())
}
