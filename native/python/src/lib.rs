//! Python owns ergonomic objects; the engine owns transactions and Wasm execution.
use foldlitedb_portable_engine::{Engine as NativeEngine, ReducerInstance};
use pyo3::{exceptions::PyValueError, prelude::*};
use std::sync::Mutex;

#[pyclass]
struct Engine {
    inner: Mutex<NativeEngine>,
}

#[pymethods]
impl Engine {
    #[new]
    fn new(py: Python<'_>, path: String, module_bytes: Vec<u8>) -> PyResult<Self> {
        let inner = py
            .detach(move || NativeEngine::open(&path, &module_bytes))
            .map_err(PyValueError::new_err)?;
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }
    fn execute_json(&self, py: Python<'_>, request: String) -> PyResult<String> {
        py.detach(|| {
            if request.len() > 8 * 1024 * 1024 {
                return Err("Native request exceeds 8 MiB".to_owned());
            }
            let request = serde_json::from_str(&request).map_err(|e| e.to_string())?;
            let mut engine = self
                .inner
                .lock()
                .map_err(|_| "Native engine lock was poisoned".to_owned())?;
            let response = engine.execute(request)?;
            serde_json::to_string(&response).map_err(|e| e.to_string())
        })
        .map_err(PyValueError::new_err)
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

    fn execute_json(&self, py: Python<'_>, request: String) -> PyResult<String> {
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
    module.add_class::<NativeReducer>()?;
    module.add("PORTABLE_ABI_VERSION", 1)?;
    Ok(())
}
