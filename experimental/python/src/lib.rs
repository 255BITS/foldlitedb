//! Thin optional Python boundary; all storage and projection work is native.
use foldlitedb_engine::Engine as NativeEngine;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::sync::Mutex;

#[pyclass]
struct Engine {
    inner: Mutex<NativeEngine>,
}

#[pymethods]
impl Engine {
    #[new]
    fn new(
        py: Python<'_>,
        path: String,
        backend: String,
        specification: String,
        workers: usize,
    ) -> PyResult<Self> {
        let inner = py
            .detach(move || {
                let spec = serde_json::from_str(&specification).map_err(|e| e.to_string())?;
                NativeEngine::open(&path, &backend, spec, workers)
            })
            .map_err(PyValueError::new_err)?;
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }

    fn execute_json(&self, py: Python<'_>, request: String) -> PyResult<String> {
        py.detach(|| {
            let request = serde_json::from_str(&request).map_err(|e| e.to_string())?;
            let mut engine = self
                .inner
                .lock()
                .map_err(|_| "Native engine lock was poisoned".to_string())?;
            let response = engine.execute(request)?;
            serde_json::to_string(&response).map_err(|e| e.to_string())
        })
        .map_err(PyValueError::new_err)
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Engine>()?;
    module.add("EXPERIMENTAL", true)?;
    module.add(
        "ENGINE_SOURCE_SHA256",
        env!("FOLDLITEDB_ENGINE_SOURCE_SHA256"),
    )?;
    module.add(
        "STORAGE_SOURCE_SHA256",
        env!("FOLDLITEDB_STORAGE_SOURCE_SHA256"),
    )?;
    module.add(
        "BINDING_SOURCE_SHA256",
        env!("FOLDLITEDB_BINDING_SOURCE_SHA256"),
    )?;
    module.add(
        "KERNEL_SOURCE_SHA256",
        env!("FOLDLITEDB_KERNEL_SOURCE_SHA256"),
    )?;
    module.add(
        "KERNEL_MANIFEST_SHA256",
        env!("FOLDLITEDB_KERNEL_MANIFEST_SHA256"),
    )?;
    // Retain a distinct runtime error type for consumers detecting build/load issues.
    module.add(
        "NativeRuntimeError",
        module.py().get_type::<PyRuntimeError>(),
    )?;
    Ok(())
}
