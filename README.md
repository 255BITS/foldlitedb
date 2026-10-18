# Foldlite DB

Embedded realtime views over immutable documents.

Foldlite DB builds versioned map/reduce views in a local, disposable database.
Applications retain their source documents and view definitions; derived storage
can be deleted and regenerated. HyperGAN is the first integrated consumer, and a
catalogue example exercises structured documents and ordered lifecycle folds.

The core uses Python's SQLite binding. The optional Rust/WebAssembly reducers
share one mathematical implementation between Python and browser workers.

```sh
python -m pip install '.[reducers,test]'
python -m pytest
```

The implementation and API are under active development. See [DESIGN.md](DESIGN.md)
for the contracts and current scope.
