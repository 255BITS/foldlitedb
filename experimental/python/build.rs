use sha2::{Digest, Sha256};
use std::{env, fs, path::PathBuf};

fn main() {
    let directory = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    for (name, relative) in [
        ("ENGINE_SOURCE", "../engine/src/lib.rs"),
        ("STORAGE_SOURCE", "../engine/src/storage.rs"),
        ("BINDING_SOURCE", "src/lib.rs"),
        ("KERNEL_SOURCE", "../../reducers/core/src/kernel.rs"),
        ("KERNEL_MANIFEST", "../../reducers/core/Cargo.toml"),
    ] {
        let path = directory.join(relative);
        println!("cargo:rerun-if-changed={}", path.display());
        let data = fs::read(&path).expect("native source must exist when building wheel");
        println!(
            "cargo:rustc-env=FOLDLITEDB_{name}_SHA256={:x}",
            Sha256::digest(data)
        );
    }
}
