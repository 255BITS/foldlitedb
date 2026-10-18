//! Foldlite DB's portable reducer library.
//!
//! [`kernel`] exposes typed, pure Rust computation. [`protocol`] applies the
//! bounded JSON protocol used identically by Python and browser WASM hosts.
pub mod kernel;
pub mod protocol;

#[cfg(target_arch = "wasm32")]
mod wasm {
    use super::protocol::{execute_json, INPUT_BYTES, OUTPUT_BYTES};

    static mut INPUT: [u8; INPUT_BYTES] = [0; INPUT_BYTES];
    static mut OUTPUT: [u8; OUTPUT_BYTES] = [0; OUTPUT_BYTES];

    #[no_mangle]
    pub extern "C" fn abi_version() -> u32 {
        1
    }
    #[no_mangle]
    pub extern "C" fn input_ptr() -> *mut u8 {
        std::ptr::addr_of_mut!(INPUT).cast()
    }
    #[no_mangle]
    pub extern "C" fn output_ptr() -> *const u8 {
        std::ptr::addr_of!(OUTPUT).cast()
    }
    #[no_mangle]
    pub extern "C" fn input_capacity() -> u32 {
        INPUT_BYTES as u32
    }
    #[no_mangle]
    pub extern "C" fn execute(length: u32) -> u32 {
        let bytes = if length as usize > INPUT_BYTES {
            br#"{"error":"request exceeds 262144 bytes"}"#.to_vec()
        } else {
            // Hosts serialize requests to an instance and may access only its
            // exported buffers. No borrowed kernel state survives this call.
            execute_json(unsafe { std::slice::from_raw_parts(input_ptr(), length as usize) })
        };
        if bytes.len() > OUTPUT_BYTES {
            return 0;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                std::ptr::addr_of_mut!(OUTPUT).cast(),
                bytes.len(),
            );
        }
        bytes.len() as u32
    }
}
