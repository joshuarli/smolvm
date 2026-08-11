//! Minimal boot helper for embedded SDKs.
//!
//! This binary deliberately exposes only smolvm's internal `_boot-vm` entry
//! point. It is launched by `EmbeddedRuntime` in a fresh single-threaded
//! process before `krun_start_enter` blocks, avoiding a dependency on the full
//! smolvm CLI or its daemon command surface.

use std::path::PathBuf;

fn main() {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let command = arguments.next();
    let config = arguments.next().map(PathBuf::from);
    if command.as_deref() != Some(std::ffi::OsStr::new("_boot-vm"))
        || config.is_none()
        || arguments.next().is_some()
    {
        eprintln!("usage: smolvm-boot _boot-vm <boot-config.json>");
        std::process::exit(64);
    }

    if let Err(error) = smolvm::boot_helper::run(config.expect("checked above")) {
        eprintln!("smolvm boot helper: {error}");
        std::process::exit(1);
    }
}
