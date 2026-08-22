//! OCI Distribution HTTP client.

#[path = "client_impl.rs"]
mod migrated;

pub use migrated::{validate_digest, RegistryClient};
