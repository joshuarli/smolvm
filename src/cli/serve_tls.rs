//! mTLS for the fleet serve API (control↔node, increment 3 of the mTLS plan).
//!
//! In a fleet, the worker runs `smolvm serve` and is driven by the control
//! plane. That channel must be mutually authenticated: the node presents a
//! CA-signed **server** cert, and only a client presenting a CA-signed **client**
//! cert (the control plane) may connect. This module builds the rustls
//! `ServerConfig` that enforces `require_and_verify_client_cert`.
//!
//! **Fail-closed:** when `SMOLVM_SERVE_REQUIRE_MTLS=1` the serve API refuses to
//! start without TLS configured — it must never fall back to plain HTTP / the
//! interim bearer token when the deploy declared it should be mTLS-protected.
//! This is a DEDICATED opt-in, deliberately NOT keyed off `SMOLVM_PUBLISH_ADDR`
//! (which every worker sets for the published-port datapath — overloading it
//! would make plain-HTTP workers refuse to boot).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use rustls_pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};

/// Env var holding the node's PEM **server** cert (signed by the node-CA).
const ENV_CERT: &str = "SMOLVM_SERVE_TLS_CERT";
/// Env var holding the node's PEM private key.
const ENV_KEY: &str = "SMOLVM_SERVE_TLS_KEY";
/// Env var holding the PEM node-CA cert used to verify the control's client cert.
const ENV_CLIENT_CA: &str = "SMOLVM_SERVE_TLS_CLIENT_CA";
/// Dedicated opt-in: the deploy declares this serve API MUST run mTLS. When set,
/// a missing/partial cert config is fatal (fail-closed) rather than a silent
/// fall-back to plain HTTP. NOT keyed off `SMOLVM_PUBLISH_ADDR` (see module doc).
const ENV_REQUIRE_MTLS: &str = "SMOLVM_SERVE_REQUIRE_MTLS";

/// True when the deploy has declared this serve API must be mTLS-protected.
pub fn require_mtls() -> bool {
    matches!(
        std::env::var(ENV_REQUIRE_MTLS).ok().as_deref(),
        Some("1") | Some("true")
    )
}

/// Loopback plain-HTTP address for the **local** node-agent when the main port
/// runs mTLS. mTLS locks the whole network port to CA-signed clients, but the
/// node's own agent polls `/capacity` locally over plain HTTP — so we open a
/// second door bound to loopback only (unreachable from the network). Defaults
/// to `127.0.0.1:<main_port + 1>`; override with `SMOLVM_SERVE_LOCAL_ADDR`.
/// Returns `None` only if an override is set but unparseable.
pub fn local_plain_addr(main: std::net::SocketAddr) -> Option<std::net::SocketAddr> {
    match std::env::var("SMOLVM_SERVE_LOCAL_ADDR")
        .ok()
        .filter(|v| !v.is_empty())
    {
        Some(v) => v.parse().ok(),
        None => Some(std::net::SocketAddr::from((
            std::net::Ipv4Addr::LOCALHOST,
            main.port().wrapping_add(1),
        ))),
    }
}

fn env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// Resolve the serve API's TLS posture from the environment.
///
/// - All three TLS env vars set ⇒ `Ok(Some(config))` (mTLS, client cert required).
/// - None set, mTLS not required ⇒ `Ok(None)` (plain HTTP, local/dev/non-mTLS worker).
/// - **`SMOLVM_SERVE_REQUIRE_MTLS` set but TLS not fully configured ⇒ `Err` (fail-closed).**
/// - A partial config (some but not all vars) ⇒ `Err` (misconfiguration).
pub fn resolve_tls() -> Result<Option<Arc<ServerConfig>>, String> {
    let cert = env_path(ENV_CERT);
    let key = env_path(ENV_KEY);
    let client_ca = env_path(ENV_CLIENT_CA);

    match (cert, key, client_ca) {
        (Some(cert), Some(key), Some(client_ca)) => {
            Ok(Some(build_server_config(&cert, &key, &client_ca)?))
        }
        (None, None, None) => {
            if require_mtls() {
                Err(format!(
                    "{ENV_REQUIRE_MTLS} is set but {ENV_CERT}/{ENV_KEY}/{ENV_CLIENT_CA} are unset — \
                     refusing to start without client-cert verification (fail-closed)"
                ))
            } else {
                Ok(None)
            }
        }
        _ => Err(format!(
            "incomplete serve TLS config: set all of {ENV_CERT}, {ENV_KEY}, {ENV_CLIENT_CA} (or none)"
        )),
    }
}

/// Build a rustls server config that requires + verifies a client cert chained
/// to the node-CA.
fn build_server_config(
    cert_path: &Path,
    key_path: &Path,
    client_ca_path: &Path,
) -> Result<Arc<ServerConfig>, String> {
    // Pin the Graviola provider explicitly rather than relying on a
    // process-global install — avoids ordering hazards if anything else
    // touches rustls and keeps ring out of the host binary.
    let provider = Arc::new(rustls_graviola::default_provider());

    // Client-cert trust anchor: the node-CA. Only the control plane holds a
    // client cert signed by it.
    let mut roots = RootCertStore::empty();
    for ca in CertificateDer::pem_file_iter(client_ca_path)
        .map_err(|e| format!("read client CA {}: {e}", client_ca_path.display()))?
    {
        let ca = ca.map_err(|e| format!("parse client CA cert: {e}"))?;
        roots
            .add(ca)
            .map_err(|e| format!("add client CA to root store: {e}"))?;
    }
    if roots.is_empty() {
        return Err(format!(
            "client CA {} contained no certificates",
            client_ca_path.display()
        ));
    }
    let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|e| format!("build client-cert verifier: {e}"))?;

    // Our server identity (node server cert + key).
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
        .map_err(|e| format!("read server cert {}: {e}", cert_path.display()))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("parse server cert chain: {e}"))?;
    if certs.is_empty() {
        return Err(format!(
            "server cert {} contained no certificates",
            cert_path.display()
        ));
    }
    let key = PrivateKeyDer::from_pem_file(key_path)
        .map_err(|e| format!("read server key {}: {e}", key_path.display()))?;

    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("rustls protocol versions: {e}"))?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|e| format!("install server cert/key: {e}"))?;
    // axum-server speaks h2 + http/1.1; advertise both via ALPN.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    // resolve_tls reads process env; these run serially via a shared guard to
    // avoid cross-test interference on the shared env vars.
    fn lock() -> std::sync::MutexGuard<'static, ()> {
        static L: std::sync::Mutex<()> = std::sync::Mutex::new(());
        L.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn clear() {
        for v in [ENV_CERT, ENV_KEY, ENV_CLIENT_CA, ENV_REQUIRE_MTLS] {
            std::env::remove_var(v);
        }
    }

    #[test]
    fn no_env_not_required_is_plain_http() {
        let _g = lock();
        clear();
        assert!(resolve_tls().unwrap().is_none());
    }

    #[test]
    fn require_mtls_without_tls_fails_closed() {
        let _g = lock();
        clear();
        std::env::set_var(ENV_REQUIRE_MTLS, "1");
        let err = resolve_tls().unwrap_err();
        assert!(err.contains("fail-closed"), "{err}");
        clear();
    }

    #[test]
    fn local_plain_addr_defaults_to_loopback_port_plus_one() {
        let _g = lock();
        std::env::remove_var("SMOLVM_SERVE_LOCAL_ADDR");
        let main: std::net::SocketAddr = "0.0.0.0:8080".parse().unwrap();
        let local = local_plain_addr(main).unwrap();
        assert!(local.ip().is_loopback());
        assert_eq!(local.port(), 8081);
    }

    #[test]
    fn local_plain_addr_honors_override() {
        let _g = lock();
        std::env::set_var("SMOLVM_SERVE_LOCAL_ADDR", "127.0.0.1:9999");
        let local = local_plain_addr("0.0.0.0:8080".parse().unwrap()).unwrap();
        assert_eq!(local.port(), 9999);
        std::env::remove_var("SMOLVM_SERVE_LOCAL_ADDR");
    }

    #[test]
    fn publish_addr_alone_does_not_force_mtls() {
        // A worker sets SMOLVM_PUBLISH_ADDR for the datapath; that must NOT make
        // a plain-HTTP serve refuse to start.
        let _g = lock();
        clear();
        std::env::set_var("SMOLVM_PUBLISH_ADDR", "0.0.0.0");
        assert!(resolve_tls().unwrap().is_none());
        std::env::remove_var("SMOLVM_PUBLISH_ADDR");
    }

    #[test]
    fn partial_tls_config_is_rejected() {
        let _g = lock();
        clear();
        std::env::set_var(ENV_CERT, "/tmp/x.crt");
        let err = resolve_tls().unwrap_err();
        assert!(err.contains("incomplete"), "{err}");
        clear();
    }
}
