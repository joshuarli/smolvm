//! Brokered peer-to-peer layer-blob fetch using h12tiny and node mTLS.

use crate::cache::BlobCache;
use crate::pull::{stream_verify_adopt, PullResult};
use crate::{RegistryError, Result};
use futures_util::StreamExt;
use h12tiny::client::Client;
use h12tiny::runtime::BoxExecutor;
use h12tiny::util::{self, BodyExt, BoxBody, IdleTimeoutBody};
use http::{Method, Request, StatusCode};
use std::path::Path;
use std::time::Duration;

const ENV_CERT: &str = "SMOLVM_SERVE_TLS_CERT";
const ENV_KEY: &str = "SMOLVM_SERVE_TLS_KEY";
const ENV_CLIENT_CA: &str = "SMOLVM_SERVE_TLS_CLIENT_CA";
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_READ_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_ERROR_BYTES: usize = 64 * 1024;

pub(crate) type PeerClient = Client<BoxBody>;

pub(crate) fn peer_client(
    cache: &std::sync::Mutex<Option<PeerClient>>,
    executor: BoxExecutor,
) -> Option<PeerClient> {
    if let Ok(cached) = cache.lock() {
        if let Some(client) = cached.as_ref() {
            return Some(client.clone());
        }
    }
    match build_peer_client(executor) {
        Ok(client) => {
            if let Ok(mut cached) = cache.lock() {
                *cached = Some(client.clone());
            }
            Some(client)
        }
        Err(error) => {
            tracing::warn!(
                error = %error,
                "P2P blob fetch disabled: could not build node->node mTLS client"
            );
            None
        }
    }
}

fn build_peer_client(executor: BoxExecutor) -> std::result::Result<PeerClient, String> {
    use futures_rustls::pki_types::CertificateDer;

    let cert = read_env_file(ENV_CERT)?;
    let key = read_env_file(ENV_KEY)?;
    let ca = read_env_file(ENV_CLIENT_CA)?;
    let certs = parse_certs(&cert, ENV_CERT)?;
    let key = parse_key(&key, ENV_KEY)?;
    let ca_certs = parse_certs(&ca, ENV_CLIENT_CA)?;

    let mut roots = rustls::RootCertStore::empty();
    for certificate in ca_certs {
        roots
            .add(CertificateDer::from(certificate.as_ref().to_vec()))
            .map_err(|error| format!("add node CA from {ENV_CLIENT_CA}: {error}"))?;
    }
    let provider = std::sync::Arc::new(rustls_graviola::default_provider());
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| format!("configure node mTLS protocol versions: {error}"))?
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|error| format!("build node mTLS client config: {error}"))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    let connector = h12tiny::client::Connector::builder()
        .connect_timeout(PEER_CONNECT_TIMEOUT)
        .tls_config(config)
        .build();
    let mut builder = Client::builder(executor);
    builder.connector(connector);
    Ok(builder.build())
}

fn parse_certs(data: &[u8], name: &str) -> std::result::Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
    let mut reader = data;
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| format!("parse certificates from {name}: {error}"))
}

fn parse_key(data: &[u8], name: &str) -> std::result::Result<rustls::pki_types::PrivateKeyDer<'static>, String> {
    let mut reader = data;
    rustls_pemfile::private_key(&mut reader)
        .map_err(|error| format!("parse private key from {name}: {error}"))?
        .ok_or_else(|| format!("no private key found in {name}"))
}

fn read_env_file(name: &str) -> std::result::Result<Vec<u8>, String> {
    let path = std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{name} is unset"))?;
    std::fs::read(&path).map_err(|error| format!("read {name} ({}): {error}", Path::new(&path).display()))
}

pub(crate) async fn fetch_blob_from_peers(
    client: &PeerClient,
    peers: &[String],
    digest: &str,
    output: Option<&Path>,
    cache: &BlobCache,
    executor: std::sync::Arc<dyn crate::RegistryExecutor>,
) -> Option<PullResult> {
    let partial_path = cache.blob_path_for(digest).with_extension("partial");
    for peer in peers {
        match fetch_one(client, peer, digest, output, cache, executor.clone()).await {
            Ok(result) => {
                tracing::info!(peer = %peer, digest = %digest, "fetched layer blob from peer (P2P)");
                return Some(result);
            }
            Err(error) => {
                tracing::warn!(peer = %peer, digest = %digest, error = %error, "P2P peer fetch failed; trying next source");
                let _ = crate::blocking_io::remove_file(&executor, &partial_path).await;
            }
        }
    }
    None
}

async fn fetch_one(
    client: &PeerClient,
    peer: &str,
    digest: &str,
    output: Option<&Path>,
    cache: &BlobCache,
    executor: std::sync::Arc<dyn crate::RegistryExecutor>,
) -> Result<PullResult> {
    let url = format!("{}/p2p/blob/{digest}", peer.trim_end_matches('/'));
    let request = Request::builder()
        .method(Method::GET)
        .uri(url)
        .body(util::boxed_body(util::empty_body()))
        .map_err(|error| RegistryError::Http(error.to_string()))?;
    let response = client
        .request(request)
        .await
        .map_err(|error| RegistryError::Http(error.to_string()))?;
    if response.status() == StatusCode::NOT_FOUND {
        return Err(RegistryError::BlobNotFound(digest.to_string()));
    }
    if !response.status().is_success() {
        let status = response.status().as_u16();
        let body = util::collect_bytes_limited(
            IdleTimeoutBody::new(response.into_body(), PEER_READ_TIMEOUT),
            MAX_ERROR_BYTES,
        )
            .await
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_default();
        return Err(RegistryError::ApiError { status, body });
    }
    let body = IdleTimeoutBody::new(response.into_body(), PEER_READ_TIMEOUT)
        .into_data_stream()
        .map(|chunk| chunk.map_err(|error| RegistryError::Http(error.to_string())));
    stream_verify_adopt(body, digest, output, cache, executor).await
}
