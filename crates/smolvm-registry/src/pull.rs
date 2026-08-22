//! Pull a `.smolmachine` artifact from an OCI registry.
//!
//! The pull flow:
//! 1. Fetch the OCI manifest by tag or digest
//! 2. Parse the manifest to find the layer blob digest
//! 3. Check the local cache for the blob
//! 4. If not cached, try any brokered P2P peers, then stream from the registry,
//!    computing the digest while writing
//! 5. Verify the digest and adopt into cache

use crate::cache::BlobCache;
use crate::client::RegistryClient;
use crate::blocking_io::BlockingFile;
use crate::{OciManifest, RegistryError, Result, LAYER_MEDIA_TYPE};
use futures_util::StreamExt;
use futures_lite::io::AsyncWriteExt;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Result of a successful pull.
#[derive(Debug)]
pub struct PullResult {
    /// Path to the downloaded `.smolmachine` file.
    pub path: PathBuf,
    /// Digest of the layer blob.
    pub digest: String,
    /// Size of the layer blob in bytes.
    pub size: u64,
    /// Whether the blob was served from local cache.
    pub cached: bool,
}

/// Pull a `.smolmachine` artifact from the registry.
///
/// `repo` is the OCI repository path (e.g., "python-dev").
/// `reference` is the tag or digest (e.g., "latest" or "sha256:abc...").
/// If `output` is Some, the blob is copied there. Otherwise it's only cached.
///
/// `blob_peers` are optional brokered P2P peers (node base URLs, e.g.
/// `https://<addr>:<port>`) supplied by the control plane. On a cache miss the
/// layer blob is fetched from a peer's `GET /p2p/blob/<digest>` (over node→node
/// mTLS) before the registry. When `blob_peers` is empty the peer block is
/// skipped entirely and the registry path is byte-for-byte what it was before.
pub async fn pull(
    client: &RegistryClient,
    repo: &str,
    reference: &str,
    output: Option<&Path>,
    cache: &BlobCache,
    blob_peers: &[String],
) -> Result<PullResult> {
    // 1. Fetch the manifest, resolving a multi-platform index to this machine's
    //    host-platform entry (Docker-style fan-out). Shared with `inspect`.
    tracing::info!(repo = %repo, reference = %reference, "fetching manifest...");
    let manifest_bytes = client.get_manifest_resolved(repo, reference).await?;

    let manifest: OciManifest = serde_json::from_slice(&manifest_bytes)?;

    // 2. Find the smolmachine layer.
    let layer = manifest
        .layers
        .iter()
        .find(|l| l.media_type == LAYER_MEDIA_TYPE)
        .ok_or_else(|| {
            RegistryError::InvalidManifest(format!(
                "no layer with media type {} in manifest",
                LAYER_MEDIA_TYPE
            ))
        })?;

    let digest = &layer.digest;
    let size = layer.size;

    // Validate the digest format BEFORE it is used to build any cache filesystem
    // path. `BlobCache::blob_path` only does `digest.replace(':', "_")`, leaving
    // `/` and `..` intact, so an attacker-controlled manifest digest could
    // otherwise create a `.partial` file or touch atime outside the cache dir.
    crate::client::validate_digest(digest)?;

    // 3. Check cache.
    if let Some(cached_path) = cache.get(digest) {
        tracing::info!(digest = %digest, "blob found in cache");

        if let Some(out) = output {
            crate::blocking_io::copy(&client.blocking_executor(), &cached_path, out).await?;
        }

        return Ok(PullResult {
            path: output.map(PathBuf::from).unwrap_or(cached_path),
            digest: digest.clone(),
            size,
            cached: true,
        });
    }

    // 4. Brokered P2P: try sibling nodes before hitting the registry. Inert when
    //    `blob_peers` is empty — the registry path below is then reached
    //    byte-for-byte as before. Peers arrive only from the control plane, and
    //    a node with no serve-TLS identity has no peer client, so this is also a
    //    no-op there.
    if !blob_peers.is_empty() {
        let peer = client.peer_client();
        if let Some(peer_client) = peer {
            if let Some(result) =
                crate::peer::fetch_blob_from_peers(
                    &peer_client,
                    blob_peers,
                    digest,
                    output,
                    cache,
                    client.blocking_executor(),
                )
                .await
            {
                return Ok(result);
            }
            tracing::info!(digest = %digest, "no peer served the blob; falling back to registry");
        }
    }

    // 5. Stream blob from the registry to disk, resuming and retrying a broken
    //    transfer, then verify the digest and adopt into cache.
    tracing::info!(digest = %digest, size, "downloading blob...");

    let result = download_with_resume(client, repo, digest, size, output, cache).await?;

    tracing::info!(digest = %digest, size = result.size, "pull complete");

    Ok(result)
}

/// Attempts allowed for one blob (first try plus retries).
const MAX_ATTEMPTS: u32 = 5;

/// Delay before the first retry; doubles each attempt.
const RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// Download `digest` into the cache, resuming a partial file across attempts.
///
/// Each attempt appends to `<digest>.partial` from wherever the last one stopped,
/// so a 90 MiB layer that breaks at 80 MiB resumes at 80 MiB instead of starting
/// over — the difference between converging and never finishing when a registry
/// drops long transfers. Retries are bounded and backed off, so a genuinely
/// broken blob fails in seconds with a real error instead of hammering the
/// registry forever (the retry storm that hid the original fault).
///
/// The digest is verified from the finished file rather than hashed incrementally
/// while streaming: resumed attempts would otherwise need the hasher state from a
/// previous attempt. Re-reading is cheap next to the transfer, and it verifies
/// exactly the bytes that landed on disk.
async fn download_with_resume(
    client: &RegistryClient,
    repo: &str,
    digest: &str,
    expected_size: u64,
    output: Option<&Path>,
    cache: &BlobCache,
) -> Result<PullResult> {
    let partial_path = cache.blob_path_for(digest).with_extension("partial");

    for attempt in 1..=MAX_ATTEMPTS {
        // Resume from whatever a previous attempt left behind. A stale partial
        // from an earlier pull is fine: the digest check at the end rejects it,
        // and a mismatch clears it so the next pull starts clean.
        let have = std::fs::metadata(&partial_path).map(|m| m.len()).unwrap_or(0);

        match fetch_into_partial(client, repo, digest, have, expected_size, &partial_path).await {
            Ok(written) => {
                let actual = hash_file(&partial_path).await?;
                if actual != *digest {
                    // Corrupt or mismatched bytes: remove them so a retry cannot
                    // resume onto a poisoned prefix, and fail — retrying the same
                    // content would only reproduce the mismatch.
                    if let Err(e) = std::fs::remove_file(&partial_path) {
                        tracing::warn!(
                            error = %e,
                            path = %partial_path.display(),
                            "failed to clean up partial blob after digest mismatch"
                        );
                    }
                    return Err(RegistryError::DigestMismatch {
                        expected: digest.to_string(),
                        actual,
                    });
                }

                let cached_path = cache.adopt(digest, written)?;
                let result_path = if let Some(out) = output {
                    std::fs::copy(&cached_path, out)?;
                    PathBuf::from(out)
                } else {
                    cached_path
                };
                return Ok(PullResult {
                    path: result_path,
                    digest: digest.to_string(),
                    size: written,
                    cached: false,
                });
            }
            Err(e) if attempt < MAX_ATTEMPTS && is_retryable(&e) => {
                let backoff = RETRY_BACKOFF * 2u32.pow(attempt - 1);
                tracing::warn!(
                    digest = %digest,
                    attempt,
                    max_attempts = MAX_ATTEMPTS,
                    have_bytes = have,
                    backoff_ms = backoff.as_millis(),
                    error = %e,
                    "blob download failed; resuming after backoff"
                );
                async_io::Timer::after(backoff).await;
            }
            Err(e) => return Err(e),
        }
    }

    unreachable!("loop returns on the final attempt")
}

/// One download attempt: append the blob to `partial_path` starting at `have`.
///
/// Returns the total bytes on disk when the body ends.
async fn fetch_into_partial(
    client: &RegistryClient,
    repo: &str,
    digest: &str,
    have: u64,
    expected_size: u64,
    partial_path: &Path,
) -> Result<u64> {
    let (stream, resumed_at) = client.pull_blob_stream_from(repo, digest, have).await?;

    // `resumed_at == 0` with bytes already on disk means the registry ignored the
    // Range header and is resending the whole blob, so the partial must be
    // truncated — appending would produce a file with a duplicated prefix.
    let mut file = if resumed_at > 0 {
        std::fs::OpenOptions::new().append(true).open(partial_path)?
    } else { std::fs::File::create(partial_path)? };

    let mut written = resumed_at;
    let mut stream = std::pin::pin!(stream);
    while let Some(chunk_result) = stream.next().await {
        let chunk: bytes::Bytes = chunk_result.map_err(RegistryError::Http)?;
        std::io::Write::write_all(&mut file, &chunk)?;
        written += chunk.len() as u64;
    }
    std::io::Write::flush(&mut file)?;

    // A body that ends early is a truncated transfer, not a complete download.
    // Without this the short file would reach the digest check, mismatch, and be
    // deleted — throwing away bytes a resume could have kept.
    if expected_size > 0 && written < expected_size {
        return Err(RegistryError::DownloadStalled {
            digest: digest.to_string(),
            received: written,
            total: expected_size,
        });
    }

    Ok(written)
}

/// SHA-256 of a file on disk, read in chunks so a large layer is never buffered.
async fn hash_file(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 128 * 1024];
    loop {
        let n = std::io::Read::read(&mut file, &mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}

/// Whether a failed attempt is worth resuming.
///
/// Transport faults and 5xx are transient — exactly the mid-transfer breakage
/// resume exists for. A missing blob, an auth failure, or a digest mismatch are
/// terminal: retrying re-fetches the same wrong answer and only delays the error
/// the caller needs to see.
fn is_retryable(err: &RegistryError) -> bool {
    match err {
        RegistryError::Http(message) => {
            // h12tiny deliberately erases transport errors to a String. Keep
            // retry behavior for recognizable transient failures, while not
            // retrying arbitrary protocol/authentication messages.
            let message = message.to_ascii_lowercase();
            [
                "timeout", "timed out", "connect", "connection reset",
                "connection refused", "broken pipe", "unexpected eof",
                "early eof", "body", "decode", "transport",
            ]
            .iter()
            .any(|needle| message.contains(needle))
        }
        RegistryError::DownloadStalled { .. } => true,
        RegistryError::Io(_) => false,
        RegistryError::ApiError { status, .. } => *status >= 500,
        _ => false,
    }
}

/// Stream `stream` into the cache's `.partial` file for `digest`, hashing while
/// writing, verify the digest, adopt into the cache, and copy to `output` if
/// requested.
///
/// Shared by the registry pull path and the P2P peer-fetch path so a blob
/// obtained either way goes through identical digest verification and the same
/// LRU/size accounting ([`BlobCache::adopt`]). On a digest mismatch the
/// `.partial` is removed before returning the error. A mid-stream transport
/// error propagates as-is (leaving the `.partial`, which the next attempt
/// truncates via `File::create`), matching the original registry pull behavior.
pub(crate) async fn stream_verify_adopt<S, E>(
    stream: S,
    digest: &str,
    output: Option<&Path>,
    cache: &BlobCache,
    executor: std::sync::Arc<dyn crate::RegistryExecutor>,
) -> Result<PullResult>
where
    S: futures_util::Stream<Item = std::result::Result<bytes::Bytes, E>>,
    E: std::fmt::Display,
{
    let partial_path = cache.blob_path_for(digest).with_extension("partial");
    let mut file = BlockingFile::create(executor.clone(), &partial_path).await?;
    let mut hasher = Sha256::new();
    let mut total_bytes: u64 = 0;

    let mut stream = std::pin::pin!(stream);
    while let Some(chunk_result) = stream.next().await {
        let chunk: bytes::Bytes = chunk_result.map_err(|error| RegistryError::Http(error.to_string()))?;
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
        total_bytes += chunk.len() as u64;
    }
    file.flush().await?;
    drop(file);

    // Verify digest.
    let actual = format!("sha256:{}", hex::encode(hasher.finalize()));
    if actual != *digest {
        if let Err(e) = crate::blocking_io::remove_file(&executor, &partial_path).await {
            tracing::warn!(
                error = %e,
                path = %partial_path.display(),
                "failed to clean up partial blob after digest mismatch"
            );
        }
        return Err(RegistryError::DigestMismatch {
            expected: digest.to_string(),
            actual,
        });
    }

    // Adopt into cache (handles eviction + atomic rename).
    let cached_path = cache.adopt(digest, total_bytes)?;

    // Copy to output if requested.
    let result_path = if let Some(out) = output {
        crate::blocking_io::copy(&executor, &cached_path, out).await?;
        PathBuf::from(out)
    } else {
        cached_path
    };

    Ok(PullResult {
        path: result_path,
        digest: digest.to_string(),
        size: total_bytes,
        cached: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CONFIG_MEDIA_TYPE, MANIFEST_MEDIA_TYPE};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
    use std::thread::JoinHandle;
    use std::time::Duration;

    struct TestResponse {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    impl TestResponse {
        fn new(status: u16, body: Vec<u8>) -> Self {
            Self {
                status,
                headers: Vec::new(),
                body,
            }
        }

        fn header(mut self, name: &str, value: impl Into<String>) -> Self {
            self.headers.push((name.to_string(), value.into()));
            self
        }
    }

    struct TestServer {
        address: SocketAddr,
        stop: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl TestServer {
        fn start<F, H>(factory: F) -> Self
        where
            F: FnOnce(String) -> H,
            H: Fn(&str, &str, Option<&str>) -> TestResponse + Send + Sync + 'static,
        {
            let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let uri = format!("http://{address}");
            let handler = Arc::new(factory(uri));
            let stop = Arc::new(AtomicBool::new(false));
            let thread_stop = stop.clone();
            let thread = std::thread::spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    let Ok((mut stream, _)) = listener.accept() else {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    };
                    loop {
                        let Ok((method, path, range)) = read_request(&mut stream) else {
                            if thread_stop.load(Ordering::Acquire) {
                                break;
                            }
                            continue;
                        };
                        let response = handler(&method, &path, range.as_deref());
                        if write_response(&mut stream, response).is_err() {
                            break;
                        }
                    }
                }
            });
            Self {
                address,
                stop,
                thread: Some(thread),
            }
        }

        fn uri(&self) -> String {
            format!("http://{}", self.address)
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            let _ = TcpStream::connect(self.address);
            if let Some(thread) = self.thread.take() {
                thread.join().unwrap();
            }
        }
    }

    fn read_request(stream: &mut TcpStream) -> std::io::Result<(String, String, Option<String>)> {
        stream.set_read_timeout(Some(Duration::from_millis(100)))?;
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            if request.len() > 64 * 1024 {
                break;
            }
        }
        let request_text = String::from_utf8_lossy(&request);
        let line = request_text
            .lines()
            .next()
            .unwrap_or_default();
        let mut parts = line.split_whitespace();
        let range = request_text.lines().find_map(|line| {
            line.split_once(':').and_then(|(name, value)| {
                name.eq_ignore_ascii_case("range")
                    .then(|| value.trim().to_string())
            })
        });
        Ok((
            parts.next().unwrap_or_default().to_string(),
            parts.next().unwrap_or_default().to_string(),
            range,
        ))
    }

    fn write_response(stream: &mut TcpStream, response: TestResponse) -> std::io::Result<()> {
        let reason = match response.status {
            200 => "OK",
            404 => "Not Found",
            _ => "Response",
        };
        write!(
            stream,
            "HTTP/1.1 {} {}\r\n",
            response.status,
            reason
        )?;
        if !response
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("Content-Length"))
        {
            write!(stream, "Content-Length: {}\r\n", response.body.len())?;
        }
        for (name, value) in response.headers {
            write!(stream, "{name}: {value}\r\n")?;
        }
        stream.write_all(b"\r\n")?;
        stream.write_all(&response.body)
    }

    /// With empty `blob_peers`, `pull` takes the registry path exactly as before:
    /// fetch the manifest, then stream the layer blob from `/v2/.../blobs/...`,
    /// verify, and adopt. No peer client is built or consulted.
    #[test]
    fn empty_blob_peers_uses_registry_path() {
        use sha2::{Digest, Sha256};

        let data = b"registry-path-layer-bytes".to_vec();
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(&data)));
        let config_digest =
            "sha256:1111111111111111111111111111111111111111111111111111111111111111";

        let manifest = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": MANIFEST_MEDIA_TYPE,
            "config": { "mediaType": CONFIG_MEDIA_TYPE, "digest": config_digest, "size": 2 },
            "layers": [ { "mediaType": LAYER_MEDIA_TYPE, "digest": digest, "size": data.len() } ],
        });

        let manifest_body = serde_json::to_vec(&manifest).unwrap();
        let manifest_path = "/v2/myrepo/manifests/latest".to_string();
        let blob_path = format!("/v2/myrepo/blobs/{digest}");
        let expected_size = data.len();
        let server = TestServer::start(move |_uri| {
            let manifest_body = manifest_body.clone();
            let blob_path = blob_path.clone();
            let data = data.clone();
            move |method, path, _range| match (method, path) {
                ("GET", path) if path == manifest_path => TestResponse::new(200, manifest_body.clone())
                    .header("Content-Type", MANIFEST_MEDIA_TYPE),
                ("GET", path) if path == blob_path => TestResponse::new(200, data.clone()),
                _ => TestResponse::new(404, Vec::new()),
            }
        });

        let tmp = tempfile::tempdir().unwrap();
        let cache = BlobCache::open(tmp.path().to_path_buf(), 1024 * 1024).unwrap();
        let client = RegistryClient::new_for_tests(server.uri());

        let result = futures_lite::future::block_on(pull(
            &client,
            "myrepo",
            "latest",
            None,
            &cache,
            &[],
        ))
        .expect("registry pull must succeed");

        assert_eq!(result.digest, digest);
        assert_eq!(result.size, expected_size as u64);
        assert!(!result.cached);
        assert!(
            cache.get(&digest).is_some(),
            "blob must be adopted into cache"
        );
        // MockServer drop asserts the blob endpoint's expect(1) was satisfied.
    }

    /// A broken response is resumed with an HTTP Range request and never
    /// duplicated onto the partial file.
    #[test]
    fn broken_transfer_resumes_with_range() {
        let data: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(&data)));
        let split = 1500;
        let manifest = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": MANIFEST_MEDIA_TYPE,
            "config": {"mediaType": CONFIG_MEDIA_TYPE, "digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111", "size": 2},
            "layers": [{"mediaType": LAYER_MEDIA_TYPE, "digest": digest, "size": data.len()}],
        });
        let manifest_body = serde_json::to_vec(&manifest).unwrap();
        let blob_path = format!("/v2/myrepo/blobs/{digest}");
        let manifest_path = "/v2/myrepo/manifests/latest".to_string();
        let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server = TestServer::start({
            let requests = requests.clone();
            let data = data.clone();
            let manifest_body = manifest_body.clone();
            let blob_path = blob_path.clone();
            move |_uri| move |method, path, range| {
                if method == "GET" && path == blob_path {
                    let n = requests.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        return TestResponse::new(200, data[..split].to_vec());
                    }
                    assert_eq!(range.map(str::to_owned), Some(format!("bytes={split}-")));
                    return TestResponse::new(206, data[split..].to_vec());
                }
                match (method, path) {
                    ("GET", path) if path == manifest_path => TestResponse::new(200, manifest_body.clone()).header("Content-Type", MANIFEST_MEDIA_TYPE),
                    _ => TestResponse::new(404, Vec::new()),
                }
            }
        });
        let tmp = tempfile::tempdir().unwrap();
        let cache = BlobCache::open(tmp.path().to_path_buf(), 1024 * 1024).unwrap();
        let client = RegistryClient::new_for_tests(server.uri());
        let result = futures_lite::future::block_on(pull(&client, "myrepo", "latest", None, &cache, &[])).unwrap();
        assert_eq!(result.size, data.len() as u64);
        assert_eq!(std::fs::read(cache.get(&digest).unwrap()).unwrap(), data);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    /// Retry classification: transient transport/5xx faults resume, terminal ones
    /// do not. This is what keeps a broken pull from becoming a retry storm.
    #[test]
    fn retry_classification_splits_transient_from_terminal() {
        assert!(is_retryable(&RegistryError::DownloadStalled {
            digest: "sha256:x".into(),
            received: 1,
            total: 2,
        }));
        assert!(is_retryable(&RegistryError::ApiError {
            status: 503,
            body: String::new(),
        }));
        assert!(!is_retryable(&RegistryError::ApiError {
            status: 404,
            body: String::new(),
        }));
        assert!(!is_retryable(&RegistryError::BlobNotFound("x".into())));
        assert!(!is_retryable(&RegistryError::DigestMismatch {
            expected: "a".into(),
            actual: "b".into(),
        }));
    }
}
