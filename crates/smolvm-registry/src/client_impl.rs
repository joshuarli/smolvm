//! Runtime-neutral OCI Distribution HTTP client implementation.

use crate::{
    OciIndex, OciPlatform, RegistryError, Result, INDEX_MEDIA_TYPE, MANIFEST_MEDIA_TYPE,
};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use h12tiny::client::Client;
use h12tiny::runtime::BoxExecutor;
#[cfg(test)]
use h12tiny::runtime::BoxSendFuture;
use h12tiny::util::{self, BodyCollectionError, BodyExt, BoxBody, IdleTimeoutBody};
use http::header::{
    HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, LINK,
    LOCATION, RANGE, WWW_AUTHENTICATE,
};
use http::{Method, Request, Response, StatusCode, Uri};
use hyper::body::Incoming;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use url::Url;

/// Maximum bytes buffered in memory for a manifest / image-index document.
const MAX_MANIFEST_BYTES: usize = 32 * 1024 * 1024;
/// Maximum bytes buffered by the small-blob API.
const MAX_BLOB_BYTES: usize = 64 * 1024 * 1024;
/// Error responses are bounded separately from successful layer streams.
const MAX_ERROR_BYTES: usize = 64 * 1024;
/// A response body may not remain idle between frames longer than this.
pub(crate) const BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Bound TCP/DNS/TLS connection establishment.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Bound registry/CDN hops when a blob is served from a redirecting backend.
const MAX_BLOB_REDIRECTS: usize = 5;

type HttpResponse = Response<Incoming>;
type BlobStream = std::pin::Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
struct TestExecutor;

#[cfg(test)]
impl hyper::rt::Executor<BoxSendFuture> for TestExecutor {
    fn execute(&self, future: BoxSendFuture) {
        std::thread::spawn(|| futures_lite::future::block_on(future));
    }
}

#[cfg(test)]
impl crate::RegistryExecutor for TestExecutor {
    fn execute(&self, future: crate::BoxSendFuture) {
        std::thread::spawn(|| futures_lite::future::block_on(future));
    }

    fn submit_blocking(
        &self,
        job: crate::BoxBlockingJob,
    ) -> std::result::Result<crate::BoxBlockingFuture, crate::BlockingSubmitError> {
        let worker = std::thread::spawn(job);
        Ok(Box::pin(async move {
            match worker.join() {
                Ok(value) => Ok(value),
                Err(_) => Err(crate::BlockingTaskError::Panicked),
            }
        }))
    }
}

/// Buffer a response body while enforcing a hard byte cap and per-frame idle
/// deadline. The cap is checked from Content-Length before body polling and is
/// enforced again by `h12tiny-util` for chunked or dishonest responses.
async fn read_body_capped(resp: HttpResponse, cap: usize, what: &str) -> Result<Vec<u8>> {
    if let Some(len) = resp
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
    {
        if len > cap as u64 {
            return Err(RegistryError::ResponseTooLarge {
                what: what.to_string(),
                limit: cap,
            });
        }
    }

    let body = IdleTimeoutBody::new(resp.into_body(), BODY_IDLE_TIMEOUT);
    match util::collect_bytes_limited(body, cap).await {
        Ok(bytes) => Ok(bytes.to_vec()),
        Err(BodyCollectionError::LimitExceeded(_)) => Err(RegistryError::ResponseTooLarge {
            what: what.to_string(),
            limit: cap,
        }),
        Err(error) => Err(RegistryError::Http(error.to_string())),
    }
}

async fn response_error(resp: HttpResponse) -> RegistryError {
    let status = resp.status().as_u16();
    let body = read_body_capped(resp, MAX_ERROR_BYTES, "error response")
        .await
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_default();
    RegistryError::ApiError { status, body }
}

fn transport_error(error: impl Display) -> RegistryError {
    RegistryError::Http(error.to_string())
}

/// Validate that a digest string matches the expected `sha256:<64 hex chars>` format.
pub fn validate_digest(digest: &str) -> Result<()> {
    if let Some(hex_part) = digest.strip_prefix("sha256:") {
        if hex_part.len() == 64 && hex_part.chars().all(|c| c.is_ascii_hexdigit()) {
            return Ok(());
        }
    }
    Err(RegistryError::InvalidManifest(format!(
        "invalid digest format: {digest}"
    )))
}

/// HTTP client for an OCI Distribution registry.
pub struct RegistryClient {
    http: Client<BoxBody>,
    executor: BoxExecutor,
    blocking_executor: Arc<dyn crate::RegistryExecutor>,
    peer_client: Mutex<Option<crate::peer::PeerClient>>,
    base_url: String,
    auth_token: Option<String>,
    identity_token: Option<String>,
    basic_credentials: Option<(String, String)>,
    token_cache: Mutex<HashMap<TokenCacheKey, CachedToken>>,
    last_challenge: Mutex<Option<BearerChallenge>>,
}

impl RegistryClient {
    /// Constructs a transport-only client from a pre-erased h12 executor.
    ///
    /// File-backed push/pull operations require [`Self::new_with_executor`]
    /// because this constructor has no application-owned blocking boundary;
    /// those operations return a descriptive [`RegistryError::Blocking`]
    /// error when called on this legacy path.
    pub fn new(base_url: String, executor: BoxExecutor) -> Self {
        Self::new_parts(base_url, executor, Arc::new(UnavailableExecutor))
    }

    fn new_parts(
        base_url: String,
        executor: BoxExecutor,
        blocking_executor: Arc<dyn crate::RegistryExecutor>,
    ) -> Self {
        let connector = h12tiny::client::Connector::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build();
        let mut builder = Client::builder(executor.clone());
        builder.connector(connector);
        Self {
            http: builder.build(),
            executor,
            blocking_executor,
            peer_client: Mutex::new(None),
            base_url,
            auth_token: None,
            identity_token: None,
            basic_credentials: None,
            token_cache: Mutex::new(HashMap::new()),
            last_challenge: Mutex::new(None),
        }
    }

    /// Construct a client from an application-owned executor handle.
    pub fn new_with_executor<E>(base_url: String, executor: E) -> Self
    where
        E: crate::RegistryExecutor,
    {
        let blocking_executor: Arc<dyn crate::RegistryExecutor> = Arc::new(executor);
        Self::new_parts(
            base_url,
            BoxExecutor::new(RegistryExecutorAdapter(blocking_executor.clone())),
            blocking_executor,
        )
    }

    #[cfg(test)]
    pub fn new_for_tests(base_url: String) -> Self {
        Self::new_with_executor(base_url, TestExecutor)
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub(crate) fn peer_client(&self) -> Option<crate::peer::PeerClient> {
        crate::peer::peer_client(&self.peer_client, self.executor.clone())
    }

    pub(crate) fn blocking_executor(&self) -> Arc<dyn crate::RegistryExecutor> {
        self.blocking_executor.clone()
    }

    pub fn identity_token(&self) -> Option<&str> {
        self.identity_token.as_deref()
    }

    pub fn with_token(mut self, token: String) -> Self {
        self.auth_token = Some(token);
        self
    }

    pub fn with_identity_token(mut self, token: String) -> Self {
        self.identity_token = Some(token);
        self
    }

    pub fn with_basic_credentials(mut self, username: String, password: String) -> Self {
        self.basic_credentials = Some((username, password));
        self
    }

    pub fn basic_credentials(&self) -> Option<(&str, &str)> {
        self.basic_credentials
            .as_ref()
            .map(|(username, password)| (username.as_str(), password.as_str()))
    }

    pub async fn ping(&self) -> Result<()> {
        let response = self
            .send_replayable(self.template(Method::GET, &format!("{}/v2/", self.base_url))?, || {
                empty_body()
            })
            .await?;
        if !response.status().is_success() {
            return Err(response_error(response).await);
        }
        Ok(())
    }

    pub async fn blob_exists(&self, repo: &str, digest: &str) -> Result<bool> {
        validate_digest(digest)?;
        let response = self
            .send_replayable(
                self.template(
                    Method::HEAD,
                    &format!("{}/v2/{repo}/blobs/{digest}", self.base_url),
                )?,
                || empty_body(),
            )
            .await?;
        Ok(response.status() == StatusCode::OK)
    }

    pub async fn push_blob(&self, repo: &str, data: &[u8]) -> Result<String> {
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(data)));
        if self.blob_exists(repo, &digest).await? {
            tracing::debug!(digest = %digest, "blob already exists, skipping upload");
            return Ok(digest);
        }

        let mut post = self.template(
            Method::POST,
            &format!("{}/v2/{repo}/blobs/uploads/", self.base_url),
        )?;
        set_header(&mut post, CONTENT_LENGTH, "0")?;
        let response = self.send_replayable(post, || empty_body()).await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(response_error(response).await);
        }
        let put_url = self.upload_location(&response)?;
        let put_url = append_digest(&put_url, &digest);
        let mut put = self.template(Method::PUT, &put_url)?;
        set_header(&mut put, CONTENT_TYPE, "application/octet-stream")?;
        set_header(&mut put, CONTENT_LENGTH, &data.len().to_string())?;
        let bytes = Bytes::copy_from_slice(data);
        let response = self
            .send_replayable(put, move || bytes_body(bytes.clone()))
            .await?;
        if response.status() != StatusCode::CREATED {
            return Err(response_error(response).await);
        }
        Ok(digest)
    }

    pub async fn pull_blob(&self, repo: &str, digest: &str) -> Result<Vec<u8>> {
        validate_digest(digest)?;
        let response = self
            .send_blob_get(self.template(
                Method::GET,
                &format!("{}/v2/{repo}/blobs/{digest}", self.base_url),
            )?)
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(RegistryError::BlobNotFound(digest.to_string()));
        }
        if !response.status().is_success() {
            return Err(response_error(response).await);
        }
        let data = read_body_capped(response, MAX_BLOB_BYTES, "blob").await?;
        let actual = format!("sha256:{}", hex::encode(Sha256::digest(&data)));
        if actual != digest {
            return Err(RegistryError::DigestMismatch {
                expected: digest.to_string(),
                actual,
            });
        }
        Ok(data)
    }

    /// Upload a blob from a replayable body factory. The factory is invoked for
    /// each authentication attempt, so a file-backed stream can be reopened.
    pub async fn push_blob_stream<F, Fut>(
        &self,
        repo: &str,
        digest: &str,
        size: u64,
        make_body: F,
    ) -> Result<()>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<BoxBody>>,
    {
        validate_digest(digest)?;
        if self.blob_exists(repo, digest).await? {
            tracing::debug!(digest = %digest, "blob already exists, skipping upload");
            return Ok(());
        }

        let mut post = self.template(
            Method::POST,
            &format!("{}/v2/{repo}/blobs/uploads/", self.base_url),
        )?;
        set_header(&mut post, CONTENT_LENGTH, "0")?;
        let response = self.send_replayable(post, || empty_body()).await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(response_error(response).await);
        }
        let put_url = append_digest(&self.upload_location(&response)?, digest);
        let mut put = self.template(Method::PUT, &put_url)?;
        set_header(&mut put, CONTENT_TYPE, "application/octet-stream")?;
        set_header(&mut put, CONTENT_LENGTH, &size.to_string())?;
        let response = self.send_nonreplayable(put, make_body).await?;
        if response.status() != StatusCode::CREATED {
            return Err(response_error(response).await);
        }
        Ok(())
    }

    /// Upload a blob in OCI chunked mode, keeping each PATCH bounded.
    pub async fn push_blob_chunked(
        &self,
        repo: &str,
        digest: &str,
        path: &Path,
        chunk_size: usize,
    ) -> Result<()> {
        use futures_lite::io::AsyncReadExt;

        validate_digest(digest)?;
        if self.blob_exists(repo, digest).await? {
            tracing::debug!(digest = %digest, "blob already exists, skipping upload");
            return Ok(());
        }

        let mut post = self.template(
            Method::POST,
            &format!("{}/v2/{repo}/blobs/uploads/", self.base_url),
        )?;
        set_header(&mut post, CONTENT_LENGTH, "0")?;
        let response = self.send_replayable(post, || empty_body()).await?;
        if response.status() != StatusCode::ACCEPTED {
            return Err(response_error(response).await);
        }
        let mut location = self.upload_location(&response)?;
        let mut file = crate::blocking_io::BlockingFile::open(self.blocking_executor(), path).await?;
        let mut buffer = vec![0; chunk_size.max(1)];
        let mut offset = 0_u64;

        loop {
            let mut filled = 0;
            while filled < buffer.len() {
                let read = file.read(&mut buffer[filled..]).await?;
                if read == 0 {
                    break;
                }
                filled += read;
            }
            if filled == 0 {
                break;
            }
            let start = offset;
            let end = offset + filled as u64 - 1;
            let mut patch = self.template(Method::PATCH, &location)?;
            set_header(&mut patch, CONTENT_TYPE, "application/octet-stream")?;
            set_header(&mut patch, CONTENT_RANGE, &format!("{start}-{end}"))?;
            set_header(&mut patch, CONTENT_LENGTH, &filled.to_string())?;
            let chunk = Bytes::copy_from_slice(&buffer[..filled]);
            let response = self
                .send_replayable(patch, move || bytes_body(chunk.clone()))
                .await?;
            if response.status() != StatusCode::ACCEPTED {
                return Err(response_error(response).await);
            }
            location = self.upload_location(&response)?;
            offset += filled as u64;
        }

        let mut put = self.template(Method::PUT, &append_digest(&location, digest))?;
        set_header(&mut put, CONTENT_LENGTH, "0")?;
        let response = self.send_replayable(put, || empty_body()).await?;
        if response.status() != StatusCode::CREATED {
            return Err(response_error(response).await);
        }
        Ok(())
    }

    pub async fn pull_blob_stream(&self, repo: &str, digest: &str) -> Result<BlobStream> {
        self.pull_blob_stream_from(repo, digest, 0).await.map(|(stream, _)| stream)
    }

    /// Fetch a blob, optionally resuming at `offset`. The returned offset is
    /// zero when the registry ignored the range request and sent the full blob.
    pub async fn pull_blob_stream_from(
        &self,
        repo: &str,
        digest: &str,
        offset: u64,
    ) -> Result<(BlobStream, u64)> {
        validate_digest(digest)?;
        let mut request = self.template(
            Method::GET,
            &format!("{}/v2/{repo}/blobs/{digest}", self.base_url),
        )?;
        if offset > 0 {
            set_header(&mut request, RANGE, &format!("bytes={offset}-"))?;
        }
        let response = self.send_blob_get(request).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(RegistryError::BlobNotFound(digest.to_string()));
        }
        if !response.status().is_success() {
            return Err(response_error(response).await);
        }
        let resumed = if offset > 0 && response.status() == StatusCode::PARTIAL_CONTENT { offset } else { 0 };
        let body = IdleTimeoutBody::new(response.into_body(), BODY_IDLE_TIMEOUT);
        Ok((body
            .into_data_stream()
            .map(|chunk| chunk.map_err(transport_error))
            .boxed(), resumed))
    }

    pub async fn put_manifest(&self, repo: &str, reference: &str, manifest: &[u8]) -> Result<()> {
        let media_type = serde_json::from_slice::<serde_json::Value>(manifest)
            .ok()
            .and_then(|value| {
                value
                    .get("mediaType")
                    .and_then(|media_type| media_type.as_str())
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| MANIFEST_MEDIA_TYPE.to_string());
        let mut request = self.template(
            Method::PUT,
            &format!("{}/v2/{repo}/manifests/{reference}", self.base_url),
        )?;
        set_header(&mut request, CONTENT_TYPE, &media_type)?;
        set_header(&mut request, CONTENT_LENGTH, &manifest.len().to_string())?;
        let bytes = Bytes::copy_from_slice(manifest);
        let response = self
            .send_replayable(request, move || bytes_body(bytes.clone()))
            .await?;
        if !response.status().is_success() {
            return Err(response_error(response).await);
        }
        Ok(())
    }

    pub async fn get_manifest(&self, repo: &str, reference: &str) -> Result<Vec<u8>> {
        let mut request = self.template(
            Method::GET,
            &format!("{}/v2/{repo}/manifests/{reference}", self.base_url),
        )?;
        set_header(&mut request, ACCEPT, MANIFEST_MEDIA_TYPE)?;
        let response = self.send_replayable(request, || empty_body()).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(RegistryError::BlobNotFound(format!("{repo}:{reference}")));
        }
        if !response.status().is_success() {
            return Err(response_error(response).await);
        }
        let content_type = header_string(&response, CONTENT_TYPE);
        if content_type.contains(INDEX_MEDIA_TYPE)
            || content_type.contains("application/vnd.docker.distribution.manifest.list.v2+json")
        {
            return Err(RegistryError::InvalidManifest(
                "OCI image indexes (multi-arch manifests) are not supported; this reference points to a Docker image, not a .smolmachine artifact".into(),
            ));
        }
        read_body_capped(response, MAX_MANIFEST_BYTES, "manifest").await
    }

    pub async fn get_manifest_raw(&self, repo: &str, reference: &str) -> Result<(Vec<u8>, String)> {
        let mut request = self.template(
            Method::GET,
            &format!("{}/v2/{repo}/manifests/{reference}", self.base_url),
        )?;
        set_header(&mut request, ACCEPT, &format!("{INDEX_MEDIA_TYPE}, {MANIFEST_MEDIA_TYPE}"))?;
        let response = self.send_replayable(request, || empty_body()).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(RegistryError::BlobNotFound(format!("{repo}:{reference}")));
        }
        if !response.status().is_success() {
            return Err(response_error(response).await);
        }
        let content_type = header_string(&response, CONTENT_TYPE);
        let data = read_body_capped(response, MAX_MANIFEST_BYTES, "manifest").await?;
        if reference.starts_with("sha256:") {
            let actual = format!("sha256:{}", hex::encode(Sha256::digest(&data)));
            if actual != reference {
                return Err(RegistryError::DigestMismatch {
                    expected: reference.to_string(),
                    actual,
                });
            }
        }
        Ok((data, content_type))
    }

    pub async fn get_manifest_resolved(&self, repo: &str, reference: &str) -> Result<Vec<u8>> {
        let (document, content_type) = self.get_manifest_raw(repo, reference).await?;
        let is_index = content_type.contains(INDEX_MEDIA_TYPE)
            || serde_json::from_slice::<serde_json::Value>(&document)
                .ok()
                .map(|value| {
                    value.get("manifests").is_some()
                        || value.get("mediaType").and_then(|m| m.as_str()) == Some(INDEX_MEDIA_TYPE)
                })
                .unwrap_or(false);
        if !is_index {
            return Ok(document);
        }
        let index: OciIndex = serde_json::from_slice(&document)?;
        let arch = OciPlatform::current().architecture;
        let entry = index
            .manifests
            .iter()
            .find(|manifest| {
                manifest
                    .platform
                    .as_ref()
                    .is_some_and(|platform| platform.os == "linux" && platform.architecture == arch)
            })
            .ok_or_else(|| {
                let available: Vec<String> = index
                    .manifests
                    .iter()
                    .filter_map(|manifest| manifest.platform.as_ref().map(|platform| platform.label()))
                    .collect();
                RegistryError::InvalidManifest(format!(
                    "no linux/{arch} build available for this machine; the registry has: {}",
                    if available.is_empty() { "(none)".into() } else { available.join(", ") }
                ))
            })?;
        Ok(self.get_manifest_raw(repo, &entry.digest).await?.0)
    }

    pub async fn list_repositories(&self) -> Result<Vec<String>> {
        #[derive(serde::Deserialize)]
        struct Catalog {
            #[serde(default)]
            repositories: Vec<String>,
        }

        let mut next = Some(format!("{}/v2/_catalog", self.base_url));
        let mut repositories = Vec::new();
        for _ in 0..1000 {
            let Some(url) = next.take() else { break };
            let request = self.template(Method::GET, &url)?;
            let response = self.send_replayable(request, || empty_body()).await?;
            if !response.status().is_success() {
                return Err(response_error(response).await);
            }
            next = response
                .headers()
                .get(LINK)
                .and_then(|value| value.to_str().ok())
                .and_then(Self::parse_next_link)
                .map(|link: String| self.resolve_location(&link))
                .transpose()?;
            let data = read_body_capped(response, MAX_MANIFEST_BYTES, "catalog").await?;
            repositories.extend(serde_json::from_slice::<Catalog>(&data)?.repositories);
        }
        Ok(repositories)
    }

    pub async fn list_tags(&self, repo: &str) -> Result<Vec<String>> {
        #[derive(serde::Deserialize)]
        struct TagList {
            #[serde(default)]
            tags: Vec<String>,
        }
        let response = self
            .send_replayable(
                self.template(Method::GET, &format!("{}/v2/{repo}/tags/list", self.base_url))?,
                || empty_body(),
            )
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(RegistryError::BlobNotFound(repo.to_string()));
        }
        if !response.status().is_success() {
            return Err(response_error(response).await);
        }
        let data = read_body_capped(response, MAX_MANIFEST_BYTES, "tags").await?;
        Ok(serde_json::from_slice::<TagList>(&data)?.tags)
    }

    fn parse_next_link(header: &str) -> Option<String> {
        for entry in header.split(',') {
            let mut parts = entry.split(';');
            let Some(url_part) = parts.next() else { continue };
            let is_next = parts.any(|part| {
                let part = part.trim().to_ascii_lowercase();
                part == "rel=\"next\"" || part == "rel=next"
            });
            if is_next {
                let url = url_part.trim().trim_start_matches('<').trim_end_matches('>');
                if !url.is_empty() {
                    return Some(url.to_string());
                }
            }
        }
        None
    }

    fn resolve_location(&self, location: &str) -> Result<String> {
        let base = Url::parse(&self.base_url).map_err(|error| RegistryError::ApiError {
            status: 202,
            body: format!("client base URL is not valid: {error}"),
        })?;
        let resolved = if location.starts_with("http://") || location.starts_with("https://") {
            Url::parse(location).map_err(|error| RegistryError::ApiError {
                status: 202,
                body: format!("Location is not a valid URL '{location}': {error}"),
            })?
        } else {
            base.join(location).map_err(|error| RegistryError::ApiError {
                status: 202,
                body: format!("Location is not a valid relative path '{location}': {error}"),
            })?
        };
        if resolved.origin() != base.origin() {
            return Err(RegistryError::ApiError {
                status: 202,
                body: format!(
                    "Location points to a different origin ('{}'), expected '{}'",
                    resolved.origin().unicode_serialization(),
                    base.origin().unicode_serialization()
                ),
            });
        }
        Ok(resolved.to_string())
    }

    fn registry_host(&self) -> String {
        Url::parse(&self.base_url)
            .ok()
            .and_then(|url| url.host_str().map(str::to_owned))
            .unwrap_or_else(|| self.base_url.clone())
    }

    fn template(&self, method: Method, url: &str) -> Result<Request<()>> {
        let uri = url.parse::<Uri>().map_err(transport_error)?;
        Request::builder()
            .method(method)
            .uri(uri)
            .body(())
            .map_err(transport_error)
    }

    fn upload_location(&self, response: &HttpResponse) -> Result<String> {
        let location = response
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| RegistryError::ApiError {
                status: 202,
                body: "upload step accepted but missing Location header".into(),
            })?;
        self.resolve_location(location)
    }

    /// Pulls may be redirected from a registry API host to a signed CDN URL
    /// (Docker Hub commonly returns 307 for blobs). Redirects are followed
    /// without forwarding the registry's bearer token to the new origin.
    async fn send_blob_get(&self, template: Request<()>) -> Result<HttpResponse> {
        let mut current = template.uri().to_string();
        let range = template.headers().get(RANGE).cloned();
        let mut response = self.send_replayable(template, || empty_body()).await?;
        for _ in 0..MAX_BLOB_REDIRECTS {
            if !response.status().is_redirection() {
                return Ok(response);
            }
            let Some(location) = response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
            else {
                return Ok(response);
            };
            let next = self.resolve_blob_location(&current, location)?;
            drop(response);
            let mut request = self.template(Method::GET, &next)?;
            if let Some(range) = &range {
                request.headers_mut().insert(RANGE, range.clone());
            }
            response = self
                .dispatch_raw(request, empty_body()?)
                .await?;
            current = next;
        }
        Err(RegistryError::ApiError {
            status: 310,
            body: format!("blob redirect limit exceeded after {MAX_BLOB_REDIRECTS} hops"),
        })
    }

    fn resolve_blob_location(&self, current: &str, location: &str) -> Result<String> {
        let base = Url::parse(current).map_err(|error| RegistryError::ApiError {
            status: 307,
            body: format!("current blob URL is not valid: {error}"),
        })?;
        let resolved = base.join(location).map_err(|error| RegistryError::ApiError {
            status: 307,
            body: format!("blob redirect Location is not a valid URL '{location}': {error}"),
        })?;
        if resolved.scheme() != "https"
            && !crate::is_local_registry(resolved.host_str().unwrap_or_default())
        {
            return Err(RegistryError::ApiError {
                status: 307,
                body: format!("blob redirect must use HTTPS: {resolved}"),
            });
        }
        Ok(resolved.to_string())
    }

    async fn dispatch(&self, mut request: Request<()>, body: BoxBody) -> Result<HttpResponse> {
        if request.headers().get(AUTHORIZATION).is_none() {
            let token = self.auth_token.clone().or_else(|| self.preemptive_token());
            if let Some(token) = token.as_deref() {
                set_header(&mut request, AUTHORIZATION, &format!("Bearer {token}"))?;
            }
        }
        self.dispatch_raw(request, body).await
    }

    async fn dispatch_raw(&self, request: Request<()>, body: BoxBody) -> Result<HttpResponse> {
        let (parts, _) = request.into_parts();
        self.http
            .request(Request::from_parts(parts, body))
            .await
            .map_err(transport_error)
    }

    async fn send_replayable<F>(&self, template: Request<()>, make_body: F) -> Result<HttpResponse>
    where
        F: Fn() -> Result<BoxBody>,
    {
        let response = self.dispatch(template.clone(), make_body()?).await?;
        if response.status() != StatusCode::UNAUTHORIZED {
            return Ok(response);
        }
        if self.auth_token.is_some() {
            return Ok(response);
        }
        let challenge = match response
            .headers()
            .get(WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok())
        {
            Some(value) => BearerChallenge::parse(value)?,
            None => return Ok(response),
        };
        drop(response);
        let token = self.get_token(&challenge, false).await?;
        let mut retry = template.clone();
        set_header(&mut retry, AUTHORIZATION, &format!("Bearer {token}"))?;
        let response = self.dispatch(retry, make_body()?).await?;
        if response.status() != StatusCode::UNAUTHORIZED {
            return Ok(response);
        }
        drop(response);
        let token = self.get_token(&challenge, true).await?;
        let mut retry = template;
        set_header(&mut retry, AUTHORIZATION, &format!("Bearer {token}"))?;
        self.dispatch(retry, make_body()?).await
    }

    async fn send_nonreplayable<F, Fut>(
        &self,
        template: Request<()>,
        make_body: F,
    ) -> Result<HttpResponse>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<BoxBody>>,
    {
        let response = self.dispatch(template.clone(), make_body().await?).await?;
        if response.status() != StatusCode::UNAUTHORIZED {
            return Ok(response);
        }
        if self.auth_token.is_some() {
            return Ok(response);
        }
        let challenge = match response
            .headers()
            .get(WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok())
        {
            Some(value) => BearerChallenge::parse(value)?,
            None => return Ok(response),
        };
        drop(response);
        let token = self.get_token(&challenge, false).await?;
        let mut retry = template.clone();
        set_header(&mut retry, AUTHORIZATION, &format!("Bearer {token}"))?;
        let response = self.dispatch(retry, make_body().await?).await?;
        if response.status() != StatusCode::UNAUTHORIZED {
            return Ok(response);
        }
        drop(response);
        let token = self.get_token(&challenge, true).await?;
        let mut retry = template;
        set_header(&mut retry, AUTHORIZATION, &format!("Bearer {token}"))?;
        self.dispatch(retry, make_body().await?).await
    }

    async fn get_token(&self, challenge: &BearerChallenge, force_refresh: bool) -> Result<String> {
        let key = TokenCacheKey {
            realm: challenge.realm.clone(),
            service: challenge.service.clone(),
            scope: challenge.scope.clone(),
        };
        {
            let mut cache = self.token_cache.lock().map_err(|_| RegistryError::Authentication {
                message: "registry token cache lock poisoned".into(),
            })?;
            if force_refresh {
                cache.remove(&key);
            } else if let Some(cached) = cache.get(&key) {
                if cached.is_valid() {
                    return Ok(cached.token.clone());
                }
            }
        }

        let mut url = Url::parse(&challenge.realm).map_err(|error| RegistryError::Authentication {
            message: format!("invalid token service realm: {error}"),
        })?;
        {
            let mut query = url.query_pairs_mut();
            if let Some(service) = &challenge.service {
                query.append_pair("service", service);
            }
            if let Some(scope) = &challenge.scope {
                query.append_pair("scope", scope);
            }
        }
        let mut request = self.template(Method::GET, url.as_str())?;
        if let Some(identity_token) = &self.identity_token {
            set_header(&mut request, AUTHORIZATION, &format!("Bearer {identity_token}"))?;
        } else if let Some((username, password)) = &self.basic_credentials {
            if url.scheme() != "https" {
                return Err(RegistryError::Authentication {
                    message: format!("refusing Basic credentials for non-HTTPS realm: {url}"),
                });
            }
            validate_realm_host(&self.registry_host(), &url)?;
            let encoded = BASE64.encode(format!("{username}:{password}"));
            set_header(&mut request, AUTHORIZATION, &format!("Basic {encoded}"))?;
        }
        let response = self
            .dispatch_raw(request, empty_body()?)
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = read_body_capped(response, MAX_ERROR_BYTES, "token response")
                .await
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                .unwrap_or_default();
            return Err(RegistryError::Authentication {
                message: format!("token service returned {status}: {body}"),
            });
        }
        let body = read_body_capped(response, MAX_MANIFEST_BYTES, "token response").await?;
        let token_response: TokenResponse = serde_json::from_slice(&body)?;
        let token = token_response
            .token
            .or(token_response.access_token)
            .ok_or_else(|| RegistryError::Authentication {
                message: "token service response did not include token".into(),
            })?;
        let expires_at = token_response
            .expires_in
            .map(|seconds| Instant::now() + Duration::from_secs(seconds));
        self.token_cache
            .lock()
            .map_err(|_| RegistryError::Authentication {
                message: "registry token cache lock poisoned".into(),
            })?
            .insert(
                key,
                CachedToken {
                    token: token.clone(),
                    expires_at,
                },
            );
        if let Ok(mut last) = self.last_challenge.lock() {
            *last = Some(challenge.clone());
        }
        Ok(token)
    }

    fn preemptive_token(&self) -> Option<String> {
        if self.auth_token.is_some() {
            return None;
        }
        let challenge = self.last_challenge.lock().ok()?.as_ref()?.clone();
        let key = TokenCacheKey {
            realm: challenge.realm,
            service: challenge.service,
            scope: challenge.scope,
        };
        let cache = self.token_cache.lock().ok()?;
        let cached = cache.get(&key)?;
        cached.is_valid().then(|| cached.token.clone())
    }
}

struct RegistryExecutorAdapter(Arc<dyn crate::RegistryExecutor>);

impl hyper::rt::Executor<h12tiny::runtime::BoxSendFuture> for RegistryExecutorAdapter {
    fn execute(&self, future: h12tiny::runtime::BoxSendFuture) {
        self.0.execute(future);
    }
}

struct UnavailableExecutor;

impl crate::RegistryExecutor for UnavailableExecutor {
    fn execute(&self, _future: crate::BoxSendFuture) {}

    fn submit_blocking(
        &self,
        _job: crate::BoxBlockingJob,
    ) -> std::result::Result<crate::BoxBlockingFuture, crate::BlockingSubmitError> {
        Err(crate::BlockingSubmitError::Unavailable)
    }
}

fn empty_body() -> Result<BoxBody> {
    Ok(util::boxed_body(util::empty_body()))
}

fn bytes_body(bytes: Bytes) -> Result<BoxBody> {
    Ok(util::boxed_body(util::bytes_body(bytes)))
}

fn set_header(request: &mut Request<()>, name: http::header::HeaderName, value: &str) -> Result<()> {
    let value = HeaderValue::from_str(value).map_err(transport_error)?;
    request.headers_mut().insert(name, value);
    Ok(())
}

fn header_string(response: &HttpResponse, name: http::header::HeaderName) -> String {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

fn append_digest(url: &str, digest: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}digest={digest}")
}

fn validate_realm_host(registry_host: &str, realm_url: &Url) -> Result<()> {
    let realm_host = realm_url.host_str().unwrap_or("");
    let expected_auth_host: Option<&str> = match registry_host {
        "registry-1.docker.io" => Some("auth.docker.io"),
        "ghcr.io" => Some("ghcr.io"),
        "quay.io" => Some("quay.io"),
        host if host.ends_with(".amazonaws.com") => {
            if realm_host.ends_with(".amazonaws.com") {
                return Ok(());
            }
            return Err(RegistryError::Authentication {
                message: format!("ECR realm host '{realm_host}' is not on amazonaws.com (registry: {registry_host})"),
            });
        }
        host if host == "gcr.io" || host.ends_with(".gcr.io") || host.ends_with(".pkg.dev") => {
            if realm_host == "oauth2.googleapis.com"
                || realm_host.ends_with(".gcr.io")
                || realm_host.ends_with(".pkg.dev")
            {
                return Ok(());
            }
            return Err(RegistryError::Authentication {
                message: format!("GCR realm host '{realm_host}' is not on googleapis.com or gcr.io (registry: {registry_host})"),
            });
        }
        _ => None,
    };
    if let Some(expected) = expected_auth_host {
        if realm_host != expected {
            return Err(RegistryError::Authentication {
                message: format!("realm host '{realm_host}' does not match expected auth host '{expected}' for registry '{registry_host}'"),
            });
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct CachedToken {
    token: String,
    expires_at: Option<Instant>,
}

impl CachedToken {
    fn is_valid(&self) -> bool {
        match self.expires_at {
            None => true,
            Some(expires_at) => Instant::now() + Duration::from_secs(30) < expires_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TokenCacheKey {
    realm: String,
    service: Option<String>,
    scope: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BearerChallenge {
    realm: String,
    service: Option<String>,
    scope: Option<String>,
}

impl BearerChallenge {
    fn parse(header: &str) -> Result<Self> {
        let params = header
            .trim()
            .strip_prefix("Bearer ")
            .or_else(|| header.trim().strip_prefix("bearer "))
            .ok_or_else(|| RegistryError::Authentication {
                message: format!("unsupported authenticate challenge: {header}"),
            })?;
        let mut values = HashMap::new();
        for part in split_auth_params(params) {
            let Some((key, value)) = part.split_once('=') else { continue };
            values.insert(key.trim().to_ascii_lowercase(), unquote(value.trim()));
        }
        let realm = values.remove("realm").ok_or_else(|| RegistryError::Authentication {
            message: "bearer challenge missing realm".into(),
        })?;
        Ok(Self {
            realm,
            service: values.remove("service"),
            scope: values.remove("scope"),
        })
    }
}

fn split_auth_params(params: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in params.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && character == '\\' {
            escaped = true;
            continue;
        }
        if character == '"' {
            quoted = !quoted;
        } else if character == ',' && !quoted {
            parts.push(params[start..index].trim());
            start = index + 1;
        }
    }
    parts.push(params[start..].trim());
    parts
}

fn unquote(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        value[1..value.len() - 1]
            .replace("\\\"", "\"")
            .replace("\\\\", "\\")
    } else {
        value.to_string()
    }
}

#[derive(Debug, serde::Deserialize)]
struct TokenResponse {
    token: Option<String>,
    access_token: Option<String>,
    expires_in: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}};
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
            H: Fn(&str, &str) -> TestResponse + Send + Sync + 'static,
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
                        let Ok((method, path)) = read_request(&mut stream) else {
                            if thread_stop.load(Ordering::Acquire) {
                                break;
                            }
                            continue;
                        };
                        let response = handler(&method, &path);
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

    fn read_request(stream: &mut TcpStream) -> std::io::Result<(String, String)> {
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
        Ok((
            parts.next().unwrap_or_default().to_string(),
            parts.next().unwrap_or_default().to_string(),
        ))
    }

    fn write_response(stream: &mut TcpStream, response: TestResponse) -> std::io::Result<()> {
        let reason = match response.status {
            200 => "OK",
            401 => "Unauthorized",
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

    const DIGEST: &str =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000";

    fn token_body(token: &str) -> serde_json::Value {
        serde_json::json!({ "token": token, "expires_in": 300 })
    }

    #[test]
    fn migrated_client_retries_bearer_challenge_and_caches_token() {
        let head_requests = Arc::new(AtomicUsize::new(0));
        let token_requests = Arc::new(AtomicUsize::new(0));
        let server = TestServer::start({
            let head_requests = head_requests.clone();
            let token_requests = token_requests.clone();
            move |uri| {
                let challenge = format!(
                    "Bearer realm=\"{uri}/token\",service=\"test\",scope=\"repository:repo:pull\""
                );
                let head_requests = head_requests.clone();
                let token_requests = token_requests.clone();
                move |method: &str, path: &str| {
                    if method == "GET" && path.split('?').next() == Some("/token") {
                        token_requests.fetch_add(1, Ordering::Relaxed);
                        return TestResponse::new(
                            200,
                            serde_json::to_vec(&token_body("token")).unwrap(),
                        )
                        .header("Content-Type", "application/json");
                    }
                    if method == "HEAD" && path == format!("/v2/repo/blobs/{DIGEST}") {
                        let request = head_requests.fetch_add(1, Ordering::Relaxed);
                        if request == 0 {
                            return TestResponse::new(401, Vec::new())
                                .header("WWW-Authenticate", challenge.clone());
                        }
                        return TestResponse::new(404, Vec::new());
                    }
                    TestResponse::new(500, Vec::new())
                }
            }
        });

        let client = RegistryClient::new_for_tests(server.uri());
        futures_lite::future::block_on(async {
            assert!(!client.blob_exists("repo", DIGEST).await.unwrap());
            // A cached token is attached before the second request, so it does not
            // trigger another token-service request.
            assert!(!client.blob_exists("repo", DIGEST).await.unwrap());
        });
        assert_eq!(head_requests.load(Ordering::Relaxed), 3);
        assert_eq!(token_requests.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn migrated_client_bounds_manifest_body_before_returning() {
        let server = TestServer::start(|_uri| {
            move |method: &str, path: &str| {
                if method == "GET" && path == "/v2/repo/manifests/latest" {
                    return TestResponse::new(200, vec![0; MAX_MANIFEST_BYTES + 1])
                        .header("Content-Length", (MAX_MANIFEST_BYTES + 1).to_string());
                }
                TestResponse::new(404, Vec::new())
            }
        });
        let client = RegistryClient::new_for_tests(server.uri());
        let error = futures_lite::future::block_on(client.get_manifest("repo", "latest"))
            .unwrap_err();
        assert!(matches!(error, RegistryError::ResponseTooLarge { .. }));
    }

    #[test]
    fn migrated_client_follows_blob_redirect_without_registry_origin_requirement() {
        let payload = b"redirected blob".to_vec();
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(&payload)));
        let target = TestServer::start({
            let payload = payload.clone();
            move |_uri| {
                move |method: &str, path: &str| {
                    if method == "GET" && path == "/signed/blob" {
                        return TestResponse::new(200, payload.clone());
                    }
                    TestResponse::new(404, Vec::new())
                }
            }
        });
        let redirect = TestServer::start({
            let target = target.uri();
            let digest = digest.clone();
            move |_uri| {
                move |method: &str, path: &str| {
                    if method == "GET" && path == format!("/v2/repo/blobs/{digest}") {
                        return TestResponse::new(307, Vec::new())
                            .header("Location", format!("{target}/signed/blob"));
                    }
                    TestResponse::new(404, Vec::new())
                }
            }
        });
        let client = RegistryClient::new_for_tests(redirect.uri());
        let result = futures_lite::future::block_on(client.pull_blob("repo", &digest)).unwrap();
        assert_eq!(result, payload);
    }

    #[test]
    fn migrated_client_resolves_same_origin_locations_only() {
        let client = RegistryClient::new_for_tests("https://registry.example/v2".into());
        assert_eq!(
            client.resolve_location("/v2/repo/uploads/session").unwrap(),
            "https://registry.example/v2/repo/uploads/session"
        );
        assert!(client.resolve_location("https://attacker.example/upload").is_err());
    }
}
