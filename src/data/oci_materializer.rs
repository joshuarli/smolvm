//! Dockerless OCI registry image materialization.
//!
//! This module pulls a digest-pinned OCI image on the host, verifies every
//! descriptor while streaming it, and writes a deterministic Docker archive
//! into smolvm's existing local archive cache. It never contacts Docker, a
//! Docker socket, or an OrbStack service.

use crate::data::image_source;
use crate::registry::{registry_client, PullAuth, Reference};
use crate::{Error, Result, SmolSettings};
use blake3::Hasher as Blake3Hasher;
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const OCI_MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";
const DOCKER_MANIFEST_MEDIA_TYPE: &str = "application/vnd.docker.distribution.manifest.v2+json";
const GZIP_LAYER_MEDIA_TYPES: &[&str] = &[
    "application/vnd.oci.image.layer.v1.tar+gzip",
    "application/vnd.docker.image.rootfs.diff.tar.gzip",
];
const TAR_LAYER_MEDIA_TYPES: &[&str] = &[
    "application/vnd.oci.image.layer.v1.tar",
    "application/vnd.docker.image.rootfs.diff.tar",
];
const COPY_CHUNK: usize = 1024 * 1024;

/// A local Docker archive made from one immutable OCI image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedRegistryArchive {
    /// Original immutable reference authored by the user.
    pub source_reference: String,
    /// Digest named by the original reference (usually an index digest).
    pub source_digest: String,
    /// Digest of the selected single-platform OCI manifest.
    pub platform_manifest_digest: String,
    /// Content-addressed Docker archive visible to smolvm's local-image resolver.
    pub archive_path: PathBuf,
    /// BLAKE3 digest of the locally prepared `archive_path`.
    ///
    /// OCI descriptors and manifests remain SHA-256; this digest identifies
    /// the host-local Docker archive and therefore uses the local-material
    /// algorithm instead.
    pub archive_blake3_digest: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OciManifest {
    schema_version: u32,
    #[serde(default)]
    media_type: Option<String>,
    config: OciDescriptor,
    layers: Vec<OciDescriptor>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct OciDescriptor {
    media_type: String,
    digest: String,
    size: u64,
}

/// Materialize an immutable OCI registry reference into smolvm's shared local
/// archive cache. The selected image platform is always `linux/<host arch>`,
/// matching the guest architecture selected by the existing registry client.
pub fn materialize_registry_archive(reference: &str) -> Result<PreparedRegistryArchive> {
    let reference = Reference::parse(reference)
        .map_err(|error| Error::config("image materialize", error.to_string()))?;
    let source_digest = reference.digest.clone().ok_or_else(|| {
        Error::config(
            "image materialize",
            "host materialization requires an immutable sha256 image digest",
        )
    })?;
    let source_reference = reference.to_string();
    let repository = repository_path(&reference);
    let image_settings = SmolSettings::load()?.images;
    let client = registry_client(&reference.registry, &image_settings, &PullAuth::FromConfig);
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|error| Error::agent("image materialize", error.to_string()))?;
    runtime.block_on(materialize_with_client(
        &client,
        &repository,
        &source_reference,
        &source_digest,
    ))
}

fn repository_path(reference: &Reference) -> String {
    match &reference.namespace {
        Some(namespace) => format!("{namespace}/{}", reference.name),
        None if matches!(
            reference.registry.as_str(),
            "docker.io" | "docker.com" | "index.docker.io" | "registry-1.docker.io"
        ) =>
        {
            format!("library/{}", reference.name)
        }
        None => reference.name.clone(),
    }
}

async fn materialize_with_client(
    client: &smolvm_registry::RegistryClient,
    repository: &str,
    source_reference: &str,
    source_digest: &str,
) -> Result<PreparedRegistryArchive> {
    let manifest_bytes = client
        .get_manifest_resolved(repository, source_digest)
        .await
        .map_err(|error| Error::agent("image materialize manifest", error.to_string()))?;
    let platform_manifest_digest = digest_bytes(&manifest_bytes);
    let manifest: OciManifest = serde_json::from_slice(&manifest_bytes).map_err(|error| {
        Error::config(
            "image material manifest",
            format!("parse selected OCI manifest: {error}"),
        )
    })?;
    if manifest.schema_version != 2 {
        return Err(Error::config(
            "image material manifest",
            format!("unsupported OCI schema version {}", manifest.schema_version),
        ));
    }
    if let Some(media_type) = manifest.media_type.as_deref() {
        if media_type != OCI_MANIFEST_MEDIA_TYPE && media_type != DOCKER_MANIFEST_MEDIA_TYPE {
            return Err(Error::config(
                "image material manifest",
                format!("unsupported manifest media type '{media_type}'"),
            ));
        }
    }
    validate_descriptor(&manifest.config, "image config")?;
    if manifest.layers.is_empty() {
        return Err(Error::config(
            "image material manifest",
            "a materialized image must contain at least one filesystem layer",
        ));
    }
    for (index, layer) in manifest.layers.iter().enumerate() {
        validate_descriptor(layer, &format!("image layer {index}"))?;
        if !GZIP_LAYER_MEDIA_TYPES.contains(&layer.media_type.as_str())
            && !TAR_LAYER_MEDIA_TYPES.contains(&layer.media_type.as_str())
        {
            return Err(Error::config(
                "image material manifest",
                format!(
                    "image layer {index} has unsupported media type '{}'; host materialization supports tar and gzip-compressed tar layers",
                    layer.media_type
                ),
            ));
        }
    }

    // Configs are deliberately small; the registry client applies a hard cap
    // and validates its descriptor digest before returning it.
    let config = client
        .pull_blob(repository, &manifest.config.digest)
        .await
        .map_err(|error| Error::agent("image materialize config", error.to_string()))?;
    if config.len() as u64 != manifest.config.size {
        return Err(Error::config(
            "image material manifest",
            format!(
                "config size mismatch: manifest says {}, registry returned {}",
                manifest.config.size,
                config.len()
            ),
        ));
    }

    let cache = image_source::archive_cache_base()?;
    fs::create_dir_all(&cache)?;
    let staging = tempfile::tempdir_in(&cache)?;
    let config_path = staging.path().join("config.json");
    fs::write(&config_path, &config)?;
    let mut layers = Vec::with_capacity(manifest.layers.len());
    let mut total_uncompressed = config.len() as u64;
    for (index, descriptor) in manifest.layers.iter().enumerate() {
        let layer_dir = staging.path().join(format!("layer-{index:04}"));
        fs::create_dir(&layer_dir)?;
        let compressed = layer_dir.join("source.blob");
        stream_blob_to_file(client, repository, descriptor, &compressed).await?;
        let layer_tar = layer_dir.join("layer.tar");
        let remaining = image_source::max_archive_bytes().saturating_sub(total_uncompressed);
        let written = if GZIP_LAYER_MEDIA_TYPES.contains(&descriptor.media_type.as_str()) {
            decompress_gzip_to_file(&compressed, &layer_tar, remaining)?
        } else {
            copy_file_bounded(&compressed, &layer_tar, remaining)?
        };
        total_uncompressed = total_uncompressed.checked_add(written).ok_or_else(|| {
            Error::config("image materialize", "prepared archive size overflow")
        })?;
        fs::remove_file(compressed)?;
        layers.push(layer_tar);
    }

    let manifest_json = docker_save_manifest(&layers)?;
    fs::write(staging.path().join("manifest.json"), manifest_json)?;
    let archive_tmp = tempfile::NamedTempFile::new_in(&cache)?;
    write_ustar_archive(
        archive_tmp.path(),
        &[staging.path().join("manifest.json"), config_path]
            .into_iter()
            .chain(layers.into_iter())
            .collect::<Vec<_>>(),
        staging.path(),
    )?;
    let archive_blake3_digest = digest_file(archive_tmp.path())?;
    let archive_size = archive_tmp.as_file().metadata()?.len();
    if archive_size > image_source::max_archive_bytes() {
        return Err(Error::config(
            "image materialize",
            format!(
                "prepared archive is {archive_size} bytes, over the {}-byte limit",
                image_source::max_archive_bytes()
            ),
        ));
    }
    let digest_hex = archive_blake3_digest
        .strip_prefix("blake3:")
        .expect("digest_file always prefixes blake3:");
    let archive_path = cache.join(digest_hex).join("archive.tar");
    if !archive_path.exists() {
        fs::create_dir_all(archive_path.parent().expect("archive has parent"))?;
        match archive_tmp.persist_noclobber(&archive_path) {
            Ok(_) => {}
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.error.into()),
        }
    }

    Ok(PreparedRegistryArchive {
        source_reference: source_reference.to_string(),
        source_digest: source_digest.to_string(),
        platform_manifest_digest,
        archive_path,
        archive_blake3_digest,
    })
}

fn validate_descriptor(descriptor: &OciDescriptor, label: &str) -> Result<()> {
    smolvm_registry::validate_digest(&descriptor.digest)
        .map_err(|error| Error::config("image material manifest", error.to_string()))?;
    if descriptor.size == 0 {
        return Err(Error::config(
            "image material manifest",
            format!("{label} must have a positive size"),
        ));
    }
    Ok(())
}

async fn stream_blob_to_file(
    client: &smolvm_registry::RegistryClient,
    repository: &str,
    descriptor: &OciDescriptor,
    output: &Path,
) -> Result<()> {
    let stream = client
        .pull_blob_stream(repository, &descriptor.digest)
        .await
        .map_err(|error| Error::agent("image materialize layer", error.to_string()))?;
    let mut file = tokio::fs::File::create(output).await?;
    let mut hasher = Sha256::new();
    let mut written = 0u64;
    futures_util::pin_mut!(stream);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|error| Error::agent("image materialize layer", error.to_string()))?;
        written = written.checked_add(chunk.len() as u64).ok_or_else(|| {
            Error::config("image materialize", "registry layer byte count overflow")
        })?;
        if written > descriptor.size {
            return Err(Error::config(
                "image material manifest",
                format!(
                    "layer {} exceeds the manifest size {}",
                    descriptor.digest, descriptor.size
                ),
            ));
        }
        hasher.update(&chunk);
        tokio::io::AsyncWriteExt::write_all(&mut file, &chunk).await?;
    }
    tokio::io::AsyncWriteExt::flush(&mut file).await?;
    if written != descriptor.size {
        return Err(Error::config(
            "image material manifest",
            format!(
                "layer {} size mismatch: manifest says {}, registry returned {written}",
                descriptor.digest, descriptor.size
            ),
        ));
    }
    let actual = format!("sha256:{}", hex::encode(hasher.finalize()));
    if actual != descriptor.digest {
        return Err(Error::config(
            "image material manifest",
            format!(
                "layer digest mismatch: expected {}, got {actual}",
                descriptor.digest
            ),
        ));
    }
    Ok(())
}

fn decompress_gzip_to_file(input: &Path, output: &Path, limit: u64) -> Result<u64> {
    let mut child = Command::new("gzip")
        .args(["-c", "-d"])
        .arg(input)
        .stdout(Stdio::piped())
        // The layer descriptor is the durable diagnostic; inheriting an
        // unbounded gzip stderr pipe here could deadlock a malformed input.
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| {
            Error::agent("image materialize", format!("run gzip: {error}"))
        })?;
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut output = fs::File::create(output)?;
    let copied = match copy_reader_bounded(&mut stdout, &mut output, limit) {
        Ok(copied) => copied,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
    };
    let status = child.wait()?;
    if !status.success() {
        return Err(Error::agent(
            "image materialize",
            format!("gzip failed while expanding an OCI layer: {status}"),
        ));
    }
    Ok(copied)
}

fn copy_file_bounded(input: &Path, output: &Path, limit: u64) -> Result<u64> {
    let mut input = fs::File::open(input)?;
    let mut output = fs::File::create(output)?;
    copy_reader_bounded(&mut input, &mut output, limit)
}

fn copy_reader_bounded(reader: &mut impl Read, writer: &mut impl Write, limit: u64) -> Result<u64> {
    let mut buffer = vec![0; COPY_CHUNK];
    let mut written = 0u64;
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        written = written.checked_add(count as u64).ok_or_else(|| {
            Error::config(
                "image materialize",
                "prepared archive byte count overflow",
            )
        })?;
        if written > limit {
            return Err(Error::config(
                "image materialize",
                format!(
                    "prepared image exceeds the {}-byte limit",
                    image_source::max_archive_bytes()
                ),
            ));
        }
        writer.write_all(&buffer[..count])?;
    }
    writer.flush()?;
    Ok(written)
}

fn docker_save_manifest(layers: &[PathBuf]) -> Result<Vec<u8>> {
    let layers = layers
        .iter()
        .map(|path| {
            path.strip_prefix(
                path.parent()
                    .and_then(Path::parent)
                    .expect("layer path has staging parent"),
            )
            .expect("layer is under staging")
            .to_string_lossy()
            .into_owned()
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&vec![serde_json::json!({
        "Config": "config.json",
        "RepoTags": Vec::<String>::new(),
        "Layers": layers,
    })])
    .map_err(|error| Error::config("image materialize", error.to_string()))
}

/// Write a deliberately small deterministic ustar archive. Docker archives only
/// need regular-file entries for `manifest.json`, the config, and `layer.tar`
/// files, so carrying a general archive writer would be unnecessary here.
fn write_ustar_archive(output: &Path, files: &[PathBuf], root: &Path) -> Result<()> {
    let mut archive = fs::File::create(output)?;
    for path in files {
        let relative = path.strip_prefix(root).map_err(|_| {
            Error::config(
                "image materialize",
                "archive member escaped staging root",
            )
        })?;
        let name = relative.to_str().ok_or_else(|| {
            Error::config(
                "image materialize",
                "archive member is not valid UTF-8",
            )
        })?;
        if name.len() > 100 || name.contains('\0') {
            return Err(Error::config(
                "image materialize",
                format!("archive member name is unsupported: {name}"),
            ));
        }
        let metadata = fs::metadata(path)?;
        write_ustar_header(&mut archive, name, metadata.len())?;
        let mut source = fs::File::open(path)?;
        std::io::copy(&mut source, &mut archive)?;
        let padding = (512 - (metadata.len() % 512)) % 512;
        if padding != 0 {
            archive.write_all(&vec![0; padding as usize])?;
        }
    }
    archive.write_all(&[0; 1024])?;
    archive.sync_all()?;
    Ok(())
}

fn write_ustar_header(output: &mut impl Write, name: &str, size: u64) -> Result<()> {
    let mut header = [0u8; 512];
    header[..name.len()].copy_from_slice(name.as_bytes());
    write_octal(&mut header[100..108], 0o644)?;
    write_octal(&mut header[108..116], 0)?;
    write_octal(&mut header[116..124], 0)?;
    write_octal(&mut header[124..136], size)?;
    write_octal(&mut header[136..148], 0)?;
    header[148..156].fill(b' ');
    header[156] = b'0';
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    let checksum = header.iter().map(|byte| *byte as u32).sum::<u32>();
    let value = format!("{:06o}\0 ", checksum);
    header[148..156].copy_from_slice(value.as_bytes());
    output.write_all(&header)?;
    Ok(())
}

fn write_octal(field: &mut [u8], value: u64) -> Result<()> {
    let width = field.len().saturating_sub(1);
    let value = format!("{value:0width$o}");
    if value.len() != width {
        return Err(Error::config(
            "image materialize",
            "ustar field is too large",
        ));
    }
    field[..width].copy_from_slice(value.as_bytes());
    field[width] = 0;
    Ok(())
}

fn digest_bytes(bytes: &[u8]) -> String {
    // The registry protocol requires SHA-256 for OCI manifests.
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn digest_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Blake3Hasher::new();
    let mut buffer = vec![0; COPY_CHUNK];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_path_adds_docker_hub_library_only_for_official_images() {
        assert_eq!(
            repository_path(&Reference::parse("docker.io/redis@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()),
            "library/redis"
        );
        assert_eq!(
            repository_path(&Reference::parse("ghcr.io/getsentry/snuba@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()),
            "getsentry/snuba"
        );
    }

    #[test]
    fn deterministic_ustar_contains_the_docker_archive_members() {
        let directory = tempfile::tempdir().unwrap();
        let config = directory.path().join("config.json");
        let manifest = directory.path().join("manifest.json");
        let layer_dir = directory.path().join("layer-0000");
        let layer = layer_dir.join("layer.tar");
        fs::create_dir(&layer_dir).unwrap();
        fs::write(&config, b"config").unwrap();
        fs::write(&manifest, b"manifest").unwrap();
        fs::write(&layer, b"layer").unwrap();
        let archive = directory.path().join("image.tar");

        write_ustar_archive(&archive, &[manifest, config, layer], directory.path()).unwrap();

        let listing = Command::new("tar")
            .args(["-tf"])
            .arg(&archive)
            .output()
            .unwrap();
        assert!(listing.status.success());
        assert_eq!(
            String::from_utf8(listing.stdout).unwrap(),
            "manifest.json\nconfig.json\nlayer-0000/layer.tar\n"
        );
    }

    #[test]
    fn local_archive_material_digest_is_blake3_and_oci_manifest_digest_stays_sha256() {
        let directory = tempfile::tempdir().unwrap();
        let archive = directory.path().join("image.tar");
        fs::write(&archive, b"prepared archive").unwrap();

        assert_eq!(
            digest_file(&archive).unwrap(),
            "blake3:5331423d52cbcede52831772da903cc3c2bacb700d9a96909cf6101135bc6517"
        );
        assert_eq!(
            digest_bytes(b"prepared manifest"),
            "sha256:0f325b54adc55e3a112408deac903ae1c3b8f7e9004bb2009607f479035eabec"
        );
    }
}
