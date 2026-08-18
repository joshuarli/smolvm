//! Smolfile support for smolvm.
//!
//! This module re-exports the standalone parser and provides generic
//! smolvm-specific helpers for image resolution and network policy.

pub use smolfile::*;

use crate::data::image_source::{self, ArchiveInput, ImageSource};
use std::path::Path;

fn image_source_for_smolfile(smolfile_path: &Path, reference: &str) -> crate::Result<ImageSource> {
    let classified = image_source::classify(reference);
    match classified {
        ImageSource::Registry(_) | ImageSource::Archive(ArchiveInput::Stdin) => Ok(classified),
        ImageSource::Archive(ArchiveInput::File(path)) | ImageSource::Directory(path) => {
            let path = if path.is_absolute() {
                path
            } else {
                smolfile_path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(path)
            };
            let path_string = path.to_string_lossy().into_owned();
            Ok(image_source::classify(&path_string))
        }
    }
}

/// Resolve a Smolfile image reference using the Smolfile's directory as the
/// base for relative local paths. Local archives are staged through the same
/// content-addressed resolver used by explicit `--image` values; registry
/// references remain unchanged for the caller to handle.
pub fn resolve_smolfile_image(path: &Path, reference: &str) -> crate::Result<String> {
    let source = image_source_for_smolfile(path, reference)?;
    match source {
        ImageSource::Registry(reference) => Ok(reference),
        ImageSource::Archive(ArchiveInput::Stdin) => Err(crate::Error::config(
            "Smolfile image",
            "stdin image sources are not supported in a Smolfile; use an explicit --image value",
        )),
        source => match image_source::resolve(source)? {
            image_source::ResolvedImage::Registry(reference) => Ok(reference),
            image_source::ResolvedImage::Local { reference, .. } => Ok(reference),
        },
    }
}

/// Load and parse a Smolfile from the given path.
pub fn load(path: &Path) -> crate::Result<Smolfile> {
    smolfile::load(path).map_err(|error| crate::Error::config("load smolfile", error.to_string()))
}

/// Resolve a hostname or IP address to CIDRs suitable for TSI egress policy.
///
/// IPv4 addresses become `/32` CIDRs; IPv6 addresses become `/128` CIDRs.
/// Rejects `host:port` syntax because the policy is address-only.
pub fn resolve_host_to_cidrs(host: &str) -> Result<Vec<String>, String> {
    use ipnet::IpNet;
    use std::net::{IpAddr, ToSocketAddrs};

    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![IpNet::from(ip).to_string()]);
    }
    if host.contains(':') {
        return Err(format!(
            "invalid hostname '{}': port suffixes are not supported. Use the hostname only (all ports are allowed to resolved IPs).",
            host
        ));
    }

    let host_owned = host.to_string();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = format!("{}:0", host_owned).to_socket_addrs().map(|addrs| {
            addrs
                .map(|address| IpNet::from(address.ip()).to_string())
                .collect::<Vec<_>>()
        });
        let _ = tx.send(result);
    });
    let addrs: Vec<String> = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .map_err(|_| format!("DNS resolution for '{}' timed out after 10 seconds", host))?
        .map_err(|error| format!("failed to resolve '{}': {}", host, error))?;
    if addrs.is_empty() {
        return Err(format!("'{}' resolved to no addresses", host));
    }
    Ok(addrs)
}

/// Parse and validate a CIDR specification.
///
/// Bare IPv4 and IPv6 addresses gain their host prefix lengths.
pub fn parse_cidr(value: &str) -> Result<String, String> {
    use ipnet::IpNet;
    use std::net::IpAddr;

    let net: IpNet = match value.parse::<IpNet>() {
        Ok(net) => net,
        Err(_) => match value.parse::<IpAddr>() {
            Ok(ip) => IpNet::from(ip),
            Err(_) => {
                return Err(format!(
                    "invalid CIDR '{}': expected format like 10.0.0.0/8 or 1.1.1.1",
                    value
                ))
            }
        },
    };
    Ok(net.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_host_bare_ipv4() {
        assert_eq!(resolve_host_to_cidrs("1.2.3.4").unwrap(), vec!["1.2.3.4/32"]);
    }

    #[test]
    fn resolve_host_bare_ipv6() {
        assert_eq!(resolve_host_to_cidrs("::1").unwrap(), vec!["::1/128"]);
        assert_eq!(
            resolve_host_to_cidrs("2001:db8::1").unwrap(),
            vec!["2001:db8::1/128"]
        );
    }

    #[test]
    fn resolve_host_rejects_port_suffix() {
        let error = resolve_host_to_cidrs("example.com:443").unwrap_err();
        assert!(error.contains("port suffixes are not supported"), "{error}");
        let error = resolve_host_to_cidrs("[::1]:80").unwrap_err();
        assert!(error.contains("port suffixes are not supported"), "{error}");
    }

    #[test]
    fn parse_cidr_valid() {
        assert_eq!(parse_cidr("10.0.0.0/8").unwrap(), "10.0.0.0/8");
        assert_eq!(parse_cidr("1.1.1.1").unwrap(), "1.1.1.1/32");
    }

    #[test]
    fn parse_cidr_invalid() {
        assert!(parse_cidr("not-a-cidr").is_err());
    }

    #[test]
    fn load_basic_smolfile() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("Smolfile");
        std::fs::write(
            &path,
            r#"
image = "alpine"
cpus = 2
memory = 1024
net = true

[dev]
volumes = ["./src:/app"]
init = ["echo hello"]
"#,
        )
        .unwrap();
        let smolfile = load(&path).unwrap();
        assert_eq!(smolfile.image.as_deref(), Some("alpine"));
        assert_eq!(smolfile.cpus, Some(2));
        assert_eq!(smolfile.dev.unwrap().volumes, vec!["./src:/app"]);
    }

    #[test]
    fn smolfile_gpu_field() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gpu.smolfile");
        std::fs::write(&path, "image = \"alpine\"\ngpu = true\n").unwrap();
        assert_eq!(load(&path).unwrap().gpu, Some(true));
        std::fs::write(&path, "image = \"alpine\"\n").unwrap();
        assert_eq!(load(&path).unwrap().gpu, None);
    }

    #[test]
    fn resolve_smolfile_relative_rootfs_material() {
        let dir = tempfile::tempdir().unwrap();
        let rootfs = dir.path().join("rootfs");
        std::fs::create_dir(&rootfs).unwrap();
        let smolfile = dir.path().join("Smolfile");
        std::fs::write(&smolfile, "image = \"./rootfs\"\n").unwrap();
        assert_eq!(
            resolve_smolfile_image(&smolfile, "./rootfs").unwrap(),
            format!("local-dir:{}", rootfs.canonicalize().unwrap().display())
        );
    }
}
