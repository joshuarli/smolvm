//! Smolfile configuration merging for generic machine and pack commands.

use crate::cli::parsers::parse_cidr;
use crate::cli::vm_common::CreateVmParams;
use smolvm::data::network::PortMapping;
use smolvm::data::resources::{DEFAULT_MICROVM_CPU_COUNT, DEFAULT_MICROVM_MEMORY_MIB};
use smolvm::network::NetworkBackend;
use std::path::PathBuf;

pub use smolvm::smolfile::{parse_duration_secs, Smolfile};

/// Load and parse a Smolfile from the given path.
pub fn load(path: &std::path::Path) -> smolvm::Result<Smolfile> {
    smolvm::smolfile::load(path)
}

/// Build `CreateVmParams` by merging CLI flags with an optional Smolfile.
#[allow(clippy::too_many_arguments)]
pub fn build_create_params(
    name: String,
    cli_image: Option<String>,
    cli_entrypoint: Option<String>,
    cli_cmd: Vec<String>,
    cli_cpus: u8,
    cli_mem: u32,
    cli_volume: Vec<String>,
    cli_port: Vec<PortMapping>,
    cli_net: bool,
    cli_network_backend: Option<NetworkBackend>,
    cli_dns: Option<std::net::Ipv4Addr>,
    cli_network_name: Option<String>,
    cli_init: Vec<String>,
    cli_env: Vec<String>,
    cli_workdir: Option<String>,
    smolfile_path: Option<PathBuf>,
    cli_storage_gb: Option<u64>,
    cli_overlay_gb: Option<u64>,
    cli_allow_cidr: Vec<String>,
    cli_labels: std::collections::BTreeMap<String, String>,
) -> smolvm::Result<CreateVmParams> {
    let cidrs_to_option = |values: Vec<String>| (!values.is_empty()).then_some(values);
    let smolfile = match smolfile_path {
        Some(path) => load(&path)?,
        None => {
            let net = cli_net
                || !cli_allow_cidr.is_empty()
                || cli_dns.is_some()
                || cli_network_name.is_some();
            return Ok(CreateVmParams {
                secret_refs: Default::default(),
                name,
                labels: cli_labels,
                image: cli_image,
                entrypoint: cli_entrypoint.map(|entrypoint| vec![entrypoint]).unwrap_or_default(),
                cmd: cli_cmd,
                cpus: cli_cpus,
                mem: cli_mem,
                volume: cli_volume,
                port: cli_port,
                net,
                network_backend: cli_network_backend,
                external_network: None,
                dns: cli_dns,
                network_name: cli_network_name,
                init: cli_init,
                env: cli_env,
                workdir: cli_workdir,
                storage_gb: cli_storage_gb,
                overlay_gb: cli_overlay_gb,
                allowed_cidrs: cidrs_to_option(cli_allow_cidr),
                restart_policy: None,
                restart_max_retries: None,
                restart_max_backoff_secs: None,
                health_cmd: None,
                health_interval_secs: None,
                health_timeout_secs: None,
                health_retries: None,
                health_startup_grace_secs: None,
                ssh_agent: false,
                cuda: false,
                forkable: false,
                cuda_fork_pool_size: None,
                cuda_vram_limit_mib: None,
                docker_socket: false,
                gpu: false,
                gpu_vram_mib: None,
                rosetta: false,
                dns_filter_hosts: None,
                published_sockets: Vec::new(),
                source_smolmachine: None,
            });
        }
    };

    let auto_graph = smolfile.auto_graph.unwrap_or(false);
    let cuda = smolfile.cuda.unwrap_or(false) || auto_graph;
    let fork = smolfile.fork.unwrap_or_default();
    if fork.pool_size == Some(0) {
        return Err(smolvm::Error::config(
            "smolfile [fork] pool_size",
            "must be greater than zero",
        ));
    }
    if fork.cuda_vram_limit_mib == Some(0) {
        return Err(smolvm::Error::config(
            "smolfile [fork] cuda_vram_limit_mib",
            "must be greater than zero",
        ));
    }
    if fork.pool_size.is_some() && !cuda {
        return Err(smolvm::Error::config(
            "smolfile [fork] pool_size",
            "requires cuda = true (or auto_graph = true)",
        ));
    }
    if fork.cuda_vram_limit_mib.is_some() && fork.pool_size.is_none() {
        return Err(smolvm::Error::config(
            "smolfile [fork] cuda_vram_limit_mib",
            "requires pool_size",
        ));
    }
    let forkable = fork.enabled.unwrap_or(false) || fork.pool_size.is_some();

    let image = cli_image.or(smolfile.image);
    let entrypoint = cli_entrypoint
        .map(|entrypoint| vec![entrypoint])
        .unwrap_or(smolfile.entrypoint);
    let cmd = if cli_cmd.is_empty() { smolfile.cmd } else { cli_cmd };
    let dev = smolfile.dev.unwrap_or_default();

    let mut ports = if !dev.ports.is_empty() { dev.ports } else { smolfile.ports }
        .iter()
        .map(|port| PortMapping::parse(port))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| smolvm::Error::config("smolfile ports", error))?;
    ports.extend(cli_port);

    let mut volumes = if !dev.volumes.is_empty() {
        dev.volumes
    } else {
        smolfile.volumes
    };
    volumes.extend(cli_volume);

    let mut env: Vec<String> = smolfile
        .env
        .into_iter()
        .map(|value| value.trim().to_string())
        .collect();
    env.extend(dev.env.into_iter().map(|value| value.trim().to_string()));
    env.extend(cli_env.into_iter().map(|value| value.trim().to_string()));
    if auto_graph {
        smolvm::util::enable_cuda_auto_graph_env_specs(&mut env);
    }

    let mut init = if !dev.init.is_empty() { dev.init } else { smolfile.init };
    init.extend(cli_init);
    let workdir = cli_workdir.or(dev.workdir).or(smolfile.workdir);
    let cpus = if cli_cpus != DEFAULT_MICROVM_CPU_COUNT {
        cli_cpus
    } else {
        smolfile.cpus.unwrap_or(cli_cpus)
    };
    let mem = if cli_mem != DEFAULT_MICROVM_MEMORY_MIB {
        cli_mem
    } else {
        smolfile.memory.unwrap_or(cli_mem)
    };
    let mut net = if cli_net { true } else { smolfile.net.unwrap_or(false) };
    let gpu = smolfile.gpu.unwrap_or(false);
    let rosetta = smolfile.rosetta.unwrap_or(false);
    let storage_gb = cli_storage_gb.or(smolfile.storage);
    let overlay_gb = cli_overlay_gb.or(smolfile.overlay);

    let network = smolfile.network.unwrap_or_default();
    let dns_filter_hosts = network.allow_hosts;
    let mut allowed_cidrs = network
        .allow_cidrs
        .iter()
        .map(|cidr| parse_cidr(cidr))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| smolvm::Error::config("smolfile [network] allow_cidrs", error))?;
    allowed_cidrs.extend(cli_allow_cidr);
    if !allowed_cidrs.is_empty() || !dns_filter_hosts.is_empty() || cli_dns.is_some() {
        net = true;
    }

    let restart_policy = smolfile
        .restart
        .as_ref()
        .and_then(|restart| restart.policy.as_deref())
        .map(|policy| {
            policy
                .parse::<smolvm::config::RestartPolicy>()
                .map_err(|error| smolvm::Error::config("smolfile [restart] policy", error))
        })
        .transpose()?;
    let restart_max_retries = smolfile.restart.as_ref().and_then(|restart| restart.max_retries);
    let restart_max_backoff_secs = smolfile
        .restart
        .as_ref()
        .and_then(|restart| restart.max_backoff.as_ref())
        .and_then(|value| parse_duration_secs(value));
    let health_cmd = smolfile
        .health
        .as_ref()
        .filter(|health| !health.exec.is_empty())
        .map(|health| health.exec.clone());
    let health_interval_secs = smolfile
        .health
        .as_ref()
        .and_then(|health| health.interval.as_ref())
        .and_then(|value| parse_duration_secs(value));
    let health_timeout_secs = smolfile
        .health
        .as_ref()
        .and_then(|health| health.timeout.as_ref())
        .and_then(|value| parse_duration_secs(value));
    let health_retries = smolfile.health.as_ref().and_then(|health| health.retries);
    let health_startup_grace_secs = smolfile
        .health
        .as_ref()
        .and_then(|health| health.startup_grace.as_ref())
        .and_then(|value| parse_duration_secs(value));

    Ok(CreateVmParams {
        labels: cli_labels,
        secret_refs: smolfile.secrets,
        name,
        image,
        entrypoint,
        cmd,
        cpus,
        mem,
        volume: volumes,
        port: ports,
        net,
        network_backend: cli_network_backend,
        external_network: None,
        dns: cli_dns,
        network_name: cli_network_name,
        init,
        env,
        workdir,
        storage_gb,
        overlay_gb,
        allowed_cidrs: cidrs_to_option(allowed_cidrs),
        restart_policy,
        restart_max_retries,
        restart_max_backoff_secs,
        health_cmd,
        health_interval_secs,
        health_timeout_secs,
        health_retries,
        health_startup_grace_secs,
        ssh_agent: smolfile.auth.as_ref().and_then(|auth| auth.ssh_agent).unwrap_or(false),
        cuda,
        forkable,
        cuda_fork_pool_size: fork.pool_size,
        cuda_vram_limit_mib: fork.cuda_vram_limit_mib,
        docker_socket: smolfile.docker_socket.unwrap_or(false),
        gpu,
        gpu_vram_mib: smolfile.gpu_vram,
        rosetta,
        dns_filter_hosts: (!dns_filter_hosts.is_empty()).then_some(dns_filter_hosts),
        published_sockets: Vec::new(),
        source_smolmachine: None,
    })
}

/// Resolved pack configuration from Smolfile and CLI inputs.
pub struct PackConfig {
    pub image: Option<String>,
    pub entrypoint: Vec<String>,
    pub cmd: Vec<String>,
    pub cpus: u8,
    pub mem: u32,
    pub oci_platform: Option<String>,
    pub env: Vec<String>,
    pub workdir: Option<String>,
    pub net: Option<bool>,
    pub gpu: bool,
    pub secret_refs: std::collections::BTreeMap<String, smolvm::secrets::SecretRef>,
}

/// Resolve pack configuration by merging CLI flags with an optional Smolfile.
pub fn resolve_pack_config(
    cli_image: Option<String>,
    cli_entrypoint: Option<String>,
    cli_cpus: u8,
    cli_mem: u32,
    cli_oci_platform: Option<String>,
    cli_gpu: bool,
    smolfile_path: Option<PathBuf>,
) -> smolvm::Result<PackConfig> {
    let smolfile = match smolfile_path {
        Some(path) => load(&path)?,
        None => {
            return Ok(PackConfig {
                image: cli_image,
                entrypoint: cli_entrypoint.map(|entrypoint| vec![entrypoint]).unwrap_or_default(),
                cmd: vec![],
                cpus: cli_cpus,
                mem: cli_mem,
                oci_platform: cli_oci_platform,
                env: vec![],
                workdir: None,
                net: None,
                gpu: cli_gpu,
                secret_refs: Default::default(),
            });
        }
    };
    let artifact = smolfile.artifact.or(smolfile.pack).unwrap_or_default();
    let image = cli_image.or(smolfile.image);
    let entrypoint = if let Some(entrypoint) = cli_entrypoint {
        vec![entrypoint]
    } else if !artifact.entrypoint.is_empty() {
        artifact.entrypoint
    } else {
        smolfile.entrypoint
    };
    let cmd = if !artifact.cmd.is_empty() { artifact.cmd } else { smolfile.cmd };
    let cpus = if cli_cpus != DEFAULT_MICROVM_CPU_COUNT {
        cli_cpus
    } else {
        artifact.cpus.or(smolfile.cpus).unwrap_or(cli_cpus)
    };
    let mem = if cli_mem != crate::cli::pack::PACK_DEFAULT_MEMORY_MIB {
        cli_mem
    } else {
        artifact.memory.or(smolfile.memory).unwrap_or(cli_mem)
    };
    let network_section_implies_net = smolfile
        .network
        .as_ref()
        .is_some_and(|network| !network.allow_hosts.is_empty() || !network.allow_cidrs.is_empty());
    Ok(PackConfig {
        image,
        entrypoint,
        cmd,
        cpus,
        mem,
        oci_platform: cli_oci_platform.or(artifact.oci_platform),
        env: smolfile.env.into_iter().map(|value| value.trim().to_string()).collect(),
        workdir: smolfile.workdir,
        net: if network_section_implies_net { Some(true) } else { smolfile.net },
        gpu: cli_gpu || smolfile.gpu.unwrap_or(false),
        secret_refs: smolfile.secrets,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_from_smolfile(path: PathBuf) -> smolvm::Result<CreateVmParams> {
        build_create_params(
            "test-vm".to_string(),
            None,
            None,
            vec![],
            DEFAULT_MICROVM_CPU_COUNT,
            DEFAULT_MICROVM_MEMORY_MIB,
            vec![],
            vec![],
            false,
            None,
            None,
            None,
            vec![],
            vec![],
            None,
            Some(path),
            None,
            None,
            vec![],
            Default::default(),
        )
    }

    #[test]
    fn auto_graph_smolfile_enables_cuda_and_framework_policy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("Smolfile");
        std::fs::write(
            &path,
            "auto_graph = true\nenv = [\"KEEP=yes\", \"TORCHINDUCTOR_CUDAGRAPHS=0\"]\n",
        )
        .unwrap();
        let params = build_create_params(
            "graph-vm".to_string(),
            None,
            None,
            vec![],
            DEFAULT_MICROVM_CPU_COUNT,
            DEFAULT_MICROVM_MEMORY_MIB,
            vec![],
            vec![],
            false,
            None,
            None,
            None,
            vec![],
            vec![],
            None,
            Some(path),
            None,
            None,
            vec![],
            Default::default(),
        )
        .unwrap();
        assert!(params.cuda);
        assert_eq!(
            params.env,
            vec![
                "KEEP=yes".to_string(),
                "SMOLVM_CUDA_AUTO_GRAPH=1".to_string(),
                "TORCHINDUCTOR_CUDAGRAPHS=1".to_string(),
            ]
        );
    }

    #[test]
    fn fork_smolfile_persists_launch_and_cuda_capacity_policy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("Smolfile");
        std::fs::write(
            &path,
            "cuda = true\n[fork]\nenabled = true\npool_size = 8\ncuda_vram_limit_mib = 6144\n",
        )
        .unwrap();
        let params = build_from_smolfile(path).unwrap();
        assert!(params.forkable);
        assert_eq!(params.cuda_fork_pool_size, Some(8));
        assert_eq!(params.cuda_vram_limit_mib, Some(6144));
    }

    #[test]
    fn fork_pool_requires_cuda() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("Smolfile");
        std::fs::write(&path, "[fork]\npool_size = 8\n").unwrap();
        let error = build_from_smolfile(path).err().unwrap();
        assert!(error.to_string().contains("requires cuda = true"));
    }

    #[test]
    fn cuda_vram_limit_requires_pool_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("Smolfile");
        std::fs::write(&path, "cuda = true\n[fork]\ncuda_vram_limit_mib = 6144\n").unwrap();
        let error = build_from_smolfile(path).err().unwrap();
        assert!(error.to_string().contains("requires pool_size"));
    }
}
