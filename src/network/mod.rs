//! Network configuration and backend selection.

/// Backend selection and serialization helpers.
pub mod backend;
/// Static configuration for an externally-owned virtio-net link.
pub mod external;
/// Launch-time backend planning and request validation rules.
pub mod launch;
pub mod policy;

pub use backend::NetworkBackend;
pub use external::{parse_ipv4_cidr, parse_mac, ExternalNetworkConfig};
pub use launch::{
    plan_launch_network, validate_requested_network_backend, EffectiveNetworkBackend,
    LaunchNetworkPlan,
};
pub use policy::get_dns_server;
