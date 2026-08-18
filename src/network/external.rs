//! Static configuration for an externally-owned virtio-net link.
//!
//! Unlike smolvm's built-in virtio gateway, this mode gives libkrun a Unix
//! stream path owned by another local process. That process is responsible for
//! all Ethernet forwarding, DHCP (if any), routing, and DNS for that NIC.
//! When `egress` is enabled, smolvm additionally owns a second virtio-net NAT
//! NIC and the guest's default route. smolvm preserves both attachments across
//! `machine start`.

use std::net::Ipv4Addr;
use std::path::PathBuf;

/// One statically configured Ethernet attachment managed outside smolvm.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExternalNetworkConfig {
    /// Absolute Unix-stream listener path supplied to libkrun at VM boot.
    pub unixstream_path: PathBuf,
    /// Guest IPv4 address and prefix length.
    pub guest_ip: Ipv4Addr,
    /// CIDR prefix length for [`Self::guest_ip`].
    pub prefix_len: u8,
    /// IPv4 default gateway advertised to the guest.
    pub gateway: Ipv4Addr,
    /// IPv4 resolver advertised to the guest.
    pub dns_server: Ipv4Addr,
    /// Guest NIC MAC address.
    pub guest_mac: [u8; 6],
    /// Attach a second virtio-net NIC backed by smolvm's existing host-side
    /// NAT runtime. The external NIC remains the first interface (`eth0`);
    /// the smolvm-owned egress NIC is `eth1`.
    #[serde(default)]
    pub egress: bool,
}

impl ExternalNetworkConfig {
    /// Reject a configuration that libkrun or the guest could not use
    /// unambiguously. The external peer remains responsible for listening on
    /// the path when the VM starts.
    pub fn validate(&self) -> Result<(), String> {
        if !self.unixstream_path.is_absolute() {
            return Err("external virtio-net Unix-stream path must be absolute".into());
        }
        if self.prefix_len == 0 || self.prefix_len > 32 {
            return Err("external virtio-net IPv4 prefix length must be between 1 and 32".into());
        }
        if self.guest_ip.is_unspecified() || self.guest_ip.is_multicast() {
            return Err("external virtio-net guest IPv4 address must be unicast".into());
        }
        if self.gateway.is_unspecified() || self.gateway.is_multicast() {
            return Err("external virtio-net gateway IPv4 address must be unicast".into());
        }
        if self.dns_server.is_unspecified() || self.dns_server.is_multicast() {
            return Err("external virtio-net DNS server IPv4 address must be unicast".into());
        }
        if self.guest_ip == self.gateway {
            return Err("external virtio-net guest IPv4 address and gateway must differ".into());
        }
        if !same_ipv4_subnet(self.guest_ip, self.gateway, self.prefix_len) {
            return Err(
                "external virtio-net guest IPv4 address and gateway must share a subnet".into(),
            );
        }
        if self.guest_mac == [0; 6] || self.guest_mac == [0xff; 6] || self.guest_mac[0] & 1 != 0 {
            return Err(
                "external virtio-net MAC address must be an individual nonzero address".into(),
            );
        }
        Ok(())
    }
}

/// Parse an IPv4 CIDR supplied by the narrow external-network CLI surface.
pub fn parse_ipv4_cidr(value: &str) -> Result<(Ipv4Addr, u8), String> {
    let (address, prefix) = value.split_once('/').ok_or_else(|| {
        "external virtio-net address must be IPv4/prefix, for example 10.89.0.2/24".to_string()
    })?;
    let address = address
        .parse::<Ipv4Addr>()
        .map_err(|_| "external virtio-net address must be a valid IPv4 address".to_string())?;
    let prefix = prefix
        .parse::<u8>()
        .map_err(|_| "external virtio-net prefix length must be a number".to_string())?;
    Ok((address, prefix))
}

/// Parse a conventional colon-separated MAC address.
pub fn parse_mac(value: &str) -> Result<[u8; 6], String> {
    let octets: Vec<&str> = value.split(':').collect();
    if octets.len() != 6 || octets.iter().any(|octet| octet.len() != 2) {
        return Err("external virtio-net MAC must have six hexadecimal octets".into());
    }
    let mut mac = [0_u8; 6];
    for (index, octet) in octets.into_iter().enumerate() {
        mac[index] = u8::from_str_radix(octet, 16)
            .map_err(|_| "external virtio-net MAC must have six hexadecimal octets".to_string())?;
    }
    Ok(mac)
}

fn same_ipv4_subnet(left: Ipv4Addr, right: Ipv4Addr, prefix_len: u8) -> bool {
    let mask = if prefix_len == 32 {
        u32::MAX
    } else {
        u32::MAX << (32 - prefix_len)
    };
    u32::from(left) & mask == u32::from(right) & mask
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ExternalNetworkConfig {
        ExternalNetworkConfig {
            unixstream_path: PathBuf::from("/tmp/external-net/p-web.sock"),
            guest_ip: Ipv4Addr::new(10, 89, 0, 2),
            prefix_len: 24,
            gateway: Ipv4Addr::new(10, 89, 0, 1),
            dns_server: Ipv4Addr::new(10, 89, 0, 1),
            guest_mac: [0x02, 0, 0, 0, 0, 2],
            egress: false,
        }
    }

    #[test]
    fn valid_static_attachment_is_accepted() {
        config().validate().unwrap();
    }

    #[test]
    fn gateway_must_share_guest_subnet() {
        let mut config = config();
        config.gateway = Ipv4Addr::new(10, 90, 0, 1);
        assert!(config.validate().unwrap_err().contains("share a subnet"));
    }

    #[test]
    fn parsers_accept_cli_values() {
        assert_eq!(
            parse_ipv4_cidr("10.89.0.2/24").unwrap(),
            (Ipv4Addr::new(10, 89, 0, 2), 24)
        );
        assert_eq!(parse_mac("02:00:00:00:00:02").unwrap(), [2, 0, 0, 0, 0, 2]);
    }
}
