use std::collections::{BTreeMap, HashMap};
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::Command;

use galactica_common::proto::common;
use if_addrs::{IfAddr, get_if_addrs};
use serde::Deserialize;

const DEFAULT_LOOPBACK_PRIORITY: u32 = 100;

pub fn detect_bind_endpoints(port: u16) -> Vec<common::v1::NetworkEndpoint> {
    let mut endpoints = BTreeMap::new();

    if let Ok(interfaces) = get_if_addrs() {
        for interface in interfaces {
            let ip = match interface.addr {
                IfAddr::V4(addr) => IpAddr::V4(addr.ip),
                IfAddr::V6(addr) => IpAddr::V6(addr.ip),
            };
            if !is_usable_ip(ip) {
                continue;
            }

            let kind = classify_endpoint_kind(&interface.name, ip);
            if kind == common::v1::EndpointKind::Unspecified {
                continue;
            }

            upsert_endpoint(
                &mut endpoints,
                build_endpoint(
                    socket_url(ip, port),
                    kind,
                    endpoint_priority(kind),
                    HashMap::from([
                        ("interface".to_string(), interface.name.clone()),
                        ("ip".to_string(), ip.to_string()),
                    ]),
                ),
            );
        }
    }

    if endpoints.is_empty() {
        upsert_endpoint(
            &mut endpoints,
            build_endpoint(
                socket_url(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
                common::v1::EndpointKind::Loopback,
                DEFAULT_LOOPBACK_PRIORITY,
                HashMap::from([("ip".to_string(), Ipv4Addr::LOCALHOST.to_string())]),
            ),
        );
    }

    endpoints.into_values().collect()
}

pub fn preferred_endpoint(endpoints: &[common::v1::NetworkEndpoint]) -> Option<String> {
    endpoints
        .iter()
        .filter(|endpoint| !endpoint.url.is_empty())
        .min_by(|left, right| endpoint_order(left).cmp(&endpoint_order(right)))
        .map(|endpoint| endpoint.url.clone())
}

pub fn endpoint_from_url(
    url: impl Into<String>,
    kind: common::v1::EndpointKind,
    priority: u32,
) -> common::v1::NetworkEndpoint {
    build_endpoint(url.into(), kind, priority, HashMap::new())
}

pub fn loopback_endpoint(port: u16) -> common::v1::NetworkEndpoint {
    endpoint_from_url(
        socket_url(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        common::v1::EndpointKind::Loopback,
        DEFAULT_LOOPBACK_PRIORITY,
    )
}

pub fn detect_tailscale_peer_control_plane_endpoints(
    port: u16,
) -> Vec<common::v1::NetworkEndpoint> {
    let Some(output) = run_tailscale_status() else {
        return Vec::new();
    };
    let Ok(status) = serde_json::from_slice::<TailscaleStatus>(&output.stdout) else {
        return Vec::new();
    };

    let mut endpoints = BTreeMap::new();
    for peer in status.peer.into_values() {
        for ip in peer.tailscale_ips {
            let Ok(ip) = ip.parse::<IpAddr>() else {
                continue;
            };
            if !is_tailscale_ip(ip) {
                continue;
            }

            let mut metadata = HashMap::from([("source".to_string(), "tailscale".to_string())]);
            if let Some(host_name) = &peer.host_name {
                metadata.insert("host_name".to_string(), host_name.clone());
            }
            if let Some(dns_name) = &peer.dns_name {
                metadata.insert("dns_name".to_string(), dns_name.clone());
            }

            upsert_endpoint(
                &mut endpoints,
                build_endpoint(
                    socket_url(ip, port),
                    common::v1::EndpointKind::Tailscale,
                    endpoint_priority(common::v1::EndpointKind::Tailscale),
                    metadata,
                ),
            );
        }
    }

    endpoints.into_values().collect()
}

pub fn detect_lan_scan_control_plane_endpoints(port: u16) -> Vec<common::v1::NetworkEndpoint> {
    let mut endpoints = BTreeMap::new();

    if let Ok(interfaces) = get_if_addrs() {
        for interface in interfaces {
            let IfAddr::V4(addr) = interface.addr else {
                continue;
            };
            let ip = addr.ip;
            if !ip.is_private() || is_tailscale_ip(IpAddr::V4(ip)) {
                continue;
            }

            let [a, b, c, self_host] = ip.octets();
            for host in 1..=254u8 {
                if host == self_host {
                    continue;
                }
                upsert_endpoint(
                    &mut endpoints,
                    build_endpoint(
                        socket_url(IpAddr::V4(Ipv4Addr::new(a, b, c, host)), port),
                        common::v1::EndpointKind::Lan,
                        endpoint_priority(common::v1::EndpointKind::Lan),
                        HashMap::from([
                            ("source".to_string(), "lan-scan".to_string()),
                            ("interface".to_string(), interface.name.clone()),
                            ("subnet".to_string(), format!("{a}.{b}.{c}.0/24")),
                        ]),
                    ),
                );
            }
        }
    }

    endpoints.into_values().collect()
}

fn build_endpoint(
    url: String,
    kind: common::v1::EndpointKind,
    priority: u32,
    metadata: HashMap<String, String>,
) -> common::v1::NetworkEndpoint {
    common::v1::NetworkEndpoint {
        url,
        kind: kind as i32,
        priority,
        metadata,
    }
}

fn upsert_endpoint(
    endpoints: &mut BTreeMap<String, common::v1::NetworkEndpoint>,
    endpoint: common::v1::NetworkEndpoint,
) {
    endpoints
        .entry(endpoint.url.clone())
        .and_modify(|existing| {
            if endpoint.priority < existing.priority {
                *existing = endpoint.clone();
            } else {
                existing.metadata.extend(endpoint.metadata.clone());
            }
        })
        .or_insert(endpoint);
}

fn endpoint_order(endpoint: &common::v1::NetworkEndpoint) -> (u32, i32, &str) {
    (endpoint.priority, endpoint.kind, endpoint.url.as_str())
}

fn endpoint_priority(kind: common::v1::EndpointKind) -> u32 {
    match kind {
        common::v1::EndpointKind::Tailscale => 10,
        common::v1::EndpointKind::Lan => 20,
        common::v1::EndpointKind::Wan => 30,
        common::v1::EndpointKind::Loopback => DEFAULT_LOOPBACK_PRIORITY,
        common::v1::EndpointKind::Unspecified => 200,
    }
}

fn classify_endpoint_kind(interface_name: &str, ip: IpAddr) -> common::v1::EndpointKind {
    if ip.is_loopback() {
        return common::v1::EndpointKind::Loopback;
    }
    if is_tailscale_interface(interface_name) || is_tailscale_ip(ip) {
        return common::v1::EndpointKind::Tailscale;
    }
    if is_private_or_lan(ip) {
        return common::v1::EndpointKind::Lan;
    }
    if is_public_ip(ip) {
        return common::v1::EndpointKind::Wan;
    }
    common::v1::EndpointKind::Unspecified
}

fn is_usable_ip(ip: IpAddr) -> bool {
    !ip.is_unspecified() && !ip.is_multicast() && !is_link_local(ip)
}

fn is_private_or_lan(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_private(),
        IpAddr::V6(ip) => ip.is_unique_local(),
    }
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => !ip.is_private() && !ip.is_loopback() && !ip.is_link_local(),
        IpAddr::V6(ip) => !ip.is_unique_local() && !ip.is_loopback() && !ip.is_unicast_link_local(),
    }
}

fn is_link_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_link_local(),
        IpAddr::V6(ip) => ip.is_unicast_link_local(),
    }
}

fn is_tailscale_interface(interface_name: &str) -> bool {
    interface_name.eq_ignore_ascii_case("tailscale0")
        || interface_name.to_ascii_lowercase().contains("tailscale")
}

fn is_tailscale_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [first, second, _, _] = ip.octets();
            first == 100 && (64..=127).contains(&second)
        }
        IpAddr::V6(ip) => {
            let octets = ip.octets();
            octets.starts_with(&[0xfd, 0x7a, 0x11, 0x5c, 0xa1, 0xe0])
        }
    }
}

fn socket_url(ip: IpAddr, port: u16) -> String {
    format!("http://{}", SocketAddr::new(ip, port))
}

fn run_tailscale_status() -> Option<std::process::Output> {
    if !command_exists("tailscale") {
        return None;
    }
    Command::new("tailscale")
        .args(["status", "--json"])
        .output()
        .ok()
        .filter(|output| output.status.success())
}

fn command_exists(command: &str) -> bool {
    env::var_os("PATH")
        .into_iter()
        .flat_map(|paths| env::split_paths(&paths).collect::<Vec<_>>())
        .map(|path| command_path(path, command))
        .any(|path| path.exists())
}

fn command_path(base: PathBuf, command: &str) -> PathBuf {
    #[cfg(target_os = "windows")]
    {
        let exe = format!("{command}.exe");
        if base.join(&exe).exists() {
            return base.join(exe);
        }
    }
    base.join(command)
}

#[derive(Debug, Default, Deserialize)]
struct TailscaleStatus {
    #[serde(rename = "Peer", default)]
    peer: HashMap<String, TailscalePeer>,
}

#[derive(Debug, Default, Deserialize)]
struct TailscalePeer {
    #[serde(rename = "HostName")]
    host_name: Option<String>,
    #[serde(rename = "DNSName")]
    dns_name: Option<String>,
    #[serde(rename = "TailscaleIPs", default)]
    tailscale_ips: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::{detect_lan_scan_control_plane_endpoints, endpoint_from_url, preferred_endpoint};
    use galactica_common::proto::common;

    #[test]
    fn preferred_endpoint_prioritizes_tailscale_over_lan() {
        let endpoints = vec![
            endpoint_from_url(
                "http://192.168.1.8:50061",
                common::v1::EndpointKind::Lan,
                20,
            ),
            endpoint_from_url(
                "http://100.101.102.103:50061",
                common::v1::EndpointKind::Tailscale,
                10,
            ),
        ];
        assert_eq!(
            preferred_endpoint(&endpoints),
            Some("http://100.101.102.103:50061".to_string())
        );
    }

    #[test]
    fn lan_scan_candidates_do_not_include_loopback() {
        let endpoints = detect_lan_scan_control_plane_endpoints(9090);
        assert!(
            endpoints
                .iter()
                .all(|endpoint| !endpoint.url.contains("127.0.0.1"))
        );
    }
}
