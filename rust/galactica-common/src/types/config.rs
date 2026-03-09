use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use super::node::NetworkMode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GalacticaConfig {
    pub control_plane: ControlPlaneConfig,
    pub node_agent: NodeAgentConfig,
    pub gateway: GatewayConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlPlaneConfig {
    pub listen_addr: SocketAddr,
    pub database_url: String,
    pub network_mode: NetworkMode,
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9090".parse().unwrap(),
            database_url: String::new(),
            network_mode: NetworkMode::Local,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAgentConfig {
    pub control_plane_addr: String,
    pub cache_dir: String,
    pub max_cache_bytes: u64,
}

impl Default for NodeAgentConfig {
    fn default() -> Self {
        Self {
            control_plane_addr: "http://localhost:9090".to_string(),
            cache_dir: "/tmp/galactica/cache".to_string(),
            max_cache_bytes: 10 * 1024 * 1024 * 1024, // 10 GB
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    pub http_listen_addr: SocketAddr,
    pub grpc_listen_addr: SocketAddr,
    pub control_plane_addr: String,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            http_listen_addr: "0.0.0.0:8080".parse().unwrap(),
            grpc_listen_addr: "0.0.0.0:8081".parse().unwrap(),
            control_plane_addr: "http://localhost:9090".to_string(),
        }
    }
}
