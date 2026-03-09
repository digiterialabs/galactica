use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use galactica_common::proto::common;
use galactica_common::{GalacticaError, Result};
use libp2p::{Multiaddr, PeerId};
use url::Url;

use crate::discovery::{DiscoverySource, PeerInfo, PeerManager, PeerRole};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TlsTransportConfig {
    pub enabled: bool,
    pub ca_cert_path: Option<PathBuf>,
    pub client_cert_path: Option<PathBuf>,
    pub client_key_path: Option<PathBuf>,
    pub server_name_override: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NatTraversalMode {
    Auto,
    PreferPublic,
    ForceRelay,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BootstrapDiscoveryConfig {
    pub endpoints: Vec<Url>,
    pub node_id: String,
    pub peer_id: PeerId,
    pub advertised_addresses: Vec<Multiaddr>,
    pub hostname: String,
    pub role: PeerRole,
    pub control_plane_endpoint: Option<String>,
    pub capabilities: Option<common::v1::NodeCapabilities>,
    pub tls: TlsTransportConfig,
    pub nat_traversal: NatTraversalMode,
    pub metadata: HashMap<String, String>,
}

impl BootstrapDiscoveryConfig {
    pub fn validate(&self) -> Result<()> {
        if self.endpoints.is_empty() {
            return Err(GalacticaError::invalid_argument(
                "at least one bootstrap endpoint is required",
            ));
        }
        Ok(())
    }

    pub fn registration(&self) -> BootstrapRegistration {
        BootstrapRegistration {
            node_id: self.node_id.clone(),
            peer: PeerInfo {
                peer_id: self.peer_id,
                addresses: self.advertised_addresses.clone(),
                hostname: self.hostname.clone(),
                role: self.role,
                control_plane_endpoint: self.control_plane_endpoint.clone(),
                capabilities: self.capabilities.clone(),
                discovered_at: Utc::now(),
                metadata: self.metadata.clone(),
            },
            tls: self.tls.clone(),
            nat_traversal: self.nat_traversal,
            endpoints: self.endpoints.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BootstrapRegistration {
    pub node_id: String,
    pub peer: PeerInfo,
    pub tls: TlsTransportConfig,
    pub nat_traversal: NatTraversalMode,
    pub endpoints: Vec<Url>,
}

#[async_trait]
pub trait BootstrapClient: Send + Sync {
    async fn register(&self, registration: BootstrapRegistration) -> Result<()>;
    async fn discover_peers(&self, endpoints: &[Url]) -> Result<Vec<PeerInfo>>;
}

pub struct BootstrapDiscovery<C> {
    config: BootstrapDiscoveryConfig,
    client: Arc<C>,
    peer_manager: Arc<PeerManager>,
}

impl<C> BootstrapDiscovery<C> {
    pub fn new(
        config: BootstrapDiscoveryConfig,
        client: Arc<C>,
        peer_manager: Arc<PeerManager>,
    ) -> Self {
        Self {
            config,
            client,
            peer_manager,
        }
    }

    pub fn config(&self) -> &BootstrapDiscoveryConfig {
        &self.config
    }
}

impl<C> BootstrapDiscovery<C>
where
    C: BootstrapClient,
{
    pub async fn register_self(&self) -> Result<()> {
        self.config.validate()?;
        self.client.register(self.config.registration()).await
    }

    pub async fn refresh(&self) -> Result<Vec<PeerInfo>> {
        self.register_self().await?;
        let peers = self.client.discover_peers(&self.config.endpoints).await?;
        let mut discovered = Vec::new();
        for peer in peers {
            if peer.peer_id == self.config.peer_id {
                continue;
            }
            discovered.push(
                self.peer_manager
                    .upsert_peer(peer, DiscoverySource::Bootstrap)
                    .await,
            );
        }
        Ok(discovered)
    }
}

pub struct DiscoveryCoordinator<C> {
    peer_manager: Arc<PeerManager>,
    bootstrap: BootstrapDiscovery<C>,
}

impl<C> DiscoveryCoordinator<C> {
    pub fn new(peer_manager: Arc<PeerManager>, bootstrap: BootstrapDiscovery<C>) -> Self {
        Self {
            peer_manager,
            bootstrap,
        }
    }
}

impl<C> DiscoveryCoordinator<C>
where
    C: BootstrapClient,
{
    pub async fn resolve_control_plane(&self) -> Result<String> {
        if let Some(endpoint) = self.peer_manager.primary_control_plane_endpoint().await {
            return Ok(endpoint);
        }

        self.bootstrap.refresh().await?;
        self.peer_manager
            .primary_control_plane_endpoint()
            .await
            .ok_or_else(|| GalacticaError::not_found("no control plane discovered"))
    }
}

#[cfg(test)]
mod tests {
    use libp2p::multiaddr::Protocol;
    use tokio::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct FakeBootstrapClient {
        registrations: Mutex<Vec<BootstrapRegistration>>,
        peers: Mutex<Vec<PeerInfo>>,
        discover_calls: Mutex<usize>,
    }

    #[async_trait]
    impl BootstrapClient for FakeBootstrapClient {
        async fn register(&self, registration: BootstrapRegistration) -> Result<()> {
            self.registrations.lock().await.push(registration);
            Ok(())
        }

        async fn discover_peers(&self, _endpoints: &[Url]) -> Result<Vec<PeerInfo>> {
            *self.discover_calls.lock().await += 1;
            Ok(self.peers.lock().await.clone())
        }
    }

    #[tokio::test]
    async fn bootstrap_refresh_registers_and_discovers_peers() {
        let peer_manager = Arc::new(PeerManager::new());
        let local_id = PeerId::random();
        let remote_id = PeerId::random();
        let client = Arc::new(FakeBootstrapClient::default());
        client.peers.lock().await.push(PeerInfo {
            peer_id: remote_id,
            addresses: vec![sample_addr(9090)],
            hostname: "cloud-control-plane".to_string(),
            role: PeerRole::ControlPlane,
            control_plane_endpoint: Some("https://cp.galactica.test:443".to_string()),
            capabilities: None,
            discovered_at: Utc::now(),
            metadata: HashMap::new(),
        });

        let discovery = BootstrapDiscovery::new(
            sample_config(local_id),
            client.clone(),
            peer_manager.clone(),
        );

        let discovered = discovery.refresh().await.unwrap();
        assert_eq!(discovered.len(), 1);
        assert_eq!(client.registrations.lock().await.len(), 1);
        assert_eq!(peer_manager.list_peers().await.len(), 1);
        assert_eq!(
            peer_manager.primary_control_plane_endpoint().await,
            Some("https://cp.galactica.test:443".to_string())
        );
    }

    #[tokio::test]
    async fn coordinator_falls_back_to_bootstrap_only_when_local_discovery_is_empty() {
        let peer_manager = Arc::new(PeerManager::new());
        let local_id = PeerId::random();
        let client = Arc::new(FakeBootstrapClient::default());
        client.peers.lock().await.push(PeerInfo {
            peer_id: PeerId::random(),
            addresses: vec![sample_addr(9090)],
            hostname: "remote-cp".to_string(),
            role: PeerRole::ControlPlane,
            control_plane_endpoint: Some("https://cp.remote:443".to_string()),
            capabilities: None,
            discovered_at: Utc::now(),
            metadata: HashMap::new(),
        });

        let coordinator = DiscoveryCoordinator::new(
            peer_manager.clone(),
            BootstrapDiscovery::new(
                sample_config(local_id),
                client.clone(),
                peer_manager.clone(),
            ),
        );

        assert_eq!(
            coordinator.resolve_control_plane().await.unwrap(),
            "https://cp.remote:443"
        );
        assert_eq!(*client.discover_calls.lock().await, 1);

        peer_manager
            .upsert_peer(
                PeerInfo {
                    peer_id: PeerId::random(),
                    addresses: vec![sample_addr(9443)],
                    hostname: "local-cp".to_string(),
                    role: PeerRole::ControlPlane,
                    control_plane_endpoint: Some("http://local-cp:9090".to_string()),
                    capabilities: None,
                    discovered_at: Utc::now(),
                    metadata: HashMap::new(),
                },
                DiscoverySource::Mdns,
            )
            .await;

        assert_eq!(
            coordinator.resolve_control_plane().await.unwrap(),
            "http://local-cp:9090"
        );
        assert_eq!(*client.discover_calls.lock().await, 1);
    }

    fn sample_config(local_id: PeerId) -> BootstrapDiscoveryConfig {
        BootstrapDiscoveryConfig {
            endpoints: vec![Url::parse("https://bootstrap.galactica.test/discover").unwrap()],
            node_id: "node-1".to_string(),
            peer_id: local_id,
            advertised_addresses: vec![sample_addr(4100)],
            hostname: "node-1".to_string(),
            role: PeerRole::NodeAgent,
            control_plane_endpoint: None,
            capabilities: None,
            tls: TlsTransportConfig {
                enabled: true,
                ca_cert_path: Some(PathBuf::from("/tmp/ca.pem")),
                client_cert_path: Some(PathBuf::from("/tmp/node.pem")),
                client_key_path: Some(PathBuf::from("/tmp/node.key")),
                server_name_override: Some("bootstrap.galactica.test".to_string()),
            },
            nat_traversal: NatTraversalMode::Auto,
            metadata: HashMap::from([("deployment".to_string(), "hybrid".to_string())]),
        }
    }

    fn sample_addr(port: u16) -> Multiaddr {
        Multiaddr::empty()
            .with(Protocol::Ip4(std::net::Ipv4Addr::LOCALHOST))
            .with(Protocol::Tcp(port))
    }
}
