use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use galactica_common::Result;
use galactica_common::proto::common;
use libp2p::{Multiaddr, PeerId};
use tokio::sync::{Mutex, RwLock, broadcast};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PeerRole {
    Unknown,
    ControlPlane,
    NodeAgent,
    Gateway,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DiscoverySource {
    Mdns,
    Bootstrap,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub addresses: Vec<Multiaddr>,
    pub hostname: String,
    pub role: PeerRole,
    pub control_plane_endpoint: Option<String>,
    pub capabilities: Option<common::v1::NodeCapabilities>,
    pub discovered_at: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PeerEvent {
    Discovered {
        peer: Box<PeerInfo>,
        source: DiscoverySource,
    },
    Expired {
        peer_id: PeerId,
        source: DiscoverySource,
    },
    Disconnected {
        peer_id: PeerId,
    },
}

#[derive(Debug, Clone)]
struct ManagedPeer {
    peer: PeerInfo,
    sources: BTreeSet<DiscoverySource>,
}

#[derive(Clone)]
pub struct PeerManager {
    peers: Arc<RwLock<BTreeMap<String, ManagedPeer>>>,
    events: broadcast::Sender<PeerEvent>,
}

impl Default for PeerManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerManager {
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(64);
        Self {
            peers: Arc::new(RwLock::new(BTreeMap::new())),
            events,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PeerEvent> {
        self.events.subscribe()
    }

    pub async fn upsert_peer(&self, peer: PeerInfo, source: DiscoverySource) -> PeerInfo {
        let mut peers = self.peers.write().await;
        let key = peer.peer_id.to_string();
        let entry = peers.entry(key).or_insert_with(|| ManagedPeer {
            peer: peer.clone(),
            sources: BTreeSet::new(),
        });

        merge_peer_info(&mut entry.peer, peer);
        entry.sources.insert(source);
        let merged = entry.peer.clone();
        drop(peers);

        let _ = self.events.send(PeerEvent::Discovered {
            peer: Box::new(merged.clone()),
            source,
        });
        merged
    }

    pub async fn expire_peer_source(
        &self,
        peer_id: &PeerId,
        source: DiscoverySource,
    ) -> Option<PeerInfo> {
        let key = peer_id.to_string();
        let mut peers = self.peers.write().await;
        let mut expired = None;
        let mut disconnected = false;

        if let Some(entry) = peers.get_mut(&key) {
            entry.sources.remove(&source);
            expired = Some(entry.peer.clone());
            if entry.sources.is_empty() {
                peers.remove(&key);
                disconnected = true;
            }
        }
        drop(peers);

        if expired.is_some() {
            let _ = self.events.send(PeerEvent::Expired {
                peer_id: *peer_id,
                source,
            });
            if disconnected {
                let _ = self
                    .events
                    .send(PeerEvent::Disconnected { peer_id: *peer_id });
            }
        }

        expired
    }

    pub async fn disconnect_peer(&self, peer_id: &PeerId) -> Option<PeerInfo> {
        let removed = self.peers.write().await.remove(&peer_id.to_string());
        if let Some(removed) = removed {
            let _ = self
                .events
                .send(PeerEvent::Disconnected { peer_id: *peer_id });
            return Some(removed.peer);
        }

        None
    }

    pub async fn list_peers(&self) -> Vec<PeerInfo> {
        let peers = self.peers.read().await;
        peers.values().map(|entry| entry.peer.clone()).collect()
    }

    pub async fn peers_by_role(&self, role: PeerRole) -> Vec<PeerInfo> {
        self.list_peers()
            .await
            .into_iter()
            .filter(|peer| peer.role == role)
            .collect()
    }

    pub async fn peers_by_source(&self, source: DiscoverySource) -> Vec<PeerInfo> {
        let peers = self.peers.read().await;
        peers
            .values()
            .filter(|entry| entry.sources.contains(&source))
            .map(|entry| entry.peer.clone())
            .collect()
    }

    pub async fn primary_control_plane(&self) -> Option<PeerInfo> {
        self.list_peers()
            .await
            .into_iter()
            .filter(|peer| {
                peer.role == PeerRole::ControlPlane || peer.control_plane_endpoint.is_some()
            })
            .max_by_key(|peer| peer.discovered_at)
    }

    pub async fn primary_control_plane_endpoint(&self) -> Option<String> {
        self.primary_control_plane()
            .await
            .and_then(|peer| peer.control_plane_endpoint)
    }
}

fn merge_peer_info(into: &mut PeerInfo, incoming: PeerInfo) {
    for address in incoming.addresses {
        if !into.addresses.contains(&address) {
            into.addresses.push(address);
        }
    }
    if !incoming.hostname.is_empty() {
        into.hostname = incoming.hostname;
    }
    if incoming.role != PeerRole::Unknown {
        into.role = incoming.role;
    }
    if incoming.control_plane_endpoint.is_some() {
        into.control_plane_endpoint = incoming.control_plane_endpoint;
    }
    if incoming.capabilities.is_some() {
        into.capabilities = incoming.capabilities;
    }
    into.discovered_at = incoming.discovered_at;
    into.metadata.extend(incoming.metadata);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MdnsDiscoveryConfig {
    pub service_name: String,
    pub advertisement_ttl: Duration,
}

impl Default for MdnsDiscoveryConfig {
    fn default() -> Self {
        Self {
            service_name: "_galactica._udp.local".to_string(),
            advertisement_ttl: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MdnsPeerAdvertisement {
    pub peer_id: PeerId,
    pub addresses: Vec<Multiaddr>,
    pub hostname: String,
    pub role: PeerRole,
    pub control_plane_endpoint: Option<String>,
    pub capabilities: Option<common::v1::NodeCapabilities>,
    pub metadata: HashMap<String, String>,
}

impl MdnsPeerAdvertisement {
    pub fn to_peer_info(&self, observed_at: DateTime<Utc>) -> PeerInfo {
        PeerInfo {
            peer_id: self.peer_id,
            addresses: self.addresses.clone(),
            hostname: self.hostname.clone(),
            role: self.role,
            control_plane_endpoint: self.control_plane_endpoint.clone(),
            capabilities: self.capabilities.clone(),
            discovered_at: observed_at,
            metadata: self.metadata.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MdnsEvent {
    Discovered(Box<MdnsPeerAdvertisement>),
    Expired { peer_id: PeerId },
}

#[async_trait]
pub trait MdnsEventSource: Send {
    async fn next_event(&mut self) -> Option<MdnsEvent>;
}

pub struct MdnsDiscovery<S> {
    config: MdnsDiscoveryConfig,
    local_advertisement: MdnsPeerAdvertisement,
    peer_manager: Arc<PeerManager>,
    source: Mutex<S>,
}

impl<S> MdnsDiscovery<S> {
    pub fn new(
        config: MdnsDiscoveryConfig,
        local_advertisement: MdnsPeerAdvertisement,
        peer_manager: Arc<PeerManager>,
        source: S,
    ) -> Self {
        Self {
            config,
            local_advertisement,
            peer_manager,
            source: Mutex::new(source),
        }
    }

    pub fn config(&self) -> &MdnsDiscoveryConfig {
        &self.config
    }

    pub fn local_advertisement(&self) -> &MdnsPeerAdvertisement {
        &self.local_advertisement
    }

    pub async fn auto_discovered_control_plane(&self) -> Option<String> {
        self.peer_manager.primary_control_plane_endpoint().await
    }
}

impl<S> MdnsDiscovery<S>
where
    S: MdnsEventSource,
{
    pub async fn run_once(&self) -> Result<Option<MdnsEvent>> {
        let event = {
            let mut source = self.source.lock().await;
            source.next_event().await
        };
        if let Some(event) = event.clone() {
            self.process_event(event).await?;
        }
        Ok(event)
    }

    pub async fn run_until_idle(&self, max_events: usize) -> Result<usize> {
        let mut processed = 0;
        for _ in 0..max_events {
            match self.run_once().await? {
                Some(_) => processed += 1,
                None => break,
            }
        }
        Ok(processed)
    }

    pub async fn process_event(&self, event: MdnsEvent) -> Result<()> {
        match event {
            MdnsEvent::Discovered(advertisement) => {
                if advertisement.peer_id != self.local_advertisement.peer_id {
                    self.peer_manager
                        .upsert_peer(
                            advertisement.as_ref().to_peer_info(Utc::now()),
                            DiscoverySource::Mdns,
                        )
                        .await;
                }
            }
            MdnsEvent::Expired { peer_id } => {
                self.peer_manager
                    .expire_peer_source(&peer_id, DiscoverySource::Mdns)
                    .await;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use libp2p::multiaddr::Protocol;

    use super::*;

    struct VecMdnsEventSource {
        events: VecDeque<MdnsEvent>,
    }

    #[async_trait]
    impl MdnsEventSource for VecMdnsEventSource {
        async fn next_event(&mut self) -> Option<MdnsEvent> {
            self.events.pop_front()
        }
    }

    #[tokio::test]
    async fn peer_manager_tracks_discovery_sources() {
        let manager = PeerManager::new();
        let peer_id = PeerId::random();
        let peer = sample_peer(peer_id, PeerRole::NodeAgent, None);

        manager
            .upsert_peer(peer.clone(), DiscoverySource::Mdns)
            .await;
        manager
            .upsert_peer(peer.clone(), DiscoverySource::Bootstrap)
            .await;
        assert_eq!(manager.list_peers().await.len(), 1);
        assert_eq!(
            manager.peers_by_source(DiscoverySource::Mdns).await.len(),
            1
        );

        manager
            .expire_peer_source(&peer_id, DiscoverySource::Mdns)
            .await
            .unwrap();
        assert_eq!(manager.list_peers().await.len(), 1);

        manager
            .expire_peer_source(&peer_id, DiscoverySource::Bootstrap)
            .await
            .unwrap();
        assert!(manager.list_peers().await.is_empty());
    }

    #[tokio::test]
    async fn mdns_discovery_ingests_events_and_finds_control_plane() {
        let peer_manager = Arc::new(PeerManager::new());
        let local_peer_id = PeerId::random();
        let control_plane_id = PeerId::random();
        let worker_id = PeerId::random();
        let discovery = MdnsDiscovery::new(
            MdnsDiscoveryConfig::default(),
            MdnsPeerAdvertisement {
                peer_id: local_peer_id,
                addresses: vec![sample_addr(4100)],
                hostname: "local-node".to_string(),
                role: PeerRole::NodeAgent,
                control_plane_endpoint: None,
                capabilities: None,
                metadata: HashMap::new(),
            },
            peer_manager.clone(),
            VecMdnsEventSource {
                events: VecDeque::from(vec![
                    MdnsEvent::Discovered(Box::new(MdnsPeerAdvertisement {
                        peer_id: control_plane_id,
                        addresses: vec![sample_addr(9090)],
                        hostname: "control-plane".to_string(),
                        role: PeerRole::ControlPlane,
                        control_plane_endpoint: Some("http://control-plane.local:9090".to_string()),
                        capabilities: None,
                        metadata: HashMap::from([("zone".to_string(), "lab".to_string())]),
                    })),
                    MdnsEvent::Discovered(Box::new(MdnsPeerAdvertisement {
                        peer_id: worker_id,
                        addresses: vec![sample_addr(4200)],
                        hostname: "worker-01".to_string(),
                        role: PeerRole::NodeAgent,
                        control_plane_endpoint: None,
                        capabilities: None,
                        metadata: HashMap::new(),
                    })),
                ]),
            },
        );

        assert_eq!(discovery.run_until_idle(8).await.unwrap(), 2);
        assert_eq!(peer_manager.list_peers().await.len(), 2);
        assert_eq!(
            discovery.auto_discovered_control_plane().await,
            Some("http://control-plane.local:9090".to_string())
        );
    }

    fn sample_peer(peer_id: PeerId, role: PeerRole, endpoint: Option<String>) -> PeerInfo {
        PeerInfo {
            peer_id,
            addresses: vec![sample_addr(4100)],
            hostname: "peer".to_string(),
            role,
            control_plane_endpoint: endpoint,
            capabilities: None,
            discovered_at: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    fn sample_addr(port: u16) -> Multiaddr {
        Multiaddr::empty()
            .with(Protocol::Ip4(std::net::Ipv4Addr::LOCALHOST))
            .with(Protocol::Tcp(port))
    }
}
