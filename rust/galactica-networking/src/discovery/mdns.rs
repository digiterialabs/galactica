use std::collections::HashSet;

use galactica_common::types::node::NodeId;
use tokio::sync::watch;

/// mDNS-based peer discovery for local/LAN clusters.
pub struct MdnsDiscovery {
    #[allow(dead_code)] // Will be used when mDNS discovery is implemented
    discovered_tx: watch::Sender<HashSet<DiscoveredPeer>>,
    discovered_rx: watch::Receiver<HashSet<DiscoveredPeer>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiscoveredPeer {
    pub node_id: NodeId,
    pub addr: String,
}

impl MdnsDiscovery {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(HashSet::new());
        Self {
            discovered_tx: tx,
            discovered_rx: rx,
        }
    }

    /// Start mDNS discovery in the background.
    pub async fn start(&self) -> galactica_common::Result<()> {
        tracing::info!("starting mDNS discovery");
        todo!("implement mDNS discovery with libp2p")
    }

    /// Get a receiver for discovered peers.
    pub fn discovered_peers(&self) -> watch::Receiver<HashSet<DiscoveredPeer>> {
        self.discovered_rx.clone()
    }
}

impl Default for MdnsDiscovery {
    fn default() -> Self {
        Self::new()
    }
}
