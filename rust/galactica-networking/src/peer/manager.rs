use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use galactica_common::types::node::NodeId;

/// Information about a connected peer.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub node_id: NodeId,
    pub addr: String,
    pub connected_at: std::time::Instant,
}

/// Manages connections to peer nodes.
#[derive(Debug, Clone)]
pub struct PeerManager {
    peers: Arc<RwLock<HashMap<String, PeerInfo>>>,
}

impl PeerManager {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Connect to a peer node.
    pub async fn connect(&self, node_id: NodeId, addr: &str) -> galactica_common::Result<()> {
        tracing::info!(%addr, "connecting to peer");
        let info = PeerInfo {
            node_id,
            addr: addr.to_string(),
            connected_at: std::time::Instant::now(),
        };
        self.peers.write().unwrap().insert(addr.to_string(), info);
        Ok(())
    }

    /// Disconnect from a peer node.
    pub async fn disconnect(&self, addr: &str) -> galactica_common::Result<()> {
        tracing::info!(%addr, "disconnecting from peer");
        self.peers.write().unwrap().remove(addr);
        Ok(())
    }

    /// Get all currently connected peers.
    pub fn connected_peers(&self) -> Vec<PeerInfo> {
        self.peers.read().unwrap().values().cloned().collect()
    }
}

impl Default for PeerManager {
    fn default() -> Self {
        Self::new()
    }
}
