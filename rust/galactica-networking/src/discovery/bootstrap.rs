use galactica_common::types::node::NodeId;

/// Bootstrap-based discovery for cloud/WAN/hybrid deployments.
///
/// Nodes register with a known bootstrap server and discover
/// peers through it, rather than relying on multicast.
pub struct BootstrapDiscovery {
    bootstrap_addr: String,
}

#[derive(Debug, Clone)]
pub struct RegisteredNode {
    pub node_id: NodeId,
    pub addr: String,
}

impl BootstrapDiscovery {
    pub fn new(bootstrap_addr: String) -> Self {
        Self { bootstrap_addr }
    }

    /// Register this node with the bootstrap server.
    pub async fn register(&self, _node_id: &NodeId, _addr: &str) -> galactica_common::Result<()> {
        tracing::info!(addr = %self.bootstrap_addr, "registering with bootstrap server");
        todo!("implement bootstrap registration")
    }

    /// Discover peers via the bootstrap server.
    pub async fn discover(&self) -> galactica_common::Result<Vec<RegisteredNode>> {
        tracing::info!(addr = %self.bootstrap_addr, "discovering peers via bootstrap");
        todo!("implement bootstrap discovery")
    }
}
