use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use galactica_common::types::node::{NodeCapabilities, NodeId, NodeStatus};

struct RegisteredNode {
    node_id: NodeId,
    _hostname: String,
    _capabilities: NodeCapabilities,
    status: NodeStatus,
    last_heartbeat: Instant,
}

pub struct NodeRegistry {
    nodes: Arc<RwLock<HashMap<String, RegisteredNode>>>,
    timeout: Duration,
}

impl NodeRegistry {
    pub fn new(timeout: Duration) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            timeout,
        }
    }

    pub fn register(&self, node_id: NodeId, hostname: String, capabilities: NodeCapabilities) {
        let mut nodes = self.nodes.write().unwrap();
        nodes.insert(
            node_id.0.clone(),
            RegisteredNode {
                node_id,
                _hostname: hostname,
                _capabilities: capabilities,
                status: NodeStatus::Registering,
                last_heartbeat: Instant::now(),
            },
        );
    }

    pub fn heartbeat(&self, node_id: &NodeId) -> bool {
        let mut nodes = self.nodes.write().unwrap();
        if let Some(node) = nodes.get_mut(&node_id.0) {
            node.last_heartbeat = Instant::now();
            node.status = NodeStatus::Online;
            true
        } else {
            false
        }
    }

    pub fn check_timeouts(&self) -> Vec<NodeId> {
        let mut timed_out = Vec::new();
        let mut nodes = self.nodes.write().unwrap();
        for node in nodes.values_mut() {
            if node.last_heartbeat.elapsed() > self.timeout && node.status == NodeStatus::Online {
                node.status = NodeStatus::Offline;
                timed_out.push(node.node_id.clone());
            }
        }
        timed_out
    }

    pub fn deregister(&self, node_id: &NodeId) {
        self.nodes.write().unwrap().remove(&node_id.0);
    }
}
