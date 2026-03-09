use std::collections::{HashMap, HashSet};

use galactica_common::types::node::NodeId;

/// Graph of node connectivity for topology-aware scheduling.
pub struct TopologyGraph {
    adjacency: HashMap<String, HashSet<String>>,
}

impl TopologyGraph {
    pub fn new() -> Self {
        Self {
            adjacency: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node_id: &NodeId) {
        self.adjacency.entry(node_id.0.clone()).or_default();
    }

    pub fn remove_node(&mut self, node_id: &NodeId) {
        self.adjacency.remove(&node_id.0);
        for edges in self.adjacency.values_mut() {
            edges.remove(&node_id.0);
        }
    }

    pub fn add_edge(&mut self, from: &NodeId, to: &NodeId) {
        self.adjacency
            .entry(from.0.clone())
            .or_default()
            .insert(to.0.clone());
        self.adjacency
            .entry(to.0.clone())
            .or_default()
            .insert(from.0.clone());
    }

    pub fn neighbors(&self, node_id: &NodeId) -> Vec<NodeId> {
        self.adjacency
            .get(&node_id.0)
            .map(|edges| edges.iter().map(|id| NodeId(id.clone())).collect())
            .unwrap_or_default()
    }
}

impl Default for TopologyGraph {
    fn default() -> Self {
        Self::new()
    }
}
