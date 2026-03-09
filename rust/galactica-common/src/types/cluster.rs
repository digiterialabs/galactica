use serde::{Deserialize, Serialize};

use super::model::ModelId;
use super::node::{NodeCapabilities, NodeId, NodeStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    pub nodes: Vec<ClusterNode>,
    pub instances: Vec<ModelInstance>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub node_id: NodeId,
    pub hostname: String,
    pub capabilities: NodeCapabilities,
    pub status: NodeStatus,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPool {
    pub name: String,
    pub node_ids: Vec<NodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct InstanceId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInstance {
    pub instance_id: InstanceId,
    pub model_id: ModelId,
    pub node_id: NodeId,
    pub runtime: String,
    pub memory_used_bytes: u64,
    pub status: InstanceStatus,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum InstanceStatus {
    Loading,
    Ready,
    Unloading,
    Failed,
}
