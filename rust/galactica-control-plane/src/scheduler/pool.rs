use galactica_common::types::cluster::{ClusterState, ExecutionPool};
use galactica_common::types::node::NodeCapabilities;

/// Compute execution pools from the current cluster state.
pub fn compute_execution_pools(state: &ClusterState) -> Vec<ExecutionPool> {
    let mut pools = std::collections::HashMap::new();

    for node in &state.nodes {
        let label = pool_label(&node.capabilities);
        pools
            .entry(label)
            .or_insert_with(Vec::new)
            .push(node.node_id.clone());
    }

    pools
        .into_iter()
        .map(|(name, node_ids)| ExecutionPool { name, node_ids })
        .collect()
}

/// Generate a pool label from node capabilities.
pub fn pool_label(caps: &NodeCapabilities) -> String {
    let os = format!("{:?}", caps.os).to_lowercase();
    let arch = format!("{:?}", caps.cpu_arch).to_lowercase();
    let accel = caps
        .accelerators
        .first()
        .map(|a| format!("{:?}", a.accelerator_type).to_lowercase())
        .unwrap_or_else(|| "cpu".to_string());
    format!("{os}-{accel}-{arch}")
}
