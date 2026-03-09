use async_trait::async_trait;

use galactica_common::types::cluster::{ClusterState, ExecutionPool};
use galactica_common::types::model::ModelId;
use galactica_common::types::node::NodeId;

#[async_trait]
pub trait PlacementEngine: Send + Sync {
    async fn place_model(
        &self,
        model_id: &ModelId,
        state: &ClusterState,
    ) -> galactica_common::Result<Vec<NodeId>>;

    fn compatible_pools(&self, state: &ClusterState) -> Vec<ExecutionPool>;
}

pub struct DefaultPlacementEngine;

impl DefaultPlacementEngine {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultPlacementEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PlacementEngine for DefaultPlacementEngine {
    async fn place_model(
        &self,
        model_id: &ModelId,
        _state: &ClusterState,
    ) -> galactica_common::Result<Vec<NodeId>> {
        tracing::info!(?model_id, "computing model placement");
        todo!("implement placement algorithm")
    }

    fn compatible_pools(&self, state: &ClusterState) -> Vec<ExecutionPool> {
        super::pool::compute_execution_pools(state)
    }
}
