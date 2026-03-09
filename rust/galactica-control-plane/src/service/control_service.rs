use std::sync::Arc;

use tonic::{Request, Response, Status};

use galactica_common::proto::common::v1::ClusterEvent;
use galactica_common::proto::control::v1::control_plane_server::ControlPlane;
use galactica_common::proto::control::v1::*;

use crate::state::store::StateStore;

pub struct ControlPlaneService {
    _store: Arc<dyn StateStore>,
}

impl ControlPlaneService {
    pub fn new(store: Arc<dyn StateStore>) -> Self {
        Self { _store: store }
    }
}

#[tonic::async_trait]
impl ControlPlane for ControlPlaneService {
    async fn register_node(
        &self,
        _request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        todo!("implement register_node")
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        todo!("implement heartbeat")
    }

    async fn report_capabilities(
        &self,
        _request: Request<ReportCapabilitiesRequest>,
    ) -> Result<Response<ReportCapabilitiesResponse>, Status> {
        todo!("implement report_capabilities")
    }

    async fn get_cluster_state(
        &self,
        _request: Request<GetClusterStateRequest>,
    ) -> Result<Response<GetClusterStateResponse>, Status> {
        todo!("implement get_cluster_state")
    }

    type WatchEventsStream =
        tokio_stream::wrappers::ReceiverStream<Result<ClusterEvent, Status>>;

    async fn watch_events(
        &self,
        _request: Request<WatchEventsRequest>,
    ) -> Result<Response<Self::WatchEventsStream>, Status> {
        todo!("implement watch_events")
    }

    async fn enroll_node(
        &self,
        _request: Request<EnrollNodeRequest>,
    ) -> Result<Response<EnrollNodeResponse>, Status> {
        todo!("implement enroll_node")
    }

    async fn authenticate(
        &self,
        _request: Request<AuthenticateRequest>,
    ) -> Result<Response<AuthenticateResponse>, Status> {
        todo!("implement authenticate")
    }
}
