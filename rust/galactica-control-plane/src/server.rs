use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::Server;

use galactica_common::proto::artifact::v1::artifact_service_server::ArtifactServiceServer;
use galactica_common::proto::control::v1::control_plane_server::ControlPlaneServer;

use crate::service::artifact_service::ArtifactServiceImpl;
use crate::service::control_service::ControlPlaneService;
use crate::state::store::{InMemoryStateStore, StateStore};

pub async fn run_server(listen_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn StateStore> = Arc::new(InMemoryStateStore::new());

    let control_service = ControlPlaneService::new(store.clone());
    let artifact_service = ArtifactServiceImpl::new(store);

    tracing::info!(%listen_addr, "starting control plane gRPC server");

    Server::builder()
        .add_service(ControlPlaneServer::new(control_service))
        .add_service(ArtifactServiceServer::new(artifact_service))
        .serve(listen_addr)
        .await?;

    Ok(())
}
