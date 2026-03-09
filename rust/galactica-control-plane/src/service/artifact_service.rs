use std::sync::Arc;

use tonic::{Request, Response, Status};

use galactica_common::proto::artifact::v1::artifact_service_server::ArtifactService;
use galactica_common::proto::artifact::v1::*;

use crate::state::store::StateStore;

pub struct ArtifactServiceImpl {
    _store: Arc<dyn StateStore>,
}

impl ArtifactServiceImpl {
    pub fn new(store: Arc<dyn StateStore>) -> Self {
        Self { _store: store }
    }
}

#[tonic::async_trait]
impl ArtifactService for ArtifactServiceImpl {
    async fn get_model_manifest(
        &self,
        _request: Request<GetModelManifestRequest>,
    ) -> Result<Response<GetModelManifestResponse>, Status> {
        todo!("implement get_model_manifest")
    }

    async fn list_models(
        &self,
        _request: Request<ListArtifactModelsRequest>,
    ) -> Result<Response<ListArtifactModelsResponse>, Status> {
        todo!("implement list_models")
    }

    type WatchDownloadStream =
        tokio_stream::wrappers::ReceiverStream<Result<DownloadProgress, Status>>;

    async fn watch_download(
        &self,
        _request: Request<WatchDownloadRequest>,
    ) -> Result<Response<Self::WatchDownloadStream>, Status> {
        todo!("implement watch_download")
    }
}
