use tonic::{Request, Response, Status};

use galactica_common::proto::node::v1::node_agent_server::NodeAgent;
use galactica_common::proto::node::v1::{
    ExecuteTaskRequest, ExecuteTaskResponse, GetStatusRequest, GetStatusResponse,
    LoadModelRequest, LoadModelResponse, UnloadModelRequest, UnloadModelResponse,
};

pub struct NodeAgentService;

impl NodeAgentService {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NodeAgentService {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl NodeAgent for NodeAgentService {
    async fn execute_task(
        &self,
        _request: Request<ExecuteTaskRequest>,
    ) -> Result<Response<ExecuteTaskResponse>, Status> {
        todo!("implement execute_task")
    }

    async fn load_model(
        &self,
        _request: Request<LoadModelRequest>,
    ) -> Result<Response<LoadModelResponse>, Status> {
        todo!("implement load_model")
    }

    async fn unload_model(
        &self,
        _request: Request<UnloadModelRequest>,
    ) -> Result<Response<UnloadModelResponse>, Status> {
        todo!("implement unload_model")
    }

    async fn get_status(
        &self,
        _request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        todo!("implement get_status")
    }
}
