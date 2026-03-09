use serde::{Deserialize, Serialize};

use crate::types::cluster::InstanceId;
use crate::types::model::ModelId;
use crate::types::node::NodeId;
use crate::types::task::{InferenceRequest, TaskId};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CommandId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    PlaceModel {
        model_id: ModelId,
        node_id: NodeId,
        runtime: String,
        quantization: String,
    },
    UnloadModel {
        instance_id: InstanceId,
    },
    InferenceRequest {
        task_id: TaskId,
        instance_id: InstanceId,
        request: InferenceRequest,
    },
    CancelInference {
        task_id: TaskId,
    },
    DrainNode {
        node_id: NodeId,
    },
}
