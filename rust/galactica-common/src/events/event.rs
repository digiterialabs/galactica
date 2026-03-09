use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::cluster::{InstanceId, InstanceStatus};
use crate::types::model::ModelId;
use crate::types::node::NodeId;
use crate::types::task::{TaskId, TaskStatus};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EventId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedEvent {
    pub event_id: EventId,
    pub timestamp: DateTime<Utc>,
    pub event: Event,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    NodeRegistered {
        node_id: NodeId,
        hostname: String,
    },
    NodeHeartbeat {
        node_id: NodeId,
    },
    NodeTimedOut {
        node_id: NodeId,
    },
    InstanceCreated {
        instance_id: InstanceId,
        model_id: ModelId,
        node_id: NodeId,
    },
    InstanceDeleted {
        instance_id: InstanceId,
    },
    InstanceStatusChanged {
        instance_id: InstanceId,
        status: InstanceStatus,
    },
    TaskCreated {
        task_id: TaskId,
        model_id: ModelId,
    },
    TaskStatusChanged {
        task_id: TaskId,
        status: TaskStatus,
    },
    DownloadProgress {
        model_id: ModelId,
        progress_percent: f32,
    },
}
