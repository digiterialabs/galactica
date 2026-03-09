// Galactica common types, errors, inference payloads, and proto-generated code.

pub mod error;
pub mod inference;

use chrono::{DateTime, Utc};

pub mod proto {
    pub mod common {
        pub mod v1 {
            tonic::include_proto!("galactica.common.v1");
        }
    }
    pub mod control {
        pub mod v1 {
            tonic::include_proto!("galactica.control.v1");
        }
    }
    pub mod node {
        pub mod v1 {
            tonic::include_proto!("galactica.node.v1");
        }
    }
    pub mod gateway {
        pub mod v1 {
            tonic::include_proto!("galactica.gateway.v1");
        }
    }
    pub mod artifact {
        pub mod v1 {
            tonic::include_proto!("galactica.artifact.v1");
        }
    }
    pub mod runtime {
        pub mod v1 {
            tonic::include_proto!("galactica.runtime.v1");
        }
    }
}

pub use error::{GalacticaError, Result};
pub use inference::{
    ChatTurn, InferenceChunk, InferenceParameters, InferenceRequest, InferenceResponse,
    InferenceUsage, NodeExecutionPayload,
};

pub fn chrono_to_timestamp(value: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

pub fn timestamp_to_chrono(value: prost_types::Timestamp) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(value.seconds, value.nanos as u32)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}
