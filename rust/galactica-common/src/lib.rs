// Galactica common types, errors, and proto-generated code.

pub mod error;
pub mod events;
pub mod types;

pub use error::{GalacticaError, Result};

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
