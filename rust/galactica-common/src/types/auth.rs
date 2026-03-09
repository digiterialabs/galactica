use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::node::NodeId;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TenantId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrollmentToken {
    pub token: String,
    pub expires_at: DateTime<Utc>,
    pub tenant_id: TenantId,
    pub allowed_roles: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeIdentity {
    pub node_id: NodeId,
    pub certificate_pem: String,
    pub private_key_pem: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantCredentials {
    pub tenant_id: TenantId,
    pub api_key: String,
    pub scopes: Vec<String>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthPolicy {
    pub tenant_id: TenantId,
    pub require_mtls: bool,
    pub max_requests_per_minute: u32,
    pub allowed_models: Vec<String>,
    pub allowed_node_pools: Vec<String>,
}
