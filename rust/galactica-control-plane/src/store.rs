use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use galactica_common::proto::common;
use galactica_common::{GalacticaError, Result, chrono_to_timestamp};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use tokio::sync::{RwLock, broadcast};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub node_id: String,
    pub hostname: String,
    pub agent_endpoint: String,
    #[serde(with = "node_capabilities_serde")]
    pub capabilities: common::v1::NodeCapabilities,
    pub status: i32,
    pub last_heartbeat: DateTime<Utc>,
    pub registered_at: DateTime<Utc>,
    pub version: String,
    #[serde(with = "option_memory_serde")]
    pub system_memory: Option<common::v1::Memory>,
}

impl NodeRecord {
    pub fn to_proto(&self) -> common::v1::NodeInfo {
        common::v1::NodeInfo {
            node_id: Some(common::v1::NodeId {
                value: self.node_id.clone(),
            }),
            hostname: self.hostname.clone(),
            capabilities: Some(self.capabilities.clone()),
            status: self.status,
            last_heartbeat: Some(chrono_to_timestamp(self.last_heartbeat)),
            version: self.version.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelInstanceRecord {
    pub instance_id: String,
    pub model_id: String,
    pub node_id: String,
    pub runtime: String,
    pub quantization: String,
    pub memory_used_bytes: u64,
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default)]
    pub shard_index: Option<u32>,
    #[serde(default)]
    pub shard_count: Option<u32>,
    #[serde(default)]
    pub is_coordinator: bool,
    #[serde(default)]
    pub backend_family: Option<String>,
    pub status: InstanceStatus,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DistributedGroupRecord {
    pub group_id: String,
    pub model_id: String,
    pub runtime: String,
    pub quantization: String,
    pub backend_family: String,
    pub coordinator_node_id: String,
    pub shard_count: u32,
    pub node_ids: Vec<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskRecord {
    pub task_id: String,
    pub request_id: String,
    pub model_id: String,
    pub node_id: String,
    pub instance_id: String,
    pub prompt: String,
    pub status: i32,
    pub result: Option<String>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DownloadRecord {
    pub model_id: String,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ClusterState {
    pub nodes: BTreeMap<String, NodeRecord>,
    pub instances: BTreeMap<String, ModelInstanceRecord>,
    pub groups: BTreeMap<String, DistributedGroupRecord>,
    pub tasks: BTreeMap<String, TaskRecord>,
    pub downloads: BTreeMap<String, DownloadRecord>,
    pub last_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceStatus {
    Loading,
    Ready,
    Running,
    Failed,
    Unloaded,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ControlEvent {
    NodeRegistered {
        node: NodeRecord,
    },
    NodeHeartbeat {
        node_id: String,
        observed_at: DateTime<Utc>,
        #[serde(with = "option_memory_serde")]
        system_memory: Option<common::v1::Memory>,
    },
    NodeTimedOut {
        node_id: String,
    },
    NodeRemoved {
        node_id: String,
        reason: String,
    },
    NodeCapabilitiesUpdated {
        node_id: String,
        #[serde(with = "node_capabilities_serde")]
        capabilities: common::v1::NodeCapabilities,
    },
    InstanceCreated {
        instance: ModelInstanceRecord,
    },
    GroupCreated {
        group: DistributedGroupRecord,
    },
    GroupDeleted {
        group_id: String,
    },
    InstanceDeleted {
        instance_id: String,
    },
    InstanceStatusChanged {
        instance_id: String,
        status: InstanceStatus,
    },
    TaskCreated {
        task: TaskRecord,
    },
    TaskStatusChanged {
        task_id: String,
        status: i32,
        result: Option<String>,
        error_message: Option<String>,
    },
    DownloadProgress {
        download: DownloadRecord,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub sequence: u64,
    pub event_id: String,
    pub timestamp: DateTime<Utc>,
    pub event: ControlEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CredentialKind {
    ApiKey,
    CertificateFingerprint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantRecord {
    pub tenant_id: String,
    pub api_key: String,
    pub scopes: Vec<String>,
    pub require_mtls: bool,
    pub max_requests_per_minute: u32,
    pub allowed_models: Vec<String>,
    pub allowed_node_pools: Vec<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollmentTokenRecord {
    pub token: String,
    pub tenant_id: String,
    pub allowed_roles: Vec<String>,
    pub expires_at: DateTime<Utc>,
    pub used_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIdentityRecord {
    pub node_id: String,
    pub tenant_id: String,
    pub certificate_pem: String,
    pub private_key_pem: String,
    pub fingerprint: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub hostname: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthContext {
    pub tenant_id: String,
    pub scopes: Vec<String>,
    pub expires_at: DateTime<Utc>,
    pub require_mtls: bool,
    pub max_requests_per_minute: u32,
    pub allowed_models: Vec<String>,
    pub allowed_node_pools: Vec<String>,
    pub actor: String,
    pub credential_kind: CredentialKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditRecord {
    pub event_id: String,
    pub timestamp: DateTime<Utc>,
    pub actor: String,
    pub action: String,
    pub resource: String,
    pub outcome: String,
    pub details: BTreeMap<String, String>,
}

pub fn event_apply(state: &ClusterState, event: &EventEnvelope) -> ClusterState {
    let mut next = state.clone();
    next.last_sequence = event.sequence;

    match &event.event {
        ControlEvent::NodeRegistered { node } => {
            next.nodes.insert(node.node_id.clone(), node.clone());
        }
        ControlEvent::NodeHeartbeat {
            node_id,
            observed_at,
            system_memory,
        } => {
            if let Some(node) = next.nodes.get_mut(node_id) {
                node.last_heartbeat = *observed_at;
                node.status = common::v1::NodeStatus::Online as i32;
                node.system_memory = *system_memory;
            }
        }
        ControlEvent::NodeTimedOut { node_id } => {
            if let Some(node) = next.nodes.get_mut(node_id) {
                node.status = common::v1::NodeStatus::Offline as i32;
            }
        }
        ControlEvent::NodeRemoved { node_id, .. } => {
            next.nodes.remove(node_id);
            next.instances
                .retain(|_, instance| instance.node_id != *node_id);
        }
        ControlEvent::NodeCapabilitiesUpdated {
            node_id,
            capabilities,
        } => {
            if let Some(node) = next.nodes.get_mut(node_id) {
                node.capabilities = capabilities.clone();
            }
        }
        ControlEvent::InstanceCreated { instance } => {
            next.instances
                .insert(instance.instance_id.clone(), instance.clone());
        }
        ControlEvent::GroupCreated { group } => {
            next.groups.insert(group.group_id.clone(), group.clone());
        }
        ControlEvent::GroupDeleted { group_id } => {
            next.groups.remove(group_id);
        }
        ControlEvent::InstanceDeleted { instance_id } => {
            next.instances.remove(instance_id);
        }
        ControlEvent::InstanceStatusChanged {
            instance_id,
            status,
        } => {
            if let Some(instance) = next.instances.get_mut(instance_id) {
                instance.status = *status;
                instance.updated_at = event.timestamp;
            }
        }
        ControlEvent::TaskCreated { task } => {
            next.tasks.insert(task.task_id.clone(), task.clone());
        }
        ControlEvent::TaskStatusChanged {
            task_id,
            status,
            result,
            error_message,
        } => {
            if let Some(task) = next.tasks.get_mut(task_id) {
                task.status = *status;
                task.result = result.clone();
                task.error_message = error_message.clone();
                task.updated_at = event.timestamp;
            }
        }
        ControlEvent::DownloadProgress { download } => {
            next.downloads
                .insert(download.model_id.clone(), download.clone());
        }
    }

    next
}

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn get_state(&self) -> Result<ClusterState>;
    async fn apply_event(&self, event: ControlEvent) -> Result<EventEnvelope>;
    async fn get_events_since(&self, sequence: u64) -> Result<Vec<EventEnvelope>>;
    async fn get_node(&self, node_id: &str) -> Result<Option<NodeRecord>>;
    async fn list_nodes(&self) -> Result<Vec<NodeRecord>>;
    async fn update_node_capabilities(
        &self,
        node_id: &str,
        capabilities: common::v1::NodeCapabilities,
    ) -> Result<()>;
    async fn upsert_tenant(&self, tenant: TenantRecord) -> Result<()>;
    async fn get_tenant(&self, tenant_id: &str) -> Result<Option<TenantRecord>>;
    async fn find_tenant_by_api_key(&self, api_key: &str) -> Result<Option<TenantRecord>>;
    async fn save_enrollment_token(&self, token: EnrollmentTokenRecord) -> Result<()>;
    async fn consume_enrollment_token(&self, token: &str) -> Result<Option<EnrollmentTokenRecord>>;
    async fn save_node_identity(&self, identity: NodeIdentityRecord) -> Result<()>;
    async fn find_node_identity_by_fingerprint(
        &self,
        fingerprint: &str,
    ) -> Result<Option<NodeIdentityRecord>>;
    async fn append_audit_record(&self, record: AuditRecord) -> Result<()>;
    async fn list_audit_records(&self) -> Result<Vec<AuditRecord>>;
    fn subscribe(&self) -> broadcast::Receiver<EventEnvelope>;
}

#[derive(Clone)]
pub struct InMemoryStateStore {
    inner: Arc<RwLock<StoreInner>>,
    event_sender: broadcast::Sender<EventEnvelope>,
}

#[derive(Default)]
struct StoreInner {
    state: ClusterState,
    events: Vec<EventEnvelope>,
    tenants: BTreeMap<String, TenantRecord>,
    enrollment_tokens: BTreeMap<String, EnrollmentTokenRecord>,
    node_identities: BTreeMap<String, NodeIdentityRecord>,
    audit: Vec<AuditRecord>,
}

impl Default for InMemoryStateStore {
    fn default() -> Self {
        let (event_sender, _) = broadcast::channel(256);
        Self {
            inner: Arc::new(RwLock::new(StoreInner::default())),
            event_sender,
        }
    }
}

#[async_trait]
impl StateStore for InMemoryStateStore {
    async fn get_state(&self) -> Result<ClusterState> {
        Ok(self.inner.read().await.state.clone())
    }

    async fn apply_event(&self, event: ControlEvent) -> Result<EventEnvelope> {
        let mut inner = self.inner.write().await;
        let envelope = EventEnvelope {
            sequence: inner.state.last_sequence + 1,
            event_id: format!("evt-{}", uuid::Uuid::new_v4()),
            timestamp: Utc::now(),
            event,
        };
        inner.state = event_apply(&inner.state, &envelope);
        inner.events.push(envelope.clone());
        let _ = self.event_sender.send(envelope.clone());
        Ok(envelope)
    }

    async fn get_events_since(&self, sequence: u64) -> Result<Vec<EventEnvelope>> {
        Ok(self
            .inner
            .read()
            .await
            .events
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect())
    }

    async fn get_node(&self, node_id: &str) -> Result<Option<NodeRecord>> {
        Ok(self.inner.read().await.state.nodes.get(node_id).cloned())
    }

    async fn list_nodes(&self) -> Result<Vec<NodeRecord>> {
        Ok(self
            .inner
            .read()
            .await
            .state
            .nodes
            .values()
            .cloned()
            .collect())
    }

    async fn update_node_capabilities(
        &self,
        node_id: &str,
        capabilities: common::v1::NodeCapabilities,
    ) -> Result<()> {
        if self.get_node(node_id).await?.is_none() {
            return Err(GalacticaError::not_found(format!(
                "unknown node: {node_id}"
            )));
        }
        self.apply_event(ControlEvent::NodeCapabilitiesUpdated {
            node_id: node_id.to_string(),
            capabilities,
        })
        .await?;
        Ok(())
    }

    async fn upsert_tenant(&self, tenant: TenantRecord) -> Result<()> {
        self.inner
            .write()
            .await
            .tenants
            .insert(tenant.tenant_id.clone(), tenant);
        Ok(())
    }

    async fn get_tenant(&self, tenant_id: &str) -> Result<Option<TenantRecord>> {
        Ok(self.inner.read().await.tenants.get(tenant_id).cloned())
    }

    async fn find_tenant_by_api_key(&self, api_key: &str) -> Result<Option<TenantRecord>> {
        Ok(self
            .inner
            .read()
            .await
            .tenants
            .values()
            .find(|tenant| tenant.api_key == api_key)
            .cloned())
    }

    async fn save_enrollment_token(&self, token: EnrollmentTokenRecord) -> Result<()> {
        self.inner
            .write()
            .await
            .enrollment_tokens
            .insert(token.token.clone(), token);
        Ok(())
    }

    async fn consume_enrollment_token(&self, token: &str) -> Result<Option<EnrollmentTokenRecord>> {
        let mut inner = self.inner.write().await;
        let Some(record) = inner.enrollment_tokens.get_mut(token) else {
            return Ok(None);
        };
        if record.used_at.is_some() || record.expires_at <= Utc::now() {
            return Ok(None);
        }
        record.used_at = Some(Utc::now());
        Ok(Some(record.clone()))
    }

    async fn save_node_identity(&self, identity: NodeIdentityRecord) -> Result<()> {
        self.inner
            .write()
            .await
            .node_identities
            .insert(identity.node_id.clone(), identity);
        Ok(())
    }

    async fn find_node_identity_by_fingerprint(
        &self,
        fingerprint: &str,
    ) -> Result<Option<NodeIdentityRecord>> {
        Ok(self
            .inner
            .read()
            .await
            .node_identities
            .values()
            .find(|identity| identity.fingerprint == fingerprint)
            .cloned())
    }

    async fn append_audit_record(&self, record: AuditRecord) -> Result<()> {
        self.inner.write().await.audit.push(record);
        Ok(())
    }

    async fn list_audit_records(&self) -> Result<Vec<AuditRecord>> {
        Ok(self.inner.read().await.audit.clone())
    }

    fn subscribe(&self) -> broadcast::Receiver<EventEnvelope> {
        self.event_sender.subscribe()
    }
}

#[derive(Clone)]
pub struct SqliteStateStore {
    pool: SqlitePool,
    state: Arc<RwLock<ClusterState>>,
    event_sender: broadcast::Sender<EventEnvelope>,
}

impl SqliteStateStore {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .map_err(|error| {
                GalacticaError::internal(format!("failed to open sqlite state store: {error}"))
            })?;
        init_sqlite_schema(&pool).await?;
        let state = load_snapshot(&pool).await?.unwrap_or_default();
        let (event_sender, _) = broadcast::channel(256);
        Ok(Self {
            pool,
            state: Arc::new(RwLock::new(state)),
            event_sender,
        })
    }
}

#[async_trait]
impl StateStore for SqliteStateStore {
    async fn get_state(&self) -> Result<ClusterState> {
        Ok(self.state.read().await.clone())
    }

    async fn apply_event(&self, event: ControlEvent) -> Result<EventEnvelope> {
        let mut state = self.state.write().await;
        let envelope = EventEnvelope {
            sequence: state.last_sequence + 1,
            event_id: format!("evt-{}", uuid::Uuid::new_v4()),
            timestamp: Utc::now(),
            event,
        };
        let next_state = event_apply(&state, &envelope);
        let event_json = to_json(&envelope)?;
        let snapshot_json = to_json(&next_state)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(sqlite_error("begin transaction"))?;
        sqlx::query(
            "INSERT INTO control_events(sequence, event_id, timestamp, payload_json)
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(envelope.sequence as i64)
        .bind(&envelope.event_id)
        .bind(envelope.timestamp.to_rfc3339())
        .bind(event_json)
        .execute(&mut *tx)
        .await
        .map_err(sqlite_error("insert control event"))?;
        sqlx::query(
            "INSERT INTO state_snapshots(id, state_json, last_sequence, updated_at)
             VALUES (1, ?1, ?2, ?3)
             ON CONFLICT(id) DO UPDATE SET state_json = excluded.state_json,
                 last_sequence = excluded.last_sequence,
                 updated_at = excluded.updated_at",
        )
        .bind(snapshot_json)
        .bind(next_state.last_sequence as i64)
        .bind(envelope.timestamp.to_rfc3339())
        .execute(&mut *tx)
        .await
        .map_err(sqlite_error("upsert state snapshot"))?;
        tx.commit()
            .await
            .map_err(sqlite_error("commit transaction"))?;
        *state = next_state;
        drop(state);
        let _ = self.event_sender.send(envelope.clone());
        Ok(envelope)
    }

    async fn get_events_since(&self, sequence: u64) -> Result<Vec<EventEnvelope>> {
        let rows = sqlx::query(
            "SELECT payload_json FROM control_events WHERE sequence > ?1 ORDER BY sequence ASC",
        )
        .bind(sequence as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(sqlite_error("load control events"))?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.get("payload_json");
                from_json(&payload)
            })
            .collect()
    }

    async fn get_node(&self, node_id: &str) -> Result<Option<NodeRecord>> {
        Ok(self.state.read().await.nodes.get(node_id).cloned())
    }

    async fn list_nodes(&self) -> Result<Vec<NodeRecord>> {
        Ok(self.state.read().await.nodes.values().cloned().collect())
    }

    async fn update_node_capabilities(
        &self,
        node_id: &str,
        capabilities: common::v1::NodeCapabilities,
    ) -> Result<()> {
        if self.get_node(node_id).await?.is_none() {
            return Err(GalacticaError::not_found(format!(
                "unknown node: {node_id}"
            )));
        }
        self.apply_event(ControlEvent::NodeCapabilitiesUpdated {
            node_id: node_id.to_string(),
            capabilities,
        })
        .await?;
        Ok(())
    }

    async fn upsert_tenant(&self, tenant: TenantRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO tenants(
                tenant_id, api_key, scopes_json, require_mtls, max_requests_per_minute,
                allowed_models_json, allowed_node_pools_json, expires_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(tenant_id) DO UPDATE SET
                api_key = excluded.api_key,
                scopes_json = excluded.scopes_json,
                require_mtls = excluded.require_mtls,
                max_requests_per_minute = excluded.max_requests_per_minute,
                allowed_models_json = excluded.allowed_models_json,
                allowed_node_pools_json = excluded.allowed_node_pools_json,
                expires_at = excluded.expires_at",
        )
        .bind(&tenant.tenant_id)
        .bind(&tenant.api_key)
        .bind(to_json(&tenant.scopes)?)
        .bind(tenant.require_mtls as i64)
        .bind(i64::from(tenant.max_requests_per_minute))
        .bind(to_json(&tenant.allowed_models)?)
        .bind(to_json(&tenant.allowed_node_pools)?)
        .bind(tenant.expires_at.map(|value| value.to_rfc3339()))
        .execute(&self.pool)
        .await
        .map_err(sqlite_error("upsert tenant"))?;
        Ok(())
    }

    async fn get_tenant(&self, tenant_id: &str) -> Result<Option<TenantRecord>> {
        let row = sqlx::query(
            "SELECT tenant_id, api_key, scopes_json, require_mtls, max_requests_per_minute,
                    allowed_models_json, allowed_node_pools_json, expires_at
             FROM tenants WHERE tenant_id = ?1",
        )
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(sqlite_error("load tenant"))?;
        row.map(row_to_tenant).transpose()
    }

    async fn find_tenant_by_api_key(&self, api_key: &str) -> Result<Option<TenantRecord>> {
        let row = sqlx::query(
            "SELECT tenant_id, api_key, scopes_json, require_mtls, max_requests_per_minute,
                    allowed_models_json, allowed_node_pools_json, expires_at
             FROM tenants WHERE api_key = ?1",
        )
        .bind(api_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(sqlite_error("find tenant by api key"))?;
        row.map(row_to_tenant).transpose()
    }

    async fn save_enrollment_token(&self, token: EnrollmentTokenRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO enrollment_tokens(token, tenant_id, allowed_roles_json, expires_at, used_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(token) DO UPDATE SET
                tenant_id = excluded.tenant_id,
                allowed_roles_json = excluded.allowed_roles_json,
                expires_at = excluded.expires_at,
                used_at = excluded.used_at",
        )
        .bind(&token.token)
        .bind(&token.tenant_id)
        .bind(to_json(&token.allowed_roles)?)
        .bind(token.expires_at.to_rfc3339())
        .bind(token.used_at.map(|value| value.to_rfc3339()))
        .execute(&self.pool)
        .await
        .map_err(sqlite_error("save enrollment token"))?;
        Ok(())
    }

    async fn consume_enrollment_token(&self, token: &str) -> Result<Option<EnrollmentTokenRecord>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(sqlite_error("begin token transaction"))?;
        let row = sqlx::query(
            "SELECT token, tenant_id, allowed_roles_json, expires_at, used_at
             FROM enrollment_tokens WHERE token = ?1",
        )
        .bind(token)
        .fetch_optional(&mut *tx)
        .await
        .map_err(sqlite_error("load enrollment token"))?;
        let Some(row) = row else {
            tx.commit()
                .await
                .map_err(sqlite_error("commit token transaction"))?;
            return Ok(None);
        };
        let record = row_to_enrollment_token(row)?;
        if record.used_at.is_some() || record.expires_at <= Utc::now() {
            tx.commit()
                .await
                .map_err(sqlite_error("commit token transaction"))?;
            return Ok(None);
        }
        let used_at = Utc::now();
        sqlx::query("UPDATE enrollment_tokens SET used_at = ?2 WHERE token = ?1")
            .bind(token)
            .bind(used_at.to_rfc3339())
            .execute(&mut *tx)
            .await
            .map_err(sqlite_error("consume enrollment token"))?;
        tx.commit()
            .await
            .map_err(sqlite_error("commit token transaction"))?;
        Ok(Some(EnrollmentTokenRecord {
            used_at: Some(used_at),
            ..record
        }))
    }

    async fn save_node_identity(&self, identity: NodeIdentityRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO node_identities(
                node_id, tenant_id, certificate_pem, private_key_pem, fingerprint,
                issued_at, expires_at, hostname
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(node_id) DO UPDATE SET
                tenant_id = excluded.tenant_id,
                certificate_pem = excluded.certificate_pem,
                private_key_pem = excluded.private_key_pem,
                fingerprint = excluded.fingerprint,
                issued_at = excluded.issued_at,
                expires_at = excluded.expires_at,
                hostname = excluded.hostname",
        )
        .bind(&identity.node_id)
        .bind(&identity.tenant_id)
        .bind(&identity.certificate_pem)
        .bind(&identity.private_key_pem)
        .bind(&identity.fingerprint)
        .bind(identity.issued_at.to_rfc3339())
        .bind(identity.expires_at.to_rfc3339())
        .bind(&identity.hostname)
        .execute(&self.pool)
        .await
        .map_err(sqlite_error("save node identity"))?;
        Ok(())
    }

    async fn find_node_identity_by_fingerprint(
        &self,
        fingerprint: &str,
    ) -> Result<Option<NodeIdentityRecord>> {
        let row = sqlx::query(
            "SELECT node_id, tenant_id, certificate_pem, private_key_pem, fingerprint,
                    issued_at, expires_at, hostname
             FROM node_identities WHERE fingerprint = ?1",
        )
        .bind(fingerprint)
        .fetch_optional(&self.pool)
        .await
        .map_err(sqlite_error("find node identity"))?;
        row.map(row_to_identity).transpose()
    }

    async fn append_audit_record(&self, record: AuditRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO audit_log(event_id, timestamp, actor, action, resource, outcome, details_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )
        .bind(&record.event_id)
        .bind(record.timestamp.to_rfc3339())
        .bind(&record.actor)
        .bind(&record.action)
        .bind(&record.resource)
        .bind(&record.outcome)
        .bind(to_json(&record.details)?)
        .execute(&self.pool)
        .await
        .map_err(sqlite_error("append audit record"))?;
        Ok(())
    }

    async fn list_audit_records(&self) -> Result<Vec<AuditRecord>> {
        let rows = sqlx::query(
            "SELECT event_id, timestamp, actor, action, resource, outcome, details_json
             FROM audit_log ORDER BY timestamp ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(sqlite_error("load audit log"))?;
        rows.into_iter().map(row_to_audit).collect()
    }

    fn subscribe(&self) -> broadcast::Receiver<EventEnvelope> {
        self.event_sender.subscribe()
    }
}

async fn init_sqlite_schema(pool: &SqlitePool) -> Result<()> {
    for statement in [
        "CREATE TABLE IF NOT EXISTS control_events(
            sequence INTEGER PRIMARY KEY,
            event_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS state_snapshots(
            id INTEGER PRIMARY KEY CHECK(id = 1),
            state_json TEXT NOT NULL,
            last_sequence INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS tenants(
            tenant_id TEXT PRIMARY KEY,
            api_key TEXT NOT NULL UNIQUE,
            scopes_json TEXT NOT NULL,
            require_mtls INTEGER NOT NULL,
            max_requests_per_minute INTEGER NOT NULL,
            allowed_models_json TEXT NOT NULL,
            allowed_node_pools_json TEXT NOT NULL,
            expires_at TEXT
        )",
        "CREATE TABLE IF NOT EXISTS enrollment_tokens(
            token TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            allowed_roles_json TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            used_at TEXT
        )",
        "CREATE TABLE IF NOT EXISTS node_identities(
            node_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            certificate_pem TEXT NOT NULL,
            private_key_pem TEXT NOT NULL,
            fingerprint TEXT NOT NULL UNIQUE,
            issued_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            hostname TEXT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS audit_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            actor TEXT NOT NULL,
            action TEXT NOT NULL,
            resource TEXT NOT NULL,
            outcome TEXT NOT NULL,
            details_json TEXT NOT NULL
        )",
    ] {
        sqlx::query(statement)
            .execute(pool)
            .await
            .map_err(sqlite_error("initialize sqlite schema"))?;
    }
    Ok(())
}

async fn load_snapshot(pool: &SqlitePool) -> Result<Option<ClusterState>> {
    let row = sqlx::query("SELECT state_json FROM state_snapshots WHERE id = 1")
        .fetch_optional(pool)
        .await
        .map_err(sqlite_error("load sqlite snapshot"))?;
    row.map(|row| {
        let payload: String = row.get("state_json");
        from_json(&payload)
    })
    .transpose()
}

fn row_to_tenant(row: sqlx::sqlite::SqliteRow) -> Result<TenantRecord> {
    Ok(TenantRecord {
        tenant_id: row.get("tenant_id"),
        api_key: row.get("api_key"),
        scopes: from_json(&row.get::<String, _>("scopes_json"))?,
        require_mtls: row.get::<i64, _>("require_mtls") != 0,
        max_requests_per_minute: row.get::<i64, _>("max_requests_per_minute") as u32,
        allowed_models: from_json(&row.get::<String, _>("allowed_models_json"))?,
        allowed_node_pools: from_json(&row.get::<String, _>("allowed_node_pools_json"))?,
        expires_at: parse_optional_datetime(row.get("expires_at"))?,
    })
}

fn row_to_enrollment_token(row: sqlx::sqlite::SqliteRow) -> Result<EnrollmentTokenRecord> {
    Ok(EnrollmentTokenRecord {
        token: row.get("token"),
        tenant_id: row.get("tenant_id"),
        allowed_roles: from_json(&row.get::<String, _>("allowed_roles_json"))?,
        expires_at: parse_datetime(&row.get::<String, _>("expires_at"))?,
        used_at: parse_optional_datetime(row.get("used_at"))?,
    })
}

fn row_to_identity(row: sqlx::sqlite::SqliteRow) -> Result<NodeIdentityRecord> {
    Ok(NodeIdentityRecord {
        node_id: row.get("node_id"),
        tenant_id: row.get("tenant_id"),
        certificate_pem: row.get("certificate_pem"),
        private_key_pem: row.get("private_key_pem"),
        fingerprint: row.get("fingerprint"),
        issued_at: parse_datetime(&row.get::<String, _>("issued_at"))?,
        expires_at: parse_datetime(&row.get::<String, _>("expires_at"))?,
        hostname: row.get("hostname"),
    })
}

fn row_to_audit(row: sqlx::sqlite::SqliteRow) -> Result<AuditRecord> {
    Ok(AuditRecord {
        event_id: row.get("event_id"),
        timestamp: parse_datetime(&row.get::<String, _>("timestamp"))?,
        actor: row.get("actor"),
        action: row.get("action"),
        resource: row.get("resource"),
        outcome: row.get("outcome"),
        details: from_json(&row.get::<String, _>("details_json"))?,
    })
}

fn sqlite_error(action: &'static str) -> impl Fn(sqlx::Error) -> GalacticaError {
    move |error| GalacticaError::internal(format!("{action}: {error}"))
}

fn parse_datetime(value: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| GalacticaError::internal(format!("invalid timestamp {value}: {error}")))
}

fn parse_optional_datetime(value: Option<String>) -> Result<Option<DateTime<Utc>>> {
    value.map(|value| parse_datetime(&value)).transpose()
}

fn to_json<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|error| {
        GalacticaError::internal(format!("failed to encode json payload: {error}"))
    })
}

fn from_json<T: DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value).map_err(|error| {
        GalacticaError::internal(format!("failed to decode json payload: {error}"))
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredMemory {
    total_bytes: u64,
    available_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredAcceleratorInfo {
    accelerator_type: i32,
    name: String,
    vram: Option<StoredMemory>,
    compute_capability: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredNodeCapabilities {
    os: i32,
    cpu_arch: i32,
    accelerators: Vec<StoredAcceleratorInfo>,
    system_memory: Option<StoredMemory>,
    network_profile: i32,
    runtime_backends: Vec<String>,
    locality: HashMap<String, String>,
}

fn memory_to_stored(value: &common::v1::Memory) -> StoredMemory {
    StoredMemory {
        total_bytes: value.total_bytes,
        available_bytes: value.available_bytes,
    }
}

fn memory_from_stored(value: StoredMemory) -> common::v1::Memory {
    common::v1::Memory {
        total_bytes: value.total_bytes,
        available_bytes: value.available_bytes,
    }
}

fn capabilities_to_stored(value: &common::v1::NodeCapabilities) -> StoredNodeCapabilities {
    StoredNodeCapabilities {
        os: value.os,
        cpu_arch: value.cpu_arch,
        accelerators: value
            .accelerators
            .iter()
            .map(|accelerator| StoredAcceleratorInfo {
                accelerator_type: accelerator.r#type,
                name: accelerator.name.clone(),
                vram: accelerator.vram.as_ref().map(memory_to_stored),
                compute_capability: accelerator.compute_capability.clone(),
            })
            .collect(),
        system_memory: value.system_memory.as_ref().map(memory_to_stored),
        network_profile: value.network_profile,
        runtime_backends: value.runtime_backends.clone(),
        locality: value.locality.clone(),
    }
}

fn capabilities_from_stored(value: StoredNodeCapabilities) -> common::v1::NodeCapabilities {
    common::v1::NodeCapabilities {
        os: value.os,
        cpu_arch: value.cpu_arch,
        accelerators: value
            .accelerators
            .into_iter()
            .map(|accelerator| common::v1::AcceleratorInfo {
                r#type: accelerator.accelerator_type,
                name: accelerator.name,
                vram: accelerator.vram.map(memory_from_stored),
                compute_capability: accelerator.compute_capability,
            })
            .collect(),
        system_memory: value.system_memory.map(memory_from_stored),
        network_profile: value.network_profile,
        runtime_backends: value.runtime_backends,
        locality: value.locality,
    }
}

mod option_memory_serde {
    use super::*;

    pub fn serialize<S>(
        value: &Option<common::v1::Memory>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        value.as_ref().map(memory_to_stored).serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> std::result::Result<Option<common::v1::Memory>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Option::<StoredMemory>::deserialize(deserializer).map(|value| value.map(memory_from_stored))
    }
}

mod node_capabilities_serde {
    use super::*;

    pub fn serialize<S>(
        value: &common::v1::NodeCapabilities,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        capabilities_to_stored(value).serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> std::result::Result<common::v1::NodeCapabilities, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        StoredNodeCapabilities::deserialize(deserializer).map(capabilities_from_stored)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn sample_capabilities() -> common::v1::NodeCapabilities {
        common::v1::NodeCapabilities {
            os: common::v1::OsType::Macos as i32,
            cpu_arch: common::v1::CpuArch::Arm64 as i32,
            accelerators: vec![common::v1::AcceleratorInfo {
                r#type: common::v1::AcceleratorType::Metal as i32,
                name: "M3".to_string(),
                vram: Some(common::v1::Memory {
                    total_bytes: 16,
                    available_bytes: 12,
                }),
                compute_capability: "metal3".to_string(),
            }],
            system_memory: Some(common::v1::Memory {
                total_bytes: 32,
                available_bytes: 24,
            }),
            network_profile: common::v1::NetworkProfile::Lan as i32,
            runtime_backends: vec!["mlx".to_string()],
            locality: HashMap::new(),
        }
    }

    fn sample_node(node_id: &str) -> NodeRecord {
        NodeRecord {
            node_id: node_id.to_string(),
            hostname: format!("{node_id}.local"),
            agent_endpoint: "http://127.0.0.1:50061".to_string(),
            capabilities: sample_capabilities(),
            status: common::v1::NodeStatus::Online as i32,
            last_heartbeat: Utc::now(),
            registered_at: Utc::now(),
            version: "0.1.0".to_string(),
            system_memory: Some(common::v1::Memory {
                total_bytes: 32,
                available_bytes: 24,
            }),
        }
    }

    #[tokio::test]
    async fn sqlite_store_persists_events_and_auth_records() {
        let path =
            std::env::temp_dir().join(format!("galactica-state-{}.db", uuid::Uuid::new_v4()));
        let store = SqliteStateStore::new(&path).await.unwrap();
        let node = sample_node("node-a");
        store
            .apply_event(ControlEvent::NodeRegistered { node: node.clone() })
            .await
            .unwrap();
        store
            .upsert_tenant(TenantRecord {
                tenant_id: "tenant-a".to_string(),
                api_key: "key-a".to_string(),
                scopes: vec!["inference:read".to_string()],
                require_mtls: false,
                max_requests_per_minute: 60,
                allowed_models: vec!["mistral-small".to_string()],
                allowed_node_pools: vec![
                    "macos-metal-arm64".to_string(),
                    "macos-cpu-x86_64".to_string(),
                ],
                expires_at: None,
            })
            .await
            .unwrap();
        store
            .append_audit_record(AuditRecord {
                event_id: "audit-1".to_string(),
                timestamp: Utc::now(),
                actor: "tenant:tenant-a".to_string(),
                action: "infer".to_string(),
                resource: "model:mistral-small".to_string(),
                outcome: "success".to_string(),
                details: BTreeMap::new(),
            })
            .await
            .unwrap();
        drop(store);

        let reopened = SqliteStateStore::new(&path).await.unwrap();
        assert!(reopened.get_node("node-a").await.unwrap().is_some());
        assert!(
            reopened
                .find_tenant_by_api_key("key-a")
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(reopened.list_audit_records().await.unwrap().len(), 1);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn in_memory_store_consumes_tokens_once() {
        let store = InMemoryStateStore::default();
        store
            .save_enrollment_token(EnrollmentTokenRecord {
                token: "token".to_string(),
                tenant_id: "tenant-a".to_string(),
                allowed_roles: vec!["node".to_string()],
                expires_at: Utc::now() + chrono::Duration::minutes(5),
                used_at: None,
            })
            .await
            .unwrap();
        assert!(
            store
                .consume_enrollment_token("token")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .consume_enrollment_token("token")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn control_event_round_trips_through_json() {
        let event = EventEnvelope {
            sequence: 1,
            event_id: "evt-1".to_string(),
            timestamp: Utc::now() + chrono::Duration::seconds(5),
            event: ControlEvent::NodeHeartbeat {
                node_id: "node-a".to_string(),
                observed_at: Utc::now(),
                system_memory: Some(common::v1::Memory {
                    total_bytes: 10,
                    available_bytes: 5,
                }),
            },
        };
        let payload = to_json(&event).unwrap();
        let decoded: EventEnvelope = from_json(&payload).unwrap();
        assert_eq!(decoded.sequence, 1);
    }

    #[tokio::test]
    async fn sqlite_store_broadcasts_new_events() {
        let path =
            std::env::temp_dir().join(format!("galactica-events-{}.db", uuid::Uuid::new_v4()));
        let store = SqliteStateStore::new(&path).await.unwrap();
        let mut receiver = store.subscribe();
        store
            .apply_event(ControlEvent::NodeRegistered {
                node: sample_node("node-a"),
            })
            .await
            .unwrap();
        let event = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.sequence, 1);
        let _ = std::fs::remove_file(path);
    }
}
