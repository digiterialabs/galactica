use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use galactica_common::events::event::{EventId, IndexedEvent};
use galactica_common::types::cluster::ClusterState;

use super::apply::event_apply;

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn get_state(&self) -> galactica_common::Result<ClusterState>;
    async fn apply_event(&self, event: IndexedEvent) -> galactica_common::Result<()>;
    async fn get_events_since(
        &self,
        event_id: &EventId,
    ) -> galactica_common::Result<Vec<IndexedEvent>>;
}

pub struct InMemoryStateStore {
    state: Arc<RwLock<ClusterState>>,
    events: Arc<RwLock<Vec<IndexedEvent>>>,
}

impl InMemoryStateStore {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(ClusterState {
                nodes: Vec::new(),
                instances: Vec::new(),
            })),
            events: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl Default for InMemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateStore for InMemoryStateStore {
    async fn get_state(&self) -> galactica_common::Result<ClusterState> {
        Ok(self.state.read().unwrap().clone())
    }

    async fn apply_event(&self, event: IndexedEvent) -> galactica_common::Result<()> {
        let mut state = self.state.write().unwrap();
        *state = event_apply(&event.event, &state);
        self.events.write().unwrap().push(event);
        Ok(())
    }

    async fn get_events_since(
        &self,
        _event_id: &EventId,
    ) -> galactica_common::Result<Vec<IndexedEvent>> {
        Ok(self.events.read().unwrap().clone())
    }
}

pub struct PostgresStateStore;

impl PostgresStateStore {
    pub fn new(_database_url: &str) -> Self {
        Self
    }
}

#[async_trait]
impl StateStore for PostgresStateStore {
    async fn get_state(&self) -> galactica_common::Result<ClusterState> {
        todo!("implement Postgres state store")
    }

    async fn apply_event(&self, _event: IndexedEvent) -> galactica_common::Result<()> {
        todo!("implement Postgres event apply")
    }

    async fn get_events_since(
        &self,
        _event_id: &EventId,
    ) -> galactica_common::Result<Vec<IndexedEvent>> {
        todo!("implement Postgres event query")
    }
}
