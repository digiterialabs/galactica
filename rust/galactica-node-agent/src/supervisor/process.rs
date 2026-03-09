use async_trait::async_trait;

/// Configuration for launching a runtime backend process.
#[derive(Debug, Clone)]
pub struct RuntimeProcessConfig {
    pub binary_path: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub listen_port: u16,
}

/// Handle to a running runtime process.
pub struct RuntimeHandle {
    pub pid: u32,
    pub config: RuntimeProcessConfig,
}

/// Trait for process supervision.
#[async_trait]
pub trait ProcessSupervisor: Send + Sync {
    async fn spawn(&self, config: RuntimeProcessConfig)
        -> galactica_common::Result<RuntimeHandle>;
    async fn kill(&self, handle: &RuntimeHandle) -> galactica_common::Result<()>;
    async fn is_alive(&self, handle: &RuntimeHandle) -> bool;
}

/// Default process supervisor using tokio::process.
pub struct DefaultProcessSupervisor;

impl DefaultProcessSupervisor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultProcessSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ProcessSupervisor for DefaultProcessSupervisor {
    async fn spawn(
        &self,
        config: RuntimeProcessConfig,
    ) -> galactica_common::Result<RuntimeHandle> {
        tracing::info!(binary = %config.binary_path, "spawning runtime process");
        todo!("implement process spawn with tokio::process")
    }

    async fn kill(&self, handle: &RuntimeHandle) -> galactica_common::Result<()> {
        tracing::info!(pid = handle.pid, "killing runtime process");
        todo!("implement process kill")
    }

    async fn is_alive(&self, _handle: &RuntimeHandle) -> bool {
        todo!("implement process liveness check")
    }
}
