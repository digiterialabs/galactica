use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Info about a discovered runtime backend.
#[derive(Debug, Clone)]
pub struct RuntimeInfo {
    pub name: String,
    pub version: String,
    pub addr: String,
}

/// Manages runtime backend discovery and lifecycle.
pub struct RuntimeManager {
    runtimes: Arc<RwLock<HashMap<String, RuntimeInfo>>>,
}

impl RuntimeManager {
    pub fn new() -> Self {
        Self {
            runtimes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a discovered runtime backend.
    pub fn register_runtime(&self, info: RuntimeInfo) {
        tracing::info!(name = %info.name, addr = %info.addr, "registering runtime");
        self.runtimes
            .write()
            .unwrap()
            .insert(info.name.clone(), info);
    }

    /// Get a runtime by name.
    pub fn get_runtime(&self, name: &str) -> Option<RuntimeInfo> {
        self.runtimes.read().unwrap().get(name).cloned()
    }

    /// List all registered runtimes.
    pub fn list_runtimes(&self) -> Vec<RuntimeInfo> {
        self.runtimes.read().unwrap().values().cloned().collect()
    }
}

impl Default for RuntimeManager {
    fn default() -> Self {
        Self::new()
    }
}
