use serde::{Deserialize, Serialize};

/// Lifecycle state of a runtime backend.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeLifecycleState {
    Stopped,
    Starting,
    Running,
    Stopping,
    Failed,
}

/// Manages lifecycle transitions for a runtime backend.
pub struct RuntimeLifecycle {
    state: RuntimeLifecycleState,
}

impl RuntimeLifecycle {
    pub fn new() -> Self {
        Self {
            state: RuntimeLifecycleState::Stopped,
        }
    }

    pub fn state(&self) -> RuntimeLifecycleState {
        self.state
    }

    /// Transition to a new state if the transition is valid.
    pub fn transition(&mut self, target: RuntimeLifecycleState) -> galactica_common::Result<()> {
        let valid = matches!(
            (self.state, target),
            (
                RuntimeLifecycleState::Stopped,
                RuntimeLifecycleState::Starting
            ) | (
                RuntimeLifecycleState::Starting,
                RuntimeLifecycleState::Running
            ) | (
                RuntimeLifecycleState::Starting,
                RuntimeLifecycleState::Failed
            ) | (
                RuntimeLifecycleState::Running,
                RuntimeLifecycleState::Stopping
            ) | (
                RuntimeLifecycleState::Stopping,
                RuntimeLifecycleState::Stopped
            ) | (
                RuntimeLifecycleState::Failed,
                RuntimeLifecycleState::Starting
            )
        );

        if valid {
            tracing::info!(from = ?self.state, to = ?target, "lifecycle transition");
            self.state = target;
            Ok(())
        } else {
            Err(galactica_common::GalacticaError::InvalidArgument(format!(
                "invalid transition from {:?} to {:?}",
                self.state, target
            )))
        }
    }
}

impl Default for RuntimeLifecycle {
    fn default() -> Self {
        Self::new()
    }
}
