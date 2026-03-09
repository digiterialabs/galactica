use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use galactica_common::Result;

use crate::supervision::RuntimeLifecycleState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredModelState {
    pub model_id: String,
    pub runtime: String,
    pub quantization: String,
    pub max_memory_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredTaskAssignment {
    pub task_id: String,
    pub model_id: String,
    pub prompt: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DesiredNodeState {
    pub model: Option<DesiredModelState>,
    pub task: Option<DesiredTaskAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedNodeState {
    pub orphaned_instance_ids: Vec<String>,
    pub lifecycle_state: RuntimeLifecycleState,
    pub downloaded_models: BTreeSet<String>,
    pub loaded_model_id: Option<String>,
    pub active_task_id: Option<String>,
    pub current_runtime: Option<String>,
}

impl Default for ObservedNodeState {
    fn default() -> Self {
        Self {
            orphaned_instance_ids: Vec::new(),
            lifecycle_state: RuntimeLifecycleState::Idle,
            downloaded_models: BTreeSet::new(),
            loaded_model_id: None,
            active_task_id: None,
            current_runtime: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanAction {
    ShutdownOrphaned {
        instance_id: String,
    },
    StartRuntime {
        runtime: String,
    },
    DownloadModel {
        model_id: String,
    },
    LoadModel {
        model_id: String,
        runtime: String,
        quantization: String,
        max_memory_bytes: u64,
    },
    ExecuteTask {
        task_id: String,
        model_id: String,
        prompt: String,
    },
}

pub fn plan(desired: &DesiredNodeState, observed: &ObservedNodeState) -> Option<PlanAction> {
    shutdown_orphaned(desired, observed)
        .or_else(|| start_needed(desired, observed))
        .or_else(|| download(desired, observed))
        .or_else(|| load(desired, observed))
        .or_else(|| execute(desired, observed))
}

pub fn shutdown_orphaned(
    _desired: &DesiredNodeState,
    observed: &ObservedNodeState,
) -> Option<PlanAction> {
    observed
        .orphaned_instance_ids
        .first()
        .cloned()
        .map(|instance_id| PlanAction::ShutdownOrphaned { instance_id })
}

pub fn start_needed(
    desired: &DesiredNodeState,
    observed: &ObservedNodeState,
) -> Option<PlanAction> {
    let model = desired.model.as_ref()?;
    if matches!(
        observed.lifecycle_state,
        RuntimeLifecycleState::Idle | RuntimeLifecycleState::Failed
    ) {
        return Some(PlanAction::StartRuntime {
            runtime: model.runtime.clone(),
        });
    }

    None
}

pub fn download(desired: &DesiredNodeState, observed: &ObservedNodeState) -> Option<PlanAction> {
    let model = desired.model.as_ref()?;
    if !observed.downloaded_models.contains(&model.model_id) {
        return Some(PlanAction::DownloadModel {
            model_id: model.model_id.clone(),
        });
    }

    None
}

pub fn load(desired: &DesiredNodeState, observed: &ObservedNodeState) -> Option<PlanAction> {
    let model = desired.model.as_ref()?;
    if !observed.downloaded_models.contains(&model.model_id) {
        return None;
    }
    if observed.loaded_model_id.as_deref() == Some(model.model_id.as_str()) {
        return None;
    }
    if observed.active_task_id.is_some() {
        return None;
    }
    if matches!(
        observed.lifecycle_state,
        RuntimeLifecycleState::Starting
            | RuntimeLifecycleState::Downloading
            | RuntimeLifecycleState::ShuttingDown
            | RuntimeLifecycleState::Idle
    ) {
        return None;
    }

    Some(PlanAction::LoadModel {
        model_id: model.model_id.clone(),
        runtime: model.runtime.clone(),
        quantization: model.quantization.clone(),
        max_memory_bytes: model.max_memory_bytes,
    })
}

pub fn execute(desired: &DesiredNodeState, observed: &ObservedNodeState) -> Option<PlanAction> {
    let task = desired.task.as_ref()?;
    if observed.loaded_model_id.as_deref() != Some(task.model_id.as_str()) {
        return None;
    }
    if observed.active_task_id.is_some() {
        return None;
    }
    if !matches!(
        observed.lifecycle_state,
        RuntimeLifecycleState::Ready | RuntimeLifecycleState::Running
    ) {
        return None;
    }

    Some(PlanAction::ExecuteTask {
        task_id: task.task_id.clone(),
        model_id: task.model_id.clone(),
        prompt: task.prompt.clone(),
    })
}

#[async_trait]
pub trait PlanContext: Send + Sync {
    async fn desired_state(&self) -> Result<DesiredNodeState>;
    async fn observed_state(&self) -> Result<ObservedNodeState>;
    async fn execute(&self, action: PlanAction) -> Result<()>;
}

pub struct NodeAgentRunner<C> {
    context: Arc<C>,
    poll_interval: Duration,
}

impl<C> NodeAgentRunner<C> {
    pub fn new(context: Arc<C>, poll_interval: Duration) -> Self {
        Self {
            context,
            poll_interval,
        }
    }
}

impl<C> NodeAgentRunner<C>
where
    C: PlanContext + 'static,
{
    pub async fn run_once(&self) -> Result<Option<PlanAction>> {
        let desired = self.context.desired_state().await?;
        let observed = self.context.observed_state().await?;
        let action = plan(&desired, &observed);
        if let Some(action) = action.clone() {
            self.context.execute(action).await?;
        }
        Ok(action)
    }

    pub async fn run_until_idle(&self, max_iterations: usize) -> Result<Vec<PlanAction>> {
        let mut actions = Vec::new();
        for _ in 0..max_iterations {
            match self.run_once().await? {
                Some(action) => actions.push(action),
                None => break,
            }
        }
        Ok(actions)
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            let _ = self.run_once().await?;
            tokio::time::sleep(self.poll_interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Mutex;

    use super::*;

    #[test]
    fn plan_prioritizes_shutdown_before_other_steps() {
        let desired = sample_desired_state();
        let observed = ObservedNodeState {
            orphaned_instance_ids: vec!["orphan-1".to_string()],
            ..ObservedNodeState::default()
        };

        assert_eq!(
            plan(&desired, &observed),
            Some(PlanAction::ShutdownOrphaned {
                instance_id: "orphan-1".to_string(),
            })
        );
    }

    #[test]
    fn plan_starts_runtime_when_idle() {
        let desired = sample_desired_state();
        let observed = ObservedNodeState::default();

        assert_eq!(
            plan(&desired, &observed),
            Some(PlanAction::StartRuntime {
                runtime: "mlx".to_string(),
            })
        );
    }

    #[test]
    fn plan_downloads_before_loading() {
        let desired = sample_desired_state();
        let observed = ObservedNodeState {
            lifecycle_state: RuntimeLifecycleState::Ready,
            current_runtime: Some("mlx".to_string()),
            ..ObservedNodeState::default()
        };

        assert_eq!(
            plan(&desired, &observed),
            Some(PlanAction::DownloadModel {
                model_id: "mistral-small".to_string(),
            })
        );
    }

    #[test]
    fn plan_loads_before_execute() {
        let desired = sample_desired_state();
        let observed = ObservedNodeState {
            lifecycle_state: RuntimeLifecycleState::Ready,
            downloaded_models: BTreeSet::from(["mistral-small".to_string()]),
            current_runtime: Some("mlx".to_string()),
            ..ObservedNodeState::default()
        };

        assert_eq!(
            plan(&desired, &observed),
            Some(PlanAction::LoadModel {
                model_id: "mistral-small".to_string(),
                runtime: "mlx".to_string(),
                quantization: "4bit".to_string(),
                max_memory_bytes: 2 * 1024 * 1024 * 1024,
            })
        );
    }

    #[test]
    fn plan_executes_when_runtime_is_ready() {
        let desired = sample_desired_state();
        let observed = ObservedNodeState {
            lifecycle_state: RuntimeLifecycleState::Ready,
            downloaded_models: BTreeSet::from(["mistral-small".to_string()]),
            loaded_model_id: Some("mistral-small".to_string()),
            current_runtime: Some("mlx".to_string()),
            ..ObservedNodeState::default()
        };

        assert_eq!(
            plan(&desired, &observed),
            Some(PlanAction::ExecuteTask {
                task_id: "task-1".to_string(),
                model_id: "mistral-small".to_string(),
                prompt: "user: summarize".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn runner_reconciles_until_idle() {
        let context = Arc::new(InMemoryPlanContext::default());
        let runner = NodeAgentRunner::new(context.clone(), Duration::from_millis(1));

        let actions = runner.run_until_idle(8).await.unwrap();

        assert_eq!(
            actions,
            vec![
                PlanAction::ShutdownOrphaned {
                    instance_id: "orphan-1".to_string(),
                },
                PlanAction::StartRuntime {
                    runtime: "mlx".to_string(),
                },
                PlanAction::DownloadModel {
                    model_id: "mistral-small".to_string(),
                },
                PlanAction::LoadModel {
                    model_id: "mistral-small".to_string(),
                    runtime: "mlx".to_string(),
                    quantization: "4bit".to_string(),
                    max_memory_bytes: 2 * 1024 * 1024 * 1024,
                },
                PlanAction::ExecuteTask {
                    task_id: "task-1".to_string(),
                    model_id: "mistral-small".to_string(),
                    prompt: "user: summarize".to_string(),
                },
            ]
        );

        let recorded = context.actions.lock().await.clone();
        assert_eq!(recorded, actions);
    }

    #[derive(Default)]
    struct InMemoryPlanContext {
        desired: Mutex<DesiredNodeState>,
        observed: Mutex<ObservedNodeState>,
        actions: Mutex<Vec<PlanAction>>,
        seeded: Mutex<bool>,
    }

    #[async_trait]
    impl PlanContext for InMemoryPlanContext {
        async fn desired_state(&self) -> Result<DesiredNodeState> {
            let mut desired = self.desired.lock().await;
            if desired.model.is_none() {
                *desired = sample_desired_state();
            }
            Ok(desired.clone())
        }

        async fn observed_state(&self) -> Result<ObservedNodeState> {
            let mut observed = self.observed.lock().await;
            let mut seeded = self.seeded.lock().await;
            if !*seeded {
                observed.orphaned_instance_ids.push("orphan-1".to_string());
                *seeded = true;
            }
            Ok(observed.clone())
        }

        async fn execute(&self, action: PlanAction) -> Result<()> {
            self.actions.lock().await.push(action.clone());
            let mut desired = self.desired.lock().await;
            let mut observed = self.observed.lock().await;
            match action {
                PlanAction::ShutdownOrphaned { instance_id } => {
                    observed
                        .orphaned_instance_ids
                        .retain(|value| value != &instance_id);
                }
                PlanAction::StartRuntime { runtime } => {
                    observed.current_runtime = Some(runtime);
                    observed.lifecycle_state = RuntimeLifecycleState::Ready;
                }
                PlanAction::DownloadModel { model_id } => {
                    observed.downloaded_models.insert(model_id);
                }
                PlanAction::LoadModel { model_id, .. } => {
                    observed.loaded_model_id = Some(model_id);
                    observed.lifecycle_state = RuntimeLifecycleState::Ready;
                }
                PlanAction::ExecuteTask { task_id, .. } => {
                    observed.active_task_id = Some(task_id);
                    desired.task = None;
                }
            }
            Ok(())
        }
    }

    fn sample_desired_state() -> DesiredNodeState {
        DesiredNodeState {
            model: Some(DesiredModelState {
                model_id: "mistral-small".to_string(),
                runtime: "mlx".to_string(),
                quantization: "4bit".to_string(),
                max_memory_bytes: 2 * 1024 * 1024 * 1024,
            }),
            task: Some(DesiredTaskAssignment {
                task_id: "task-1".to_string(),
                model_id: "mistral-small".to_string(),
                prompt: "user: summarize".to_string(),
            }),
        }
    }
}
