use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use galactica_common::{GalacticaError, Result};
use tokio::process::{Child, Command};
use tokio::time::timeout;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeLifecycleState {
    Idle,
    Starting,
    Downloading,
    Loading,
    Ready,
    Running,
    ShuttingDown,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeLifecycle {
    state: RuntimeLifecycleState,
    history: Vec<RuntimeLifecycleState>,
    last_error: Option<String>,
}

impl Default for RuntimeLifecycle {
    fn default() -> Self {
        Self {
            state: RuntimeLifecycleState::Idle,
            history: vec![RuntimeLifecycleState::Idle],
            last_error: None,
        }
    }
}

impl RuntimeLifecycle {
    pub fn state(&self) -> RuntimeLifecycleState {
        self.state
    }

    pub fn history(&self) -> &[RuntimeLifecycleState] {
        &self.history
    }

    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    pub fn transition(&mut self, next: RuntimeLifecycleState) -> Result<()> {
        if self.state == next {
            return Ok(());
        }
        if !is_valid_transition(self.state, next) {
            return Err(GalacticaError::failed_precondition(format!(
                "invalid runtime lifecycle transition: {:?} -> {:?}",
                self.state, next
            )));
        }
        self.state = next;
        self.history.push(next);
        if next != RuntimeLifecycleState::Failed {
            self.last_error = None;
        }
        Ok(())
    }

    pub fn fail(&mut self, error: impl Into<String>) {
        self.state = RuntimeLifecycleState::Failed;
        self.history.push(RuntimeLifecycleState::Failed);
        self.last_error = Some(error.into());
    }
}

fn is_valid_transition(current: RuntimeLifecycleState, next: RuntimeLifecycleState) -> bool {
    use RuntimeLifecycleState::{
        Downloading, Failed, Idle, Loading, Ready, Running, ShuttingDown, Starting,
    };

    matches!(
        (current, next),
        (Idle, Starting)
            | (Starting, Downloading)
            | (Starting, Loading)
            | (Starting, Ready)
            | (Starting, Failed)
            | (Downloading, Loading)
            | (Downloading, Failed)
            | (Downloading, ShuttingDown)
            | (Loading, Ready)
            | (Loading, Failed)
            | (Loading, ShuttingDown)
            | (Ready, Loading)
            | (Ready, Running)
            | (Ready, ShuttingDown)
            | (Ready, Failed)
            | (Running, Ready)
            | (Running, ShuttingDown)
            | (Running, Failed)
            | (ShuttingDown, Idle)
            | (ShuttingDown, Failed)
            | (Failed, Starting)
            | (Failed, Idle)
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeProcessConfig {
    pub runtime_name: String,
    pub program: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub working_directory: Option<PathBuf>,
    pub shutdown_grace_period: Duration,
    pub health_grace_period: Duration,
}

impl RuntimeProcessConfig {
    pub fn new(runtime_name: impl Into<String>, program: impl Into<String>) -> Self {
        Self {
            runtime_name: runtime_name.into(),
            program: program.into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            working_directory: None,
            shutdown_grace_period: Duration::from_secs(5),
            health_grace_period: Duration::from_millis(25),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHealth {
    pub runtime_name: String,
    pub pid: Option<u32>,
    pub alive: bool,
    pub lifecycle_state: RuntimeLifecycleState,
    pub restart_count: u32,
    pub last_exit_code: Option<i32>,
}

#[derive(Debug)]
pub struct RuntimeHandle {
    config: RuntimeProcessConfig,
    lifecycle: RuntimeLifecycle,
    child: Option<Child>,
    pid: Option<u32>,
    started_at: Option<Instant>,
    restart_count: u32,
    last_exit_code: Option<i32>,
}

impl RuntimeHandle {
    pub fn config(&self) -> &RuntimeProcessConfig {
        &self.config
    }

    pub fn state(&self) -> RuntimeLifecycleState {
        self.lifecycle.state()
    }

    pub fn lifecycle(&self) -> &RuntimeLifecycle {
        &self.lifecycle
    }

    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    pub fn restart_count(&self) -> u32 {
        self.restart_count
    }

    pub fn last_exit_code(&self) -> Option<i32> {
        self.last_exit_code
    }

    pub fn transition(&mut self, next: RuntimeLifecycleState) -> Result<()> {
        self.lifecycle.transition(next)
    }

    pub fn mark_failed(&mut self, error: impl Into<String>) {
        self.lifecycle.fail(error);
    }

    pub async fn observe(&mut self) -> Result<RuntimeHealth> {
        let mut alive = false;
        if let Some(child) = self.child.as_mut() {
            match child.try_wait().map_err(io_error)? {
                Some(status) => {
                    self.last_exit_code = status.code();
                    self.child = None;
                    self.pid = None;
                    self.started_at = None;
                    if self.state() == RuntimeLifecycleState::ShuttingDown {
                        self.lifecycle.transition(RuntimeLifecycleState::Idle)?;
                    } else {
                        self.lifecycle.fail(format!(
                            "{} exited unexpectedly with {:?}",
                            self.config.runtime_name, self.last_exit_code
                        ));
                    }
                }
                None => {
                    alive = true;
                    if self.state() == RuntimeLifecycleState::Starting
                        && self.started_at.is_some_and(|started_at| {
                            started_at.elapsed() >= self.config.health_grace_period
                        })
                    {
                        self.lifecycle.transition(RuntimeLifecycleState::Ready)?;
                    }
                }
            }
        }

        Ok(RuntimeHealth {
            runtime_name: self.config.runtime_name.clone(),
            pid: self.pid,
            alive,
            lifecycle_state: self.state(),
            restart_count: self.restart_count,
            last_exit_code: self.last_exit_code,
        })
    }

    async fn shutdown(&mut self) -> Result<()> {
        if self.child.is_none() {
            if self.state() == RuntimeLifecycleState::Failed {
                self.lifecycle.transition(RuntimeLifecycleState::Idle)?;
            } else if self.state() != RuntimeLifecycleState::Idle {
                self.lifecycle
                    .transition(RuntimeLifecycleState::ShuttingDown)?;
                self.lifecycle.transition(RuntimeLifecycleState::Idle)?;
            }
            return Ok(());
        }

        self.lifecycle
            .transition(RuntimeLifecycleState::ShuttingDown)?;
        let pid = self.pid;
        let mut child = self.child.take().expect("child checked above");
        request_graceful_stop(pid).await;

        let status = match timeout(self.config.shutdown_grace_period, child.wait()).await {
            Ok(result) => result.map_err(io_error)?,
            Err(_) => {
                child.kill().await.map_err(io_error)?;
                child.wait().await.map_err(io_error)?
            }
        };

        self.last_exit_code = status.code();
        self.pid = None;
        self.started_at = None;
        self.lifecycle.transition(RuntimeLifecycleState::Idle)?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct DefaultProcessSupervisor;

impl DefaultProcessSupervisor {
    pub async fn spawn(&self, config: RuntimeProcessConfig) -> Result<RuntimeHandle> {
        let mut lifecycle = RuntimeLifecycle::default();
        lifecycle.transition(RuntimeLifecycleState::Starting)?;

        let mut command = Command::new(&config.program);
        command
            .args(&config.args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(working_directory) = &config.working_directory {
            command.current_dir(working_directory);
        }
        for (key, value) in &config.env {
            command.env(key, value);
        }

        let child = command.spawn().map_err(io_error)?;
        let pid = child.id();

        Ok(RuntimeHandle {
            config,
            lifecycle,
            child: Some(child),
            pid,
            started_at: Some(Instant::now()),
            restart_count: 0,
            last_exit_code: None,
        })
    }

    pub async fn shutdown(&self, handle: &mut RuntimeHandle) -> Result<()> {
        handle.shutdown().await
    }

    pub async fn restart(&self, handle: &mut RuntimeHandle) -> Result<()> {
        handle.shutdown().await?;
        if handle.state() == RuntimeLifecycleState::Failed {
            handle.transition(RuntimeLifecycleState::Idle)?;
        }

        handle.transition(RuntimeLifecycleState::Starting)?;
        let mut command = Command::new(&handle.config.program);
        command
            .args(&handle.config.args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(working_directory) = &handle.config.working_directory {
            command.current_dir(working_directory);
        }
        for (key, value) in &handle.config.env {
            command.env(key, value);
        }

        let child = command.spawn().map_err(io_error)?;
        handle.pid = child.id();
        handle.child = Some(child);
        handle.started_at = Some(Instant::now());
        handle.restart_count += 1;
        handle.last_exit_code = None;
        Ok(())
    }

    pub async fn health(&self, handle: &mut RuntimeHandle) -> Result<RuntimeHealth> {
        handle.observe().await
    }
}

fn io_error(error: std::io::Error) -> GalacticaError {
    GalacticaError::internal(error.to_string())
}

async fn request_graceful_stop(pid: Option<u32>) {
    #[cfg(unix)]
    if let Some(pid) = pid {
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .await;
    }

    #[cfg(not(unix))]
    let _ = pid;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_validates_transitions() {
        let mut lifecycle = RuntimeLifecycle::default();
        lifecycle
            .transition(RuntimeLifecycleState::Starting)
            .unwrap();
        lifecycle
            .transition(RuntimeLifecycleState::Downloading)
            .unwrap();
        lifecycle
            .transition(RuntimeLifecycleState::Loading)
            .unwrap();
        lifecycle.transition(RuntimeLifecycleState::Ready).unwrap();
        lifecycle
            .transition(RuntimeLifecycleState::Running)
            .unwrap();
        lifecycle
            .transition(RuntimeLifecycleState::ShuttingDown)
            .unwrap();
        lifecycle.transition(RuntimeLifecycleState::Idle).unwrap();
        assert_eq!(
            lifecycle.history(),
            &[
                RuntimeLifecycleState::Idle,
                RuntimeLifecycleState::Starting,
                RuntimeLifecycleState::Downloading,
                RuntimeLifecycleState::Loading,
                RuntimeLifecycleState::Ready,
                RuntimeLifecycleState::Running,
                RuntimeLifecycleState::ShuttingDown,
                RuntimeLifecycleState::Idle,
            ]
        );
    }

    #[test]
    fn lifecycle_rejects_invalid_transitions() {
        let mut lifecycle = RuntimeLifecycle::default();
        let error = lifecycle
            .transition(RuntimeLifecycleState::Running)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("invalid runtime lifecycle transition")
        );
    }

    #[tokio::test]
    async fn supervisor_spawns_restarts_and_shuts_down_runtime() {
        let supervisor = DefaultProcessSupervisor;
        let mut handle = supervisor.spawn(sleep_config()).await.unwrap();

        tokio::time::sleep(Duration::from_millis(40)).await;
        let health = supervisor.health(&mut handle).await.unwrap();
        assert!(health.alive);
        assert_eq!(health.lifecycle_state, RuntimeLifecycleState::Ready);

        supervisor.restart(&mut handle).await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        let health = supervisor.health(&mut handle).await.unwrap();
        assert!(health.alive);
        assert_eq!(health.restart_count, 1);

        supervisor.shutdown(&mut handle).await.unwrap();
        let health = supervisor.health(&mut handle).await.unwrap();
        assert!(!health.alive);
        assert_eq!(health.lifecycle_state, RuntimeLifecycleState::Idle);
    }

    #[tokio::test]
    async fn supervisor_isolates_crashes() {
        let supervisor = DefaultProcessSupervisor;
        let mut stable = supervisor.spawn(sleep_config()).await.unwrap();
        let mut crashy = supervisor.spawn(exit_config()).await.unwrap();

        tokio::time::sleep(Duration::from_millis(40)).await;
        let failed = supervisor.health(&mut crashy).await.unwrap();
        let healthy = supervisor.health(&mut stable).await.unwrap();

        assert_eq!(failed.lifecycle_state, RuntimeLifecycleState::Failed);
        assert_eq!(failed.last_exit_code, Some(7));
        assert!(healthy.alive);
        assert_ne!(healthy.lifecycle_state, RuntimeLifecycleState::Failed);

        supervisor.shutdown(&mut stable).await.unwrap();
    }

    fn sleep_config() -> RuntimeProcessConfig {
        let mut config = RuntimeProcessConfig::new("test-runtime", test_shell());
        config.args = test_shell_args("sleep 30");
        config.shutdown_grace_period = Duration::from_millis(100);
        config.health_grace_period = Duration::from_millis(10);
        config
    }

    fn exit_config() -> RuntimeProcessConfig {
        let mut config = RuntimeProcessConfig::new("test-runtime", test_shell());
        config.args = test_shell_args("exit 7");
        config.shutdown_grace_period = Duration::from_millis(100);
        config.health_grace_period = Duration::from_millis(10);
        config
    }

    #[cfg(unix)]
    fn test_shell() -> String {
        "sh".to_string()
    }

    #[cfg(unix)]
    fn test_shell_args(script: &str) -> Vec<String> {
        vec!["-c".to_string(), script.to_string()]
    }

    #[cfg(windows)]
    fn test_shell() -> String {
        "cmd".to_string()
    }

    #[cfg(windows)]
    fn test_shell_args(script: &str) -> Vec<String> {
        vec!["/C".to_string(), script.to_string()]
    }
}
