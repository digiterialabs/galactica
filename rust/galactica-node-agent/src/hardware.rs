use std::collections::{BTreeSet, HashMap};
use std::env;
use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Utc};
use galactica_common::proto::common;
use tokio::sync::{Mutex, broadcast};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceleratorHint {
    pub accelerator_type: common::v1::AcceleratorType,
    pub name: String,
    pub vram_bytes: Option<u64>,
    pub compute_capability: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HardwareSnapshot {
    pub hostname: String,
    pub capabilities: common::v1::NodeCapabilities,
    pub system_memory: common::v1::Memory,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HardwareSample {
    pub observed_at: DateTime<Utc>,
    pub system_memory: common::v1::Memory,
    pub cpu_utilization_percent: f32,
    pub gpu_utilization_percent: Option<f32>,
    pub temperature_celsius: Option<f32>,
}

pub trait HardwareProbe: Send {
    fn hostname(&mut self) -> Option<String>;
    fn os_type(&mut self) -> common::v1::OsType;
    fn cpu_arch(&mut self) -> common::v1::CpuArch;
    fn total_memory_bytes(&mut self) -> u64;
    fn available_memory_bytes(&mut self) -> u64;
    fn accelerator_hints(&mut self) -> Vec<AcceleratorHint>;
    fn cpu_utilization_percent(&mut self) -> f32;
    fn gpu_utilization_percent(&mut self) -> Option<f32>;
    fn temperature_celsius(&mut self) -> Option<f32>;
    fn env_var(&mut self, key: &str) -> Option<String>;
    fn command_exists(&mut self, command: &str) -> bool;
}

#[derive(Debug)]
pub struct SystemHardwareProbe {
    system: sysinfo::System,
}

impl Default for SystemHardwareProbe {
    fn default() -> Self {
        Self {
            system: sysinfo::System::new_all(),
        }
    }
}

impl HardwareProbe for SystemHardwareProbe {
    fn hostname(&mut self) -> Option<String> {
        sysinfo::System::host_name()
    }

    fn os_type(&mut self) -> common::v1::OsType {
        match env::consts::OS {
            "macos" => common::v1::OsType::Macos,
            "linux" => common::v1::OsType::Linux,
            "windows" => common::v1::OsType::Windows,
            _ => common::v1::OsType::Unspecified,
        }
    }

    fn cpu_arch(&mut self) -> common::v1::CpuArch {
        match env::consts::ARCH {
            "aarch64" => common::v1::CpuArch::Arm64,
            "x86_64" => common::v1::CpuArch::X8664,
            _ => common::v1::CpuArch::Unspecified,
        }
    }

    fn total_memory_bytes(&mut self) -> u64 {
        self.system.refresh_memory();
        self.system.total_memory()
    }

    fn available_memory_bytes(&mut self) -> u64 {
        self.system.refresh_memory();
        self.system.available_memory()
    }

    fn accelerator_hints(&mut self) -> Vec<AcceleratorHint> {
        let mut hints = vec![AcceleratorHint {
            accelerator_type: common::v1::AcceleratorType::Cpu,
            name: "CPU".to_string(),
            vram_bytes: None,
            compute_capability: String::new(),
        }];
        let os_type = self.os_type();
        let cpu_arch = self.cpu_arch();

        if os_type == common::v1::OsType::Macos && cpu_arch == common::v1::CpuArch::Arm64 {
            hints.push(AcceleratorHint {
                accelerator_type: common::v1::AcceleratorType::Metal,
                name: "Apple GPU".to_string(),
                vram_bytes: Some(self.total_memory_bytes()),
                compute_capability: "metal3".to_string(),
            });
        }
        if self
            .env_var("CUDA_VISIBLE_DEVICES")
            .is_some_and(|value| !value.trim().is_empty())
            || self.command_exists("nvidia-smi")
        {
            hints.push(AcceleratorHint {
                accelerator_type: common::v1::AcceleratorType::Cuda,
                name: self
                    .env_var("CUDA_DEVICE_NAME")
                    .unwrap_or_else(|| "NVIDIA GPU".to_string()),
                vram_bytes: parse_u64(self.env_var("GALACTICA_GPU_VRAM_BYTES")),
                compute_capability: self.env_var("CUDA_COMPUTE_CAPABILITY").unwrap_or_default(),
            });
        }
        if self.env_var("ROCM_HOME").is_some() || self.command_exists("rocm-smi") {
            hints.push(AcceleratorHint {
                accelerator_type: common::v1::AcceleratorType::Rocm,
                name: "AMD GPU".to_string(),
                vram_bytes: parse_u64(self.env_var("GALACTICA_GPU_VRAM_BYTES")),
                compute_capability: self.env_var("ROCM_ARCH").unwrap_or_default(),
            });
        }
        if os_type == common::v1::OsType::Windows
            && (self
                .env_var("DIRECTML_AVAILABLE")
                .is_some_and(|value| is_truthy(&value))
                || self.command_exists("DirectML"))
        {
            hints.push(AcceleratorHint {
                accelerator_type: common::v1::AcceleratorType::Directml,
                name: "DirectML".to_string(),
                vram_bytes: parse_u64(self.env_var("GALACTICA_GPU_VRAM_BYTES")),
                compute_capability: String::new(),
            });
        }

        dedupe_accelerators(hints)
    }

    fn cpu_utilization_percent(&mut self) -> f32 {
        self.system.refresh_cpu_all();
        let cpus = self.system.cpus();
        if cpus.is_empty() {
            return 0.0;
        }
        cpus.iter().map(sysinfo::Cpu::cpu_usage).sum::<f32>() / cpus.len() as f32
    }

    fn gpu_utilization_percent(&mut self) -> Option<f32> {
        self.env_var("GALACTICA_GPU_UTILIZATION_PERCENT")
            .and_then(|value| value.parse::<f32>().ok())
    }

    fn temperature_celsius(&mut self) -> Option<f32> {
        self.env_var("GALACTICA_TEMPERATURE_C")
            .and_then(|value| value.parse::<f32>().ok())
    }

    fn env_var(&mut self, key: &str) -> Option<String> {
        env::var(key).ok()
    }

    fn command_exists(&mut self, command: &str) -> bool {
        env::var_os("PATH")
            .into_iter()
            .flat_map(|paths| env::split_paths(&paths).collect::<Vec<_>>())
            .map(|path| command_path(path, command))
            .any(|path| path.exists())
    }
}

pub struct DefaultHardwareDetector<P = SystemHardwareProbe> {
    probe: P,
}

impl Default for DefaultHardwareDetector<SystemHardwareProbe> {
    fn default() -> Self {
        Self::new(SystemHardwareProbe::default())
    }
}

impl<P> DefaultHardwareDetector<P> {
    pub fn new(probe: P) -> Self {
        Self { probe }
    }

    pub fn into_probe(self) -> P {
        self.probe
    }
}

impl<P> DefaultHardwareDetector<P>
where
    P: HardwareProbe,
{
    pub fn detect_snapshot(&mut self) -> HardwareSnapshot {
        let hostname = self
            .probe
            .hostname()
            .unwrap_or_else(|| "unknown-host".to_string());
        let system_memory = common::v1::Memory {
            total_bytes: self.probe.total_memory_bytes(),
            available_bytes: self.probe.available_memory_bytes(),
        };
        let capabilities = common::v1::NodeCapabilities {
            os: self.probe.os_type() as i32,
            cpu_arch: self.probe.cpu_arch() as i32,
            accelerators: self.detect_accelerators(),
            system_memory: Some(system_memory),
            network_profile: self.detect_network_profile() as i32,
            runtime_backends: self.detect_runtime_backends(),
            locality: self.detect_locality(hostname.clone()),
        };

        HardwareSnapshot {
            hostname,
            capabilities,
            system_memory,
        }
    }

    pub fn detect_capabilities(&mut self) -> common::v1::NodeCapabilities {
        self.detect_snapshot().capabilities
    }

    pub fn detect_system_memory(&mut self) -> common::v1::Memory {
        self.detect_snapshot().system_memory
    }

    fn detect_accelerators(&mut self) -> Vec<common::v1::AcceleratorInfo> {
        if let Some(explicit) = self.parse_csv_env("GALACTICA_ACCELERATORS") {
            let hints = explicit
                .into_iter()
                .map(|value| AcceleratorHint {
                    accelerator_type: parse_accelerator_type(&value),
                    name: value.to_uppercase(),
                    vram_bytes: parse_u64(self.probe.env_var("GALACTICA_GPU_VRAM_BYTES")),
                    compute_capability: String::new(),
                })
                .collect::<Vec<_>>();
            return accelerators_from_hints(dedupe_accelerators(hints));
        }

        accelerators_from_hints(self.probe.accelerator_hints())
    }

    fn detect_runtime_backends(&mut self) -> Vec<String> {
        if let Some(explicit) = self.parse_csv_env("GALACTICA_RUNTIME_BACKENDS") {
            return explicit;
        }

        let mut backends = BTreeSet::new();
        let os_type = self.probe.os_type();
        let cpu_arch = self.probe.cpu_arch();
        if os_type == common::v1::OsType::Macos && cpu_arch == common::v1::CpuArch::Arm64 {
            backends.insert("mlx".to_string());
        }
        if self.probe.command_exists("vllm")
            || self
                .probe
                .env_var("VLLM_AVAILABLE")
                .is_some_and(|value| is_truthy(&value))
            || self
                .probe
                .env_var("CUDA_VISIBLE_DEVICES")
                .is_some_and(|value| !value.trim().is_empty())
        {
            backends.insert("vllm".to_string());
        }
        if self.probe.command_exists("llama-server")
            || self.probe.command_exists("llama-cli")
            || self.probe.command_exists("llama.cpp")
            || self
                .probe
                .env_var("LLAMA_CPP_AVAILABLE")
                .is_some_and(|value| is_truthy(&value))
        {
            backends.insert("llama.cpp".to_string());
        }
        if os_type == common::v1::OsType::Windows
            || self.probe.command_exists("onnxruntime_server")
            || self
                .probe
                .env_var("ONNX_RUNTIME_AVAILABLE")
                .is_some_and(|value| is_truthy(&value))
        {
            backends.insert("onnxruntime".to_string());
        }

        backends.into_iter().collect()
    }

    fn detect_network_profile(&mut self) -> common::v1::NetworkProfile {
        if let Some(value) = self.probe.env_var("GALACTICA_NETWORK_PROFILE") {
            return parse_network_profile(&value);
        }
        if self
            .probe
            .env_var("GALACTICA_PUBLIC_ADDRESS")
            .is_some_and(|value| !value.trim().is_empty())
        {
            return common::v1::NetworkProfile::Public;
        }
        if self
            .probe
            .env_var("GALACTICA_NATED")
            .is_some_and(|value| is_truthy(&value))
        {
            return common::v1::NetworkProfile::Nated;
        }
        if self.probe.env_var("GALACTICA_BOOTSTRAP_ENDPOINT").is_some()
            || self.probe.env_var("GALACTICA_CLOUD_PROVIDER").is_some()
        {
            return common::v1::NetworkProfile::Wan;
        }

        common::v1::NetworkProfile::Lan
    }

    fn detect_locality(&mut self, hostname: String) -> HashMap<String, String> {
        let mut locality = HashMap::new();
        locality.insert(
            "machine".to_string(),
            self.probe.env_var("GALACTICA_MACHINE").unwrap_or(hostname),
        );
        if let Some(rack) = self.probe.env_var("GALACTICA_RACK") {
            locality.insert("rack".to_string(), rack);
        }
        if let Some(region) = self.probe.env_var("GALACTICA_REGION") {
            locality.insert("region".to_string(), region);
        }
        if let Some(provider) = self.probe.env_var("GALACTICA_CLOUD_PROVIDER") {
            locality.insert("cloud_provider".to_string(), provider);
        }

        locality
    }

    fn parse_csv_env(&mut self, key: &str) -> Option<Vec<String>> {
        self.probe.env_var(key).map(|value| {
            let mut values = value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            values.sort();
            values.dedup();
            values
        })
    }
}

#[derive(Clone)]
pub struct HardwareMonitor<P> {
    probe: std::sync::Arc<Mutex<P>>,
}

impl<P> HardwareMonitor<P> {
    pub fn new(probe: P) -> Self {
        Self {
            probe: std::sync::Arc::new(Mutex::new(probe)),
        }
    }
}

impl<P> HardwareMonitor<P>
where
    P: HardwareProbe,
{
    pub async fn sample(&self) -> HardwareSample {
        let mut probe = self.probe.lock().await;
        HardwareSample {
            observed_at: Utc::now(),
            system_memory: common::v1::Memory {
                total_bytes: probe.total_memory_bytes(),
                available_bytes: probe.available_memory_bytes(),
            },
            cpu_utilization_percent: probe.cpu_utilization_percent(),
            gpu_utilization_percent: probe.gpu_utilization_percent(),
            temperature_celsius: probe.temperature_celsius(),
        }
    }

    pub fn start(
        &self,
        interval: Duration,
    ) -> (
        broadcast::Receiver<HardwareSample>,
        tokio::task::JoinHandle<()>,
    )
    where
        P: 'static,
    {
        let monitor = Self {
            probe: std::sync::Arc::clone(&self.probe),
        };
        let (sender, receiver) = broadcast::channel(16);
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let sample = monitor.sample().await;
                if sender.send(sample).is_err() {
                    break;
                }
            }
        });
        (receiver, handle)
    }
}

fn accelerators_from_hints(hints: Vec<AcceleratorHint>) -> Vec<common::v1::AcceleratorInfo> {
    hints
        .into_iter()
        .map(|hint| common::v1::AcceleratorInfo {
            r#type: hint.accelerator_type as i32,
            name: hint.name,
            vram: hint.vram_bytes.map(|total_bytes| common::v1::Memory {
                total_bytes,
                available_bytes: total_bytes,
            }),
            compute_capability: hint.compute_capability,
        })
        .collect()
}

fn dedupe_accelerators(hints: Vec<AcceleratorHint>) -> Vec<AcceleratorHint> {
    let mut seen = BTreeSet::new();
    hints
        .into_iter()
        .filter(|hint| seen.insert(hint.accelerator_type as i32))
        .collect()
}

fn parse_u64(value: Option<String>) -> Option<u64> {
    value.and_then(|value| value.parse::<u64>().ok())
}

fn parse_accelerator_type(value: &str) -> common::v1::AcceleratorType {
    match value.trim().to_ascii_lowercase().as_str() {
        "metal" => common::v1::AcceleratorType::Metal,
        "cuda" => common::v1::AcceleratorType::Cuda,
        "rocm" => common::v1::AcceleratorType::Rocm,
        "directml" => common::v1::AcceleratorType::Directml,
        "cpu" => common::v1::AcceleratorType::Cpu,
        _ => common::v1::AcceleratorType::Unspecified,
    }
}

fn parse_network_profile(value: &str) -> common::v1::NetworkProfile {
    match value.trim().to_ascii_lowercase().as_str() {
        "lan" => common::v1::NetworkProfile::Lan,
        "wan" => common::v1::NetworkProfile::Wan,
        "public" => common::v1::NetworkProfile::Public,
        "nated" | "nat" => common::v1::NetworkProfile::Nated,
        _ => common::v1::NetworkProfile::Unspecified,
    }
}

fn is_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn command_path(base: PathBuf, command: &str) -> PathBuf {
    #[cfg(windows)]
    {
        let executable = if command.ends_with(".exe") {
            command.to_string()
        } else {
            format!("{command}.exe")
        };
        return base.join(executable);
    }

    #[cfg(not(windows))]
    {
        base.join(command)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;

    #[derive(Debug, Clone)]
    struct FakeHardwareProbe {
        hostname: String,
        os_type: common::v1::OsType,
        cpu_arch: common::v1::CpuArch,
        total_memory_bytes: u64,
        available_memory_bytes: u64,
        accelerators: Vec<AcceleratorHint>,
        cpu_utilization_percent: f32,
        gpu_utilization_percent: Option<f32>,
        temperature_celsius: Option<f32>,
        env: HashMap<String, String>,
        commands: HashSet<String>,
    }

    impl HardwareProbe for FakeHardwareProbe {
        fn hostname(&mut self) -> Option<String> {
            Some(self.hostname.clone())
        }

        fn os_type(&mut self) -> common::v1::OsType {
            self.os_type
        }

        fn cpu_arch(&mut self) -> common::v1::CpuArch {
            self.cpu_arch
        }

        fn total_memory_bytes(&mut self) -> u64 {
            self.total_memory_bytes
        }

        fn available_memory_bytes(&mut self) -> u64 {
            self.available_memory_bytes
        }

        fn accelerator_hints(&mut self) -> Vec<AcceleratorHint> {
            self.accelerators.clone()
        }

        fn cpu_utilization_percent(&mut self) -> f32 {
            self.cpu_utilization_percent
        }

        fn gpu_utilization_percent(&mut self) -> Option<f32> {
            self.gpu_utilization_percent
        }

        fn temperature_celsius(&mut self) -> Option<f32> {
            self.temperature_celsius
        }

        fn env_var(&mut self, key: &str) -> Option<String> {
            self.env.get(key).cloned()
        }

        fn command_exists(&mut self, command: &str) -> bool {
            self.commands.contains(command)
        }
    }

    #[test]
    fn detects_macos_capabilities_and_locality() {
        let mut detector = DefaultHardwareDetector::new(FakeHardwareProbe {
            hostname: "mbp.local".to_string(),
            os_type: common::v1::OsType::Macos,
            cpu_arch: common::v1::CpuArch::Arm64,
            total_memory_bytes: 32 * 1024 * 1024 * 1024,
            available_memory_bytes: 24 * 1024 * 1024 * 1024,
            accelerators: vec![AcceleratorHint {
                accelerator_type: common::v1::AcceleratorType::Metal,
                name: "Apple GPU".to_string(),
                vram_bytes: Some(24 * 1024 * 1024 * 1024),
                compute_capability: "metal3".to_string(),
            }],
            cpu_utilization_percent: 14.5,
            gpu_utilization_percent: Some(33.0),
            temperature_celsius: Some(48.2),
            env: HashMap::from([
                ("GALACTICA_RACK".to_string(), "desk-a".to_string()),
                ("GALACTICA_REGION".to_string(), "home-lab".to_string()),
            ]),
            commands: HashSet::new(),
        });

        let snapshot = detector.detect_snapshot();
        assert_eq!(snapshot.hostname, "mbp.local");
        assert_eq!(snapshot.capabilities.os, common::v1::OsType::Macos as i32);
        assert_eq!(
            snapshot.capabilities.cpu_arch,
            common::v1::CpuArch::Arm64 as i32
        );
        assert_eq!(
            snapshot.capabilities.network_profile,
            common::v1::NetworkProfile::Lan as i32
        );
        assert_eq!(snapshot.capabilities.runtime_backends, vec!["mlx"]);
        assert_eq!(
            snapshot
                .capabilities
                .locality
                .get("machine")
                .map(String::as_str),
            Some("mbp.local")
        );
        assert_eq!(
            snapshot
                .capabilities
                .locality
                .get("rack")
                .map(String::as_str),
            Some("desk-a")
        );
        assert_eq!(snapshot.capabilities.accelerators.len(), 1);
    }

    #[test]
    fn detects_linux_cuda_runtime_stack() {
        let mut detector = DefaultHardwareDetector::new(FakeHardwareProbe {
            hostname: "trainer-01".to_string(),
            os_type: common::v1::OsType::Linux,
            cpu_arch: common::v1::CpuArch::X8664,
            total_memory_bytes: 128 * 1024 * 1024 * 1024,
            available_memory_bytes: 96 * 1024 * 1024 * 1024,
            accelerators: vec![
                AcceleratorHint {
                    accelerator_type: common::v1::AcceleratorType::Cpu,
                    name: "CPU".to_string(),
                    vram_bytes: None,
                    compute_capability: String::new(),
                },
                AcceleratorHint {
                    accelerator_type: common::v1::AcceleratorType::Cuda,
                    name: "NVIDIA H100".to_string(),
                    vram_bytes: Some(80 * 1024 * 1024 * 1024),
                    compute_capability: "sm90".to_string(),
                },
            ],
            cpu_utilization_percent: 62.0,
            gpu_utilization_percent: Some(77.5),
            temperature_celsius: Some(71.0),
            env: HashMap::from([
                ("GALACTICA_NETWORK_PROFILE".to_string(), "wan".to_string()),
                ("GALACTICA_CLOUD_PROVIDER".to_string(), "aws".to_string()),
                ("GALACTICA_REGION".to_string(), "us-east-1".to_string()),
                ("CUDA_VISIBLE_DEVICES".to_string(), "0".to_string()),
            ]),
            commands: HashSet::from([
                "vllm".to_string(),
                "llama-server".to_string(),
                "nvidia-smi".to_string(),
            ]),
        });

        let capabilities = detector.detect_capabilities();
        assert_eq!(capabilities.os, common::v1::OsType::Linux as i32);
        assert_eq!(capabilities.cpu_arch, common::v1::CpuArch::X8664 as i32);
        assert_eq!(
            capabilities.network_profile,
            common::v1::NetworkProfile::Wan as i32
        );
        assert_eq!(
            capabilities
                .locality
                .get("cloud_provider")
                .map(String::as_str),
            Some("aws")
        );
        assert_eq!(
            capabilities.locality.get("region").map(String::as_str),
            Some("us-east-1")
        );
        assert_eq!(capabilities.runtime_backends, vec!["llama.cpp", "vllm"]);
        assert_eq!(capabilities.accelerators.len(), 2);
    }

    #[tokio::test]
    async fn hardware_monitor_samples_telemetry() {
        let monitor = HardwareMonitor::new(FakeHardwareProbe {
            hostname: "node".to_string(),
            os_type: common::v1::OsType::Linux,
            cpu_arch: common::v1::CpuArch::X8664,
            total_memory_bytes: 64 * 1024 * 1024 * 1024,
            available_memory_bytes: 48 * 1024 * 1024 * 1024,
            accelerators: vec![],
            cpu_utilization_percent: 55.0,
            gpu_utilization_percent: Some(31.0),
            temperature_celsius: Some(64.0),
            env: HashMap::new(),
            commands: HashSet::new(),
        });

        let sample = monitor.sample().await;
        assert_eq!(sample.system_memory.total_bytes, 64 * 1024 * 1024 * 1024);
        assert_eq!(
            sample.system_memory.available_bytes,
            48 * 1024 * 1024 * 1024
        );
        assert_eq!(sample.cpu_utilization_percent, 55.0);
        assert_eq!(sample.gpu_utilization_percent, Some(31.0));
        assert_eq!(sample.temperature_celsius, Some(64.0));
    }
}
