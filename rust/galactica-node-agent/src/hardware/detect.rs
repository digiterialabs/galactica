use async_trait::async_trait;
use galactica_common::types::node::{
    AcceleratorInfo, AcceleratorType, CpuArch, MemoryInfo, OsType,
};

/// Trait for detecting hardware capabilities.
#[async_trait]
pub trait HardwareDetector: Send + Sync {
    async fn detect_os(&self) -> OsType;
    async fn detect_cpu_arch(&self) -> CpuArch;
    async fn detect_memory(&self) -> MemoryInfo;
    async fn detect_accelerators(&self) -> Vec<AcceleratorInfo>;
}

/// Default hardware detector using sysinfo.
pub struct DefaultHardwareDetector;

impl DefaultHardwareDetector {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultHardwareDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HardwareDetector for DefaultHardwareDetector {
    async fn detect_os(&self) -> OsType {
        match std::env::consts::OS {
            "macos" => OsType::MacOs,
            "linux" => OsType::Linux,
            "windows" => OsType::Windows,
            _ => OsType::Linux,
        }
    }

    async fn detect_cpu_arch(&self) -> CpuArch {
        match std::env::consts::ARCH {
            "aarch64" => CpuArch::Arm64,
            "x86_64" => CpuArch::X86_64,
            _ => CpuArch::X86_64,
        }
    }

    async fn detect_memory(&self) -> MemoryInfo {
        let sys = sysinfo::System::new_all();
        MemoryInfo {
            total_bytes: sys.total_memory(),
            available_bytes: sys.available_memory(),
        }
    }

    async fn detect_accelerators(&self) -> Vec<AcceleratorInfo> {
        // Platform-specific detection would go here
        // For now, on macOS assume Metal is available
        if cfg!(target_os = "macos") && cfg!(target_arch = "aarch64") {
            vec![AcceleratorInfo {
                accelerator_type: AcceleratorType::Metal,
                name: "Apple Silicon GPU".to_string(),
                vram: MemoryInfo {
                    total_bytes: 0, // Unified memory — detected at runtime
                    available_bytes: 0,
                },
                compute_capability: None,
            }]
        } else {
            Vec::new()
        }
    }
}
