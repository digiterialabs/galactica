use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AcceleratorType {
    Cpu,
    Metal,
    Cuda,
    Rocm,
    DirectMl,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OsType {
    MacOs,
    Linux,
    Windows,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CpuArch {
    Arm64,
    X86_64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NetworkProfile {
    Lan,
    Wan,
    Public,
    Nated,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NetworkMode {
    Local,
    Rendezvous,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Locality {
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    pub os: OsType,
    pub cpu_arch: CpuArch,
    pub accelerators: Vec<AcceleratorInfo>,
    pub system_memory: MemoryInfo,
    pub network_profile: NetworkProfile,
    pub runtime_backends: Vec<String>,
    pub locality: Locality,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceleratorInfo {
    pub accelerator_type: AcceleratorType,
    pub name: String,
    pub vram: MemoryInfo,
    pub compute_capability: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MemoryInfo {
    pub total_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeProfile {
    pub node_id: NodeId,
    pub hostname: String,
    pub capabilities: NodeCapabilities,
    pub version: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeStatus {
    Registering,
    Online,
    Offline,
    Draining,
    Removed,
}
