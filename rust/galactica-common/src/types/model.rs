use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::node::AcceleratorType;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ModelId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelManifest {
    pub model_id: ModelId,
    pub name: String,
    pub family: String,
    pub variants: Vec<ModelVariant>,
    pub chat_template: Option<String>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelVariant {
    pub runtime: String,
    pub quantization: String,
    pub format: String,
    pub size_bytes: u64,
    pub compatible_accelerators: Vec<AcceleratorType>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ModelCapability {
    TextGeneration,
    Embedding,
    ImageGeneration,
}
