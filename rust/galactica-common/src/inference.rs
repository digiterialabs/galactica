use serde::{Deserialize, Serialize};

use crate::proto::{control, gateway};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatTurn {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InferenceParameters {
    pub temperature: f32,
    pub top_p: f32,
    pub max_tokens: u32,
    pub stop: Vec<String>,
    pub frequency_penalty: f32,
    pub presence_penalty: f32,
}

impl Default for InferenceParameters {
    fn default() -> Self {
        Self {
            temperature: 0.7,
            top_p: 1.0,
            max_tokens: 256,
            stop: Vec::new(),
            frequency_penalty: 0.0,
            presence_penalty: 0.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct InferenceUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InferenceRequest {
    pub request_id: String,
    pub model: String,
    pub messages: Vec<ChatTurn>,
    pub params: InferenceParameters,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InferenceChunk {
    pub request_id: String,
    pub model: String,
    pub choice_index: u32,
    pub delta_content: String,
    pub finish_reason: Option<String>,
    pub usage: Option<InferenceUsage>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InferenceResponse {
    pub request_id: String,
    pub model: String,
    pub content: String,
    pub finish_reason: String,
    pub usage: InferenceUsage,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeExecutionPayload {
    pub task_id: String,
    pub prompt: String,
    pub params: InferenceParameters,
}

impl InferenceRequest {
    pub fn prompt(&self) -> String {
        self.messages
            .iter()
            .map(|message| format!("{}: {}", message.role, message.content))
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn prompt_token_estimate(&self) -> u32 {
        estimate_tokens(&self.prompt())
    }
}

impl From<gateway::v1::ChatMessage> for ChatTurn {
    fn from(value: gateway::v1::ChatMessage) -> Self {
        Self {
            role: value.role,
            content: value.content,
        }
    }
}

impl From<ChatTurn> for gateway::v1::ChatMessage {
    fn from(value: ChatTurn) -> Self {
        Self {
            role: value.role,
            content: value.content,
        }
    }
}

impl From<gateway::v1::InferenceParams> for InferenceParameters {
    fn from(value: gateway::v1::InferenceParams) -> Self {
        Self {
            temperature: value.temperature,
            top_p: value.top_p,
            max_tokens: value.max_tokens,
            stop: value.stop,
            frequency_penalty: value.frequency_penalty,
            presence_penalty: value.presence_penalty,
        }
    }
}

impl From<InferenceParameters> for gateway::v1::InferenceParams {
    fn from(value: InferenceParameters) -> Self {
        Self {
            temperature: value.temperature,
            top_p: value.top_p,
            max_tokens: value.max_tokens,
            stop: value.stop,
            frequency_penalty: value.frequency_penalty,
            presence_penalty: value.presence_penalty,
        }
    }
}

impl From<InferenceUsage> for gateway::v1::Usage {
    fn from(value: InferenceUsage) -> Self {
        Self {
            prompt_tokens: value.prompt_tokens,
            completion_tokens: value.completion_tokens,
            total_tokens: value.total_tokens,
        }
    }
}

impl From<gateway::v1::Usage> for InferenceUsage {
    fn from(value: gateway::v1::Usage) -> Self {
        Self {
            prompt_tokens: value.prompt_tokens,
            completion_tokens: value.completion_tokens,
            total_tokens: value.total_tokens,
        }
    }
}

impl From<control::v1::InferRequest> for InferenceRequest {
    fn from(value: control::v1::InferRequest) -> Self {
        Self {
            request_id: value.request_id,
            model: value.model,
            messages: value.messages.into_iter().map(Into::into).collect(),
            params: value.params.unwrap_or_default().into(),
        }
    }
}

impl From<InferenceRequest> for control::v1::InferRequest {
    fn from(value: InferenceRequest) -> Self {
        Self {
            request_id: value.request_id,
            model: value.model,
            messages: value.messages.into_iter().map(Into::into).collect(),
            params: Some(value.params.into()),
        }
    }
}

impl From<InferenceRequest> for gateway::v1::InferRequest {
    fn from(value: InferenceRequest) -> Self {
        Self {
            request_id: value.request_id,
            model: value.model,
            messages: value.messages.into_iter().map(Into::into).collect(),
            params: Some(value.params.into()),
        }
    }
}

impl From<InferenceChunk> for control::v1::InferChunk {
    fn from(value: InferenceChunk) -> Self {
        Self {
            request_id: value.request_id,
            model: value.model,
            choice_index: value.choice_index,
            delta_content: value.delta_content,
            finish_reason: value.finish_reason.unwrap_or_default(),
            usage: value.usage.map(Into::into),
        }
    }
}

impl From<control::v1::InferChunk> for InferenceChunk {
    fn from(value: control::v1::InferChunk) -> Self {
        Self {
            request_id: value.request_id,
            model: value.model,
            choice_index: value.choice_index,
            delta_content: value.delta_content,
            finish_reason: if value.finish_reason.is_empty() {
                None
            } else {
                Some(value.finish_reason)
            },
            usage: value.usage.map(Into::into),
        }
    }
}

pub fn estimate_tokens(content: &str) -> u32 {
    content.split_whitespace().count() as u32
}
