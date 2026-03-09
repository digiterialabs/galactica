"""Pydantic models for the runtime backend contract."""

from enum import Enum

from pydantic import BaseModel


class AcceleratorType(str, Enum):
    CPU = "cpu"
    METAL = "metal"
    CUDA = "cuda"
    ROCM = "rocm"
    DIRECTML = "directml"


class RuntimeCapabilities(BaseModel):
    runtime_name: str
    runtime_version: str
    supported_accelerators: list[AcceleratorType]
    supported_quantizations: list[str]
    supports_embedding: bool = False
    supports_streaming: bool = True
    max_model_size_bytes: int = 0


class ModelSpec(BaseModel):
    model_id: str
    runtime: str
    quantization: str = "fp16"
    max_memory_bytes: int = 0


class LoadedModel(BaseModel):
    instance_id: str
    model_id: str
    quantization: str
    memory_used_bytes: int = 0
    ready: bool = False


class GenerateRequest(BaseModel):
    instance_id: str
    prompt: str
    temperature: float = 1.0
    top_p: float = 1.0
    max_tokens: int = 256
    stop: list[str] = []


class GenerateChunk(BaseModel):
    text: str
    finished: bool = False
    finish_reason: str = ""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    tokens_per_second: float = 0.0


class EmbedRequest(BaseModel):
    instance_id: str
    inputs: list[str]


class EmbedResponse(BaseModel):
    embeddings: list[list[float]]
    total_tokens: int = 0


class RuntimeStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class HealthStatus(BaseModel):
    status: RuntimeStatus
    runtime_name: str
    runtime_version: str
    uptime_seconds: int = 0
    loaded_model_count: int = 0


class RuntimeEventType(str, Enum):
    MODEL_LOADED = "model_loaded"
    MODEL_UNLOADED = "model_unloaded"
    ERROR = "error"
    MEMORY_PRESSURE = "memory_pressure"


class RuntimeEvent(BaseModel):
    event_type: RuntimeEventType
    event_id: str = ""
    data: dict = {}
