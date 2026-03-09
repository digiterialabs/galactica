"""Galactica runtime backend framework."""

from galactica_runtime.contract import RuntimeBackend
from galactica_runtime.types import (
    EmbedRequest,
    EmbedResponse,
    GenerateChunk,
    GenerateRequest,
    HealthStatus,
    LoadedModel,
    ModelSpec,
    RuntimeCapabilities,
    RuntimeEvent,
)

__all__ = [
    "RuntimeBackend",
    "RuntimeCapabilities",
    "ModelSpec",
    "LoadedModel",
    "GenerateRequest",
    "GenerateChunk",
    "EmbedRequest",
    "EmbedResponse",
    "HealthStatus",
    "RuntimeEvent",
]
