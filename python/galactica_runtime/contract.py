"""RuntimeBackend abstract base class — the contract all backends must implement."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

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


class RuntimeBackend(ABC):
    """Abstract base class for runtime backends.

    Every backend (MLX, vLLM, llama.cpp, ONNX) must implement all methods.
    """

    @abstractmethod
    async def get_capabilities(self) -> RuntimeCapabilities:
        """Return the capabilities of this runtime backend."""
        ...

    @abstractmethod
    async def list_models(self) -> list[LoadedModel]:
        """List all currently loaded models."""
        ...

    @abstractmethod
    async def ensure_model(self, spec: ModelSpec) -> bool:
        """Ensure model artifacts are available locally. Returns True if ready."""
        ...

    @abstractmethod
    async def load_model(self, spec: ModelSpec) -> str:
        """Load a model into memory. Returns instance ID."""
        ...

    @abstractmethod
    async def unload_model(self, instance_id: str) -> None:
        """Unload a model from memory."""
        ...

    @abstractmethod
    async def generate(self, request: GenerateRequest) -> AsyncIterator[GenerateChunk]:
        """Generate text, yielding chunks as they're produced."""
        ...

    @abstractmethod
    async def embed(self, request: EmbedRequest) -> EmbedResponse:
        """Generate embeddings for the given inputs."""
        ...

    @abstractmethod
    async def health(self) -> HealthStatus:
        """Return the health status of this backend."""
        ...

    @abstractmethod
    async def stream_events(self) -> AsyncIterator[RuntimeEvent]:
        """Stream runtime events (model loads, errors, memory pressure)."""
        ...
