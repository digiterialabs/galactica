"""ONNX Runtime backend adapter."""

from collections.abc import AsyncIterator

from galactica_runtime.contract import RuntimeBackend
from galactica_runtime.types import (
    AcceleratorType,
    EmbedRequest,
    EmbedResponse,
    GenerateChunk,
    GenerateRequest,
    HealthStatus,
    LoadedModel,
    ModelSpec,
    RuntimeCapabilities,
    RuntimeEvent,
    RuntimeStatus,
)


class OnnxBackend(RuntimeBackend):
    """ONNX Runtime backend for cross-platform inference."""

    async def get_capabilities(self) -> RuntimeCapabilities:
        return RuntimeCapabilities(
            runtime_name="onnx",
            runtime_version="0.1.0",
            supported_accelerators=[AcceleratorType.CPU, AcceleratorType.CUDA, AcceleratorType.DIRECTML],
            supported_quantizations=["fp16", "int8"],
            supports_embedding=True,
            supports_streaming=False,
        )

    async def list_models(self) -> list[LoadedModel]:
        return []

    async def ensure_model(self, spec: ModelSpec) -> bool:
        raise NotImplementedError

    async def load_model(self, spec: ModelSpec) -> str:
        raise NotImplementedError

    async def unload_model(self, instance_id: str) -> None:
        raise NotImplementedError

    async def generate(self, request: GenerateRequest) -> AsyncIterator[GenerateChunk]:
        raise NotImplementedError
        yield  # noqa: RET503

    async def embed(self, request: EmbedRequest) -> EmbedResponse:
        raise NotImplementedError

    async def health(self) -> HealthStatus:
        return HealthStatus(
            status=RuntimeStatus.HEALTHY,
            runtime_name="onnx",
            runtime_version="0.1.0",
        )

    async def stream_events(self) -> AsyncIterator[RuntimeEvent]:
        raise NotImplementedError
        yield  # noqa: RET503
