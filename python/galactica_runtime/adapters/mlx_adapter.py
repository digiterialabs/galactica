"""MLX backend adapter for Apple Silicon."""

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


class MlxBackend(RuntimeBackend):
    """MLX runtime backend for Apple Silicon Macs."""

    async def get_capabilities(self) -> RuntimeCapabilities:
        return RuntimeCapabilities(
            runtime_name="mlx",
            runtime_version="0.1.0",
            supported_accelerators=[AcceleratorType.METAL],
            supported_quantizations=["fp16", "q4_0", "q8_0"],
            supports_embedding=True,
            supports_streaming=True,
        )

    async def list_models(self) -> list[LoadedModel]:
        return []

    async def ensure_model(self, spec: ModelSpec) -> bool:
        raise NotImplementedError("MLX ensure_model not yet implemented")

    async def load_model(self, spec: ModelSpec) -> str:
        raise NotImplementedError("MLX load_model not yet implemented")

    async def unload_model(self, instance_id: str) -> None:
        raise NotImplementedError("MLX unload_model not yet implemented")

    async def generate(self, request: GenerateRequest) -> AsyncIterator[GenerateChunk]:
        raise NotImplementedError("MLX generate not yet implemented")
        yield  # make it a generator  # noqa: RET503

    async def embed(self, request: EmbedRequest) -> EmbedResponse:
        raise NotImplementedError("MLX embed not yet implemented")

    async def health(self) -> HealthStatus:
        return HealthStatus(
            status=RuntimeStatus.HEALTHY,
            runtime_name="mlx",
            runtime_version="0.1.0",
        )

    async def stream_events(self) -> AsyncIterator[RuntimeEvent]:
        raise NotImplementedError("MLX stream_events not yet implemented")
        yield  # make it a generator  # noqa: RET503
