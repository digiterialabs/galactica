"""Tests for the MLX adapter stub."""

import pytest

from galactica_runtime.adapters.mlx_adapter import MlxBackend
from galactica_runtime.types import AcceleratorType, ModelSpec


@pytest.mark.asyncio
async def test_mlx_capabilities_accelerators():
    backend = MlxBackend()
    caps = await backend.get_capabilities()
    assert AcceleratorType.METAL in caps.supported_accelerators


@pytest.mark.asyncio
async def test_mlx_capabilities_quantizations():
    backend = MlxBackend()
    caps = await backend.get_capabilities()
    assert "fp16" in caps.supported_quantizations
    assert "q4_0" in caps.supported_quantizations


@pytest.mark.asyncio
async def test_mlx_load_model_not_implemented():
    backend = MlxBackend()
    spec = ModelSpec(model_id="test-model", runtime="mlx")
    with pytest.raises(NotImplementedError):
        await backend.load_model(spec)


@pytest.mark.asyncio
async def test_mlx_ensure_model_not_implemented():
    backend = MlxBackend()
    spec = ModelSpec(model_id="test-model", runtime="mlx")
    with pytest.raises(NotImplementedError):
        await backend.ensure_model(spec)
