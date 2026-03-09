"""Tests for the RuntimeBackend contract."""

import pytest

from galactica_runtime.contract import RuntimeBackend
from galactica_runtime.adapters.mlx_adapter import MlxBackend


def test_cannot_instantiate_abc():
    """RuntimeBackend ABC cannot be instantiated directly."""
    with pytest.raises(TypeError):
        RuntimeBackend()


def test_mlx_implements_all_methods():
    """MlxBackend implements all abstract methods."""
    backend = MlxBackend()
    assert isinstance(backend, RuntimeBackend)


@pytest.mark.asyncio
async def test_mlx_get_capabilities():
    backend = MlxBackend()
    caps = await backend.get_capabilities()
    assert caps.runtime_name == "mlx"
    assert caps.supports_streaming is True


@pytest.mark.asyncio
async def test_mlx_health():
    backend = MlxBackend()
    health = await backend.health()
    assert health.status.value == "healthy"
    assert health.runtime_name == "mlx"


@pytest.mark.asyncio
async def test_mlx_list_models():
    backend = MlxBackend()
    models = await backend.list_models()
    assert models == []
