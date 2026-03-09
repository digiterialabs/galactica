"""gRPC server wrapping a RuntimeBackend implementation."""

from galactica_runtime.contract import RuntimeBackend


class RuntimeServer:
    """Wraps a RuntimeBackend and serves it over gRPC."""

    def __init__(self, backend: RuntimeBackend, listen_addr: str = "0.0.0.0:50051") -> None:
        self.backend = backend
        self.listen_addr = listen_addr

    async def start(self) -> None:
        """Start the gRPC server."""
        # TODO: create gRPC server, register RuntimeBackend service, and serve
        print(f"Runtime gRPC server starting on {self.listen_addr}")
        raise NotImplementedError("gRPC server not yet implemented")

    async def stop(self) -> None:
        """Stop the gRPC server gracefully."""
        print("Runtime gRPC server stopping")
