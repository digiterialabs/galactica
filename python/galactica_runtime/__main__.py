"""CLI entrypoint for standalone runtime server."""

import argparse
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description="Galactica Runtime Backend Server")
    parser.add_argument("--listen-addr", default="0.0.0.0:50051", help="gRPC listen address")
    parser.add_argument("--backend", default="mlx", choices=["mlx", "vllm", "llamacpp", "onnx"])
    parser.add_argument("--log-level", default="info", choices=["debug", "info", "warning", "error"])
    args = parser.parse_args()

    print(f"galactica-runtime starting with backend={args.backend} on {args.listen_addr}")
    # TODO: instantiate backend and start gRPC server
    sys.exit(0)


if __name__ == "__main__":
    main()
