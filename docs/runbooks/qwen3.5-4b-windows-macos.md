# Qwen3.5-4B on Windows and Mixed Mac/Windows

This repo can run `qwen3.5-4b` in two useful modes:

1. Windows local inference on a CUDA-capable NVIDIA machine via `llama.cpp`
2. A mixed cluster where macOS uses `mlx` and Windows uses `llama.cpp`

## Prerequisites

### Windows node

- Install a CUDA-enabled `llama-server.exe` build from `llama.cpp`
- Make sure `llama-server.exe` is on `PATH`
- Confirm `nvidia-smi` works in the same shell that will launch `galactica-node-agent`

### macOS node

- Apple Silicon is assumed for the `mlx` path
- Install `mlx-lm` into `.venv-mlx314`, `.venv-mlx`, or a `python3` on `PATH`

## Preferred path

Use `galactica-cli`. It auto-detects the current host, picks the matching node config, and recommends the model runtime for `qwen3.5-4b`.

Detect the current host:

```bash
cargo run -p galactica-cli -- detect
```

Check prerequisites:

```bash
cargo run -p galactica-cli -- doctor
```

Prepare local state and print the next commands:

```bash
cargo run -p galactica-cli -- install
```

On macOS this bootstraps a local MLX virtualenv. On Windows it is intended to download a local `llama.cpp` runtime into `var/dev/runtimes/llama.cpp`.

Install the CLI into a user-local bin directory and create a repo-bound `galactica` launcher:

```bash
cargo run -p galactica-cli -- self-install
```

`self-install` now prefers a prebuilt `galactica-cli` asset from the latest GitHub Release for this repo. If the latest release does not match the workspace version in your checkout, it falls back to `cargo install` so the installed binary stays compatible with local source changes.

## Quick start

On the Mac host:

```bash
cargo run -p galactica-cli -- up control-plane
```

In a second terminal on the Mac host:

```bash
cargo run -p galactica-cli -- up gateway
```

Mint a node enrollment token:

```bash
cargo run -p galactica-cli -- token mint
```

That writes the token to `var/dev/enrollment-token.txt` on the Mac and prints it to stdout.

## Local Windows bring-up

Start the Windows node agent with the token you just minted:

```powershell
.\scripts\dev\start-windows-node-agent.ps1 -EnrollmentToken <TOKEN>
```

Or use the cross-platform CLI once it is built on the Windows machine:

```powershell
cargo run -p galactica-cli -- up node --token <TOKEN>
```

Verify the model is visible:

```bash
curl -s http://127.0.0.1:8080/v1/models -H 'x-api-key: galactica-dev-key'
```

Run a chat completion:

```bash
curl -s http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'x-api-key: galactica-dev-key' \
  -d '{
    "model": "qwen3.5-4b",
    "messages": [{"role": "user", "content": "Say hello from the Windows CUDA node."}]
  }'
```

## Mixed macOS + Windows cluster

Run the control plane and gateway on the Mac:

```bash
cargo run -p galactica-cli -- up control-plane
```

```bash
cargo run -p galactica-cli -- up gateway
```

Mint a token:

```bash
cargo run -p galactica-cli -- token mint
```

Start the macOS node agent. Set `GALACTICA_AGENT_ENDPOINT` to the Mac LAN endpoint if the control plane should call back to it over the network:

```bash
GALACTICA_AGENT_ENDPOINT=http://<MAC_IP>:50061 \
cargo run -p galactica-cli -- up node
```

Start the Windows node agent, replacing `<MAC_IP>` with the Mac LAN IP reachable from Windows:

```powershell
.\scripts\dev\start-windows-node-agent.ps1 `
  -EnrollmentToken <TOKEN> `
  -ControlPlaneAddr http://<MAC_IP>:9090 `
  -AgentEndpoint http://<WINDOWS_IP>:50061
```

Check cluster state from the Mac:

```bash
curl -s http://127.0.0.1:8080/admin/cluster -H 'x-api-key: galactica-dev-key'
```

You should see both pools:

- `macos-metal-arm64`
- `windows-cuda-x86_64`

With the current scheduler, repeated compatible requests will round-robin across both local pools instead of pinning all traffic to the first pool name.

## Notes

- `galactica-cli` lives in `rust/galactica-cli/` and is the preferred entry point
- The checked-in configs live under `config/dev/`
- `just dev-detect`, `just dev-doctor`, `just dev-install`, `just dev-control-plane`, `just dev-token`, `just dev-gateway`, and `just dev-node` wrap the CLI
- Tagged pushes like `v0.1.4` publish prebuilt `galactica-cli` archives through GitHub Releases via `.github/workflows/release.yml`
- The first load of `qwen3.5-4b` can download artifacts from Hugging Face
- The `llama.cpp` variant is pinned to `Qwen3.5-4B.Q4_K_M.gguf`
- If Windows cannot see the control plane, check firewall rules for ports `9090`, `8080`, and `50061`
