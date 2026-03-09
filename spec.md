**EXO Cross-Platform Redesign Spec**
Status: Draft v0.1

**1. Summary**
`exo` will be redesigned from a Python-first monolith into a modular distributed inference platform that supports:
- macOS, Windows, and Linux
- local desktop clusters, LAN clusters, and cloud/hybrid clusters
- multiple inference backends behind one orchestration layer

The new system separates:
- `control plane`: cluster state, scheduling, auth, APIs, observability
- `node agent`: per-machine lifecycle, capability reporting, caching, execution supervision
- `runtime backend`: actual model execution engine for a given platform/runtime

**2. Goals**
- Support heterogeneous clusters across macOS, Windows, Linux, and cloud VMs
- Run a single orchestration framework across all supported environments
- Allow multiple inference runtimes without changing the control plane
- Preserve local-first UX for desktop users
- Add secure cloud and hybrid deployment support
- Make backend-specific performance optimizations possible without contaminating the whole codebase

**3. Non-Goals**
- One universal tensor-parallel engine across all GPU vendors and OSes
- Rewriting every ML runtime in one language
- Perfect feature parity across all runtimes on day one

**4. Core Design Principles**
- Separate orchestration from execution
- Treat hardware/runtime heterogeneity as a scheduling concern
- Make cluster state explicit and centrally understandable
- Keep high-performance paths backend-specific
- Prefer stable, typed service boundaries over in-process coupling
- Keep desktop deployment simple while enabling cloud-scale deployment

**5. Target Architecture**
1. `Gateway/API`
   - Exposes OpenAI-compatible APIs, admin APIs, and dashboard data
   - Handles auth, rate limiting, session tracking, multi-tenant concerns
2. `Control Plane`
   - Maintains desired state and observed state
   - Schedules workloads onto compatible nodes
   - Manages placement, autoscaling hooks, traces, and policy
3. `Node Agent`
   - Runs on every machine
   - Registers node identity and capabilities
   - Manages local cache, downloads, runner processes, health reporting
   - Hosts one or more runtime backends
4. `Runtime Backend`
   - Executes inference via MLX, vLLM, TensorRT-LLM, llama.cpp, ONNX Runtime, or future backends
   - Implements a standard runtime contract
5. `Artifact Registry/Cache`
   - Stores model manifests and runtime-specific artifacts
   - Supports resumable download, deduplication, and local caching
6. `Observability Stack`
   - Centralized traces, logs, metrics, and event history

**6. Platform Model**
A node advertises:
- OS: macOS, Windows, Linux
- CPU architecture: arm64, x86_64
- accelerator type: Metal, CUDA, ROCm, DirectML, CPU
- memory and VRAM
- runtime backends installed
- network profile: LAN, WAN, public, NATed
- locality metadata: machine, rack, region, cloud provider

The scheduler groups nodes into `execution pools`.
Examples:
- `macos-metal-arm64`
- `linux-cuda-sm90`
- `windows-cuda`
- `linux-cpu`
- `cloud-aws-us-east-1-cuda`

Distributed low-latency inference is allowed within compatible pools. Cross-pool coordination is allowed for routing and failover, not assumed for tensor-parallel execution.

**7. Control Plane Responsibilities**
- Node registration and liveness
- Cluster topology and compatibility graph
- Workload placement and admission control
- Model lifecycle orchestration
- Tenant/project isolation
- API request routing
- Trace/event aggregation
- Policy enforcement

Recommended implementation:
- `Rust` service
- `SQLite` for durable state with a single active control plane
- `Redis` or `NATS` optional for ephemeral coordination/events

**8. Node Agent Responsibilities**
- Secure bootstrap with control plane
- Hardware and runtime capability detection
- Local process supervision
- Artifact download and cache management
- Runtime backend discovery
- Periodic health and utilization reporting
- Streaming execution relay back to gateway/control plane

Recommended implementation:
- `Rust` binary/service
- Single cross-platform codebase
- Optional native wrappers for desktop UX

**9. Runtime Backend Contract**
Every backend must implement:
- `get_capabilities()`
- `list_models()`
- `ensure_model(model_manifest, variant)`
- `load_model(model_instance_spec)`
- `unload_model(instance_id)`
- `generate(request)`
- `embed(request)` if supported
- `health()`
- `stream_events()`

Backends may be:
- in-process libraries
- subprocess workers
- sidecar services

Default choice: subprocess or sidecar isolation for failure containment.

**10. Language Choices**
- `Rust`: control plane, node agent, networking, artifact manager, scheduler, supervisor
- `Python`: runtime adapters for ML ecosystems that are Python-native
- `TypeScript`: dashboard, web UI, admin console, public JS SDK
- `Swift`: optional macOS desktop shell
- `C#`: optional Windows desktop shell/service UI

Minimum required stack:
- `Rust + Python + TypeScript`

**11. Backend Strategy**
Initial supported runtimes:
- macOS: `MLX`
- Linux CUDA: `vLLM` and/or `TensorRT-LLM`
- Windows: `CUDA backend` where available, otherwise `ONNX Runtime` or `llama.cpp`
- Cross-platform CPU fallback: `llama.cpp` or `ONNX Runtime`

Model manifests must support multiple variants per logical model:
- quantization
- tensor format
- chat template
- runtime compatibility
- memory requirements

**12. Networking**
Two operating modes:
- `Local mode`: mDNS/libp2p or equivalent peer discovery for home/lab clusters
- `Rendezvous mode`: central bootstrap for cloud/WAN/hybrid deployments

Control-plane transport:
- `gRPC` or `ConnectRPC` over TLS

Data-plane transport:
- streaming RPC for tokens/results
- backend-specific transport for fast distributed execution where needed

**13. Security**
- Mutual TLS between nodes and control plane
- Signed node enrollment tokens
- Tenant-aware auth at gateway
- Artifact integrity verification via content hashes
- Secrets stored outside model runtime processes
- Audit trail for model loads, API calls, and admin actions

**14. Deployment Modes**
- Single-machine desktop
- Local multi-machine cluster
- Hybrid desktop + cloud burst
- Cloud-only managed cluster

All modes use the same control-plane and node-agent abstractions.

**15. Migration Plan**
Phase 1:
- Extract current scheduling/state/API concepts into a control-plane interface
- Introduce a runtime backend abstraction
- Keep existing Python worker as the first backend

Phase 2:
- Build Rust node agent
- Move discovery, supervision, and lifecycle out of Python
- Add Linux and Windows backends

Phase 3:
- Add durable control plane, auth, cloud bootstrap, and hybrid scheduling
- Retire direct peer-only coordination for non-local deployments

**16. Success Criteria**
- One API surface across all supported platforms
- One node agent binary for macOS, Linux, Windows
- At least one production runtime backend per major platform
- Local cluster and cloud cluster both supported without architectural forks
- Mixed clusters degrade gracefully through compatibility-aware scheduling

If you want, I can turn this into a proper RFC-style markdown document with sections like `Motivation`, `Detailed Design`, `Alternatives Considered`, and `Open Questions`, wri
