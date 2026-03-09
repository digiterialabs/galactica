# Proto Definitions Implementation Plan (DIG-59)

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Define all gRPC service boundaries as Protocol Buffer files and wire up Rust codegen via tonic-build.

**Architecture:** Proto files live under `proto/galactica/<domain>/v1/`. Buf manages linting. Rust stubs are generated at build time by `tonic-build` in `galactica-common/build.rs`. Python stubs are generated on-demand via justfile.

**Tech Stack:** Protocol Buffers v3, buf, tonic-build, prost, tonic, grpcio-tools (Python)

---

### Task 1: Buf Configuration

**Files:**
- Create: `proto/buf.yaml`
- Create: `proto/buf.gen.yaml`

**Step 1: Create `proto/buf.yaml`**

```yaml
version: v2
modules:
  - path: .
lint:
  use:
    - STANDARD
  except:
    - PACKAGE_VERSION_SUFFIX
breaking:
  use:
    - FILE
```

**Step 2: Create `proto/buf.gen.yaml`**

```yaml
version: v2
plugins:
  - remote: buf.build/protocolbuffers/rust
    out: ../rust/galactica-common/src/gen
  - remote: buf.build/grpc/python
    out: ../python/galactica_runtime/proto
```

Note: This buf.gen.yaml is for reference/CI. Actual Rust codegen happens via tonic-build in build.rs. Python codegen uses the justfile command.

**Step 3: Commit**

```bash
git add proto/buf.yaml proto/buf.gen.yaml
git commit -m "feat(proto): add buf module and codegen config (DIG-59)"
```

---

### Task 2: Common Proto — Shared Types

**Files:**
- Create: `proto/galactica/common/v1/common.proto`

**Step 1: Create common.proto**

```protobuf
syntax = "proto3";

package galactica.common.v1;

import "google/protobuf/timestamp.proto";

// === Identifiers ===

message NodeId {
  string value = 1;
}

message ModelId {
  string value = 1;
}

message TaskId {
  string value = 1;
}

message InstanceId {
  string value = 1;
}

message TenantId {
  string value = 1;
}

// === Enums ===

enum AcceleratorType {
  ACCELERATOR_TYPE_UNSPECIFIED = 0;
  ACCELERATOR_TYPE_CPU = 1;
  ACCELERATOR_TYPE_METAL = 2;
  ACCELERATOR_TYPE_CUDA = 3;
  ACCELERATOR_TYPE_ROCM = 4;
  ACCELERATOR_TYPE_DIRECTML = 5;
}

enum OsType {
  OS_TYPE_UNSPECIFIED = 0;
  OS_TYPE_MACOS = 1;
  OS_TYPE_LINUX = 2;
  OS_TYPE_WINDOWS = 3;
}

enum CpuArch {
  CPU_ARCH_UNSPECIFIED = 0;
  CPU_ARCH_ARM64 = 1;
  CPU_ARCH_X86_64 = 2;
}

enum NetworkProfile {
  NETWORK_PROFILE_UNSPECIFIED = 0;
  NETWORK_PROFILE_LAN = 1;
  NETWORK_PROFILE_WAN = 2;
  NETWORK_PROFILE_PUBLIC = 3;
  NETWORK_PROFILE_NATED = 4;
}

enum NodeStatus {
  NODE_STATUS_UNSPECIFIED = 0;
  NODE_STATUS_REGISTERING = 1;
  NODE_STATUS_ONLINE = 2;
  NODE_STATUS_OFFLINE = 3;
  NODE_STATUS_DRAINING = 4;
  NODE_STATUS_REMOVED = 5;
}

enum TaskStatus {
  TASK_STATUS_UNSPECIFIED = 0;
  TASK_STATUS_PENDING = 1;
  TASK_STATUS_RUNNING = 2;
  TASK_STATUS_COMPLETED = 3;
  TASK_STATUS_FAILED = 4;
  TASK_STATUS_CANCELLED = 5;
}

// === Composite Types ===

message Memory {
  uint64 total_bytes = 1;
  uint64 available_bytes = 2;
}

message AcceleratorInfo {
  AcceleratorType type = 1;
  string name = 2;
  Memory vram = 3;
  string compute_capability = 4; // e.g. "sm_90" for CUDA
}

message NodeCapabilities {
  OsType os = 1;
  CpuArch cpu_arch = 2;
  repeated AcceleratorInfo accelerators = 3;
  Memory system_memory = 4;
  NetworkProfile network_profile = 5;
  repeated string runtime_backends = 6; // e.g. ["mlx", "vllm"]
  map<string, string> locality = 7; // e.g. {"region": "us-east-1", "rack": "r1"}
}

message NodeInfo {
  NodeId node_id = 1;
  string hostname = 2;
  NodeCapabilities capabilities = 3;
  NodeStatus status = 4;
  google.protobuf.Timestamp last_heartbeat = 5;
  string version = 6;
}

// === Model Types ===

message ModelVariant {
  string runtime = 1;        // e.g. "mlx", "vllm"
  string quantization = 2;   // e.g. "q4_0", "fp16"
  string format = 3;         // e.g. "safetensors", "gguf"
  uint64 size_bytes = 4;
  repeated AcceleratorType compatible_accelerators = 5;
}

message ModelManifest {
  ModelId model_id = 1;
  string name = 2;           // e.g. "meta-llama/Llama-3-8B"
  string family = 3;
  repeated ModelVariant variants = 4;
  string chat_template = 5;
  map<string, string> metadata = 6;
}

// === Event Types ===

message ClusterEvent {
  string event_id = 1;
  google.protobuf.Timestamp timestamp = 2;
  oneof event {
    NodeJoined node_joined = 3;
    NodeLeft node_left = 4;
    ModelLoaded model_loaded = 5;
    ModelUnloaded model_unloaded = 6;
    TaskCompleted task_completed = 7;
  }
}

message NodeJoined {
  NodeInfo node = 1;
}

message NodeLeft {
  NodeId node_id = 1;
  string reason = 2;
}

message ModelLoaded {
  NodeId node_id = 1;
  ModelId model_id = 2;
  InstanceId instance_id = 3;
}

message ModelUnloaded {
  NodeId node_id = 1;
  InstanceId instance_id = 2;
}

message TaskCompleted {
  TaskId task_id = 1;
  TaskStatus status = 2;
}
```

**Step 2: Commit**

```bash
git add proto/galactica/common/v1/common.proto
git commit -m "feat(proto): add common types — IDs, enums, capabilities (DIG-59)"
```

---

### Task 3: Auth Proto — Security Types

**Files:**
- Create: `proto/galactica/control/v1/auth.proto`

**Step 1: Create auth.proto**

```protobuf
syntax = "proto3";

package galactica.control.v1;

import "google/protobuf/timestamp.proto";
import "galactica/common/v1/common.proto";

// Enrollment token issued by control plane for node bootstrap
message EnrollmentToken {
  string token = 1;
  google.protobuf.Timestamp expires_at = 2;
  galactica.common.v1.TenantId tenant_id = 3;
  repeated string allowed_roles = 4; // e.g. ["node-agent", "gateway"]
}

// Identity assigned to a node after enrollment
message NodeIdentity {
  galactica.common.v1.NodeId node_id = 1;
  string certificate_pem = 2;
  string private_key_pem = 3;
  google.protobuf.Timestamp issued_at = 4;
  google.protobuf.Timestamp expires_at = 5;
}

// Credentials scoped to a tenant
message TenantCredentials {
  galactica.common.v1.TenantId tenant_id = 1;
  string api_key = 2;
  repeated string scopes = 3; // e.g. ["infer", "admin"]
  google.protobuf.Timestamp expires_at = 4;
}

// Auth policy for a tenant
message AuthPolicy {
  galactica.common.v1.TenantId tenant_id = 1;
  bool require_mtls = 2;
  uint32 max_requests_per_minute = 3;
  repeated string allowed_models = 4; // empty = all models
  repeated string allowed_node_pools = 5;
}
```

**Step 2: Commit**

```bash
git add proto/galactica/control/v1/auth.proto
git commit -m "feat(proto): add auth types — enrollment, identity, credentials (DIG-59)"
```

---

### Task 4: Control Plane Proto — ControlPlane Service

**Files:**
- Create: `proto/galactica/control/v1/control.proto`

**Step 1: Create control.proto**

```protobuf
syntax = "proto3";

package galactica.control.v1;

import "google/protobuf/timestamp.proto";
import "galactica/common/v1/common.proto";
import "galactica/control/v1/auth.proto";

service ControlPlane {
  // Node lifecycle
  rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
  rpc ReportCapabilities(ReportCapabilitiesRequest) returns (ReportCapabilitiesResponse);

  // Cluster state
  rpc GetClusterState(GetClusterStateRequest) returns (GetClusterStateResponse);
  rpc WatchEvents(WatchEventsRequest) returns (stream galactica.common.v1.ClusterEvent);

  // Security
  rpc EnrollNode(EnrollNodeRequest) returns (EnrollNodeResponse);
  rpc Authenticate(AuthenticateRequest) returns (AuthenticateResponse);
}

// === RegisterNode ===

message RegisterNodeRequest {
  string hostname = 1;
  galactica.common.v1.NodeCapabilities capabilities = 2;
  string version = 3;
}

message RegisterNodeResponse {
  galactica.common.v1.NodeId node_id = 1;
  google.protobuf.Timestamp registered_at = 2;
}

// === Heartbeat ===

message HeartbeatRequest {
  galactica.common.v1.NodeId node_id = 1;
  galactica.common.v1.Memory system_memory = 2;
  repeated AcceleratorUtilization accelerator_utilization = 3;
  repeated RunningInstance running_instances = 4;
}

message AcceleratorUtilization {
  uint32 index = 1;
  float utilization_percent = 2;
  galactica.common.v1.Memory vram = 3;
}

message RunningInstance {
  galactica.common.v1.InstanceId instance_id = 1;
  galactica.common.v1.ModelId model_id = 2;
  uint64 memory_used_bytes = 3;
}

message HeartbeatResponse {
  google.protobuf.Timestamp server_time = 1;
  uint32 heartbeat_interval_seconds = 2;
}

// === ReportCapabilities ===

message ReportCapabilitiesRequest {
  galactica.common.v1.NodeId node_id = 1;
  galactica.common.v1.NodeCapabilities capabilities = 2;
}

message ReportCapabilitiesResponse {}

// === GetClusterState ===

message GetClusterStateRequest {}

message GetClusterStateResponse {
  repeated galactica.common.v1.NodeInfo nodes = 1;
  repeated LoadedModel loaded_models = 2;
}

message LoadedModel {
  galactica.common.v1.InstanceId instance_id = 1;
  galactica.common.v1.ModelId model_id = 2;
  galactica.common.v1.NodeId node_id = 3;
  string runtime = 4;
}

// === WatchEvents ===

message WatchEventsRequest {
  google.protobuf.Timestamp since = 1; // optional: replay from timestamp
}

// === EnrollNode ===

message EnrollNodeRequest {
  string enrollment_token = 1;
  string hostname = 2;
  galactica.common.v1.NodeCapabilities capabilities = 3;
}

message EnrollNodeResponse {
  NodeIdentity identity = 1;
}

// === Authenticate ===

message AuthenticateRequest {
  oneof credential {
    string api_key = 1;
    string certificate_fingerprint = 2;
  }
}

message AuthenticateResponse {
  bool authenticated = 1;
  galactica.common.v1.TenantId tenant_id = 2;
  repeated string scopes = 3;
  google.protobuf.Timestamp expires_at = 4;
}
```

**Step 2: Commit**

```bash
git add proto/galactica/control/v1/control.proto
git commit -m "feat(proto): add ControlPlane service — register, heartbeat, auth (DIG-59)"
```

---

### Task 5: Node Agent Proto

**Files:**
- Create: `proto/galactica/node/v1/node.proto`

**Step 1: Create node.proto**

```protobuf
syntax = "proto3";

package galactica.node.v1;

import "galactica/common/v1/common.proto";

service NodeAgent {
  rpc ExecuteTask(ExecuteTaskRequest) returns (ExecuteTaskResponse);
  rpc LoadModel(LoadModelRequest) returns (LoadModelResponse);
  rpc UnloadModel(UnloadModelRequest) returns (UnloadModelResponse);
  rpc GetStatus(GetStatusRequest) returns (GetStatusResponse);
}

// === ExecuteTask ===

message ExecuteTaskRequest {
  galactica.common.v1.TaskId task_id = 1;
  galactica.common.v1.InstanceId instance_id = 2;
  bytes payload = 3; // serialized inference request
}

message ExecuteTaskResponse {
  galactica.common.v1.TaskId task_id = 1;
  galactica.common.v1.TaskStatus status = 2;
  bytes result = 3;
  string error_message = 4;
}

// === LoadModel ===

message LoadModelRequest {
  galactica.common.v1.ModelId model_id = 1;
  string variant_runtime = 2;    // e.g. "mlx"
  string variant_quantization = 3; // e.g. "q4_0"
  uint64 max_memory_bytes = 4;   // 0 = no limit
}

message LoadModelResponse {
  galactica.common.v1.InstanceId instance_id = 1;
  bool success = 2;
  string error_message = 3;
}

// === UnloadModel ===

message UnloadModelRequest {
  galactica.common.v1.InstanceId instance_id = 1;
}

message UnloadModelResponse {
  bool success = 1;
  string error_message = 2;
}

// === GetStatus ===

message GetStatusRequest {}

message GetStatusResponse {
  galactica.common.v1.NodeStatus status = 1;
  galactica.common.v1.NodeCapabilities capabilities = 2;
  galactica.common.v1.Memory system_memory = 3;
  repeated ModelInstance loaded_models = 4;
}

message ModelInstance {
  galactica.common.v1.InstanceId instance_id = 1;
  galactica.common.v1.ModelId model_id = 2;
  string runtime = 3;
  uint64 memory_used_bytes = 4;
}
```

**Step 2: Commit**

```bash
git add proto/galactica/node/v1/node.proto
git commit -m "feat(proto): add NodeAgent service — task exec, model load/unload (DIG-59)"
```

---

### Task 6: Gateway Proto

**Files:**
- Create: `proto/galactica/gateway/v1/gateway.proto`

**Step 1: Create gateway.proto**

```protobuf
syntax = "proto3";

package galactica.gateway.v1;

import "galactica/common/v1/common.proto";

service InferenceGateway {
  rpc Infer(InferRequest) returns (InferResponse);
  rpc InferStream(InferRequest) returns (stream InferStreamChunk);
  rpc ListModels(ListModelsRequest) returns (ListModelsResponse);
}

// === Infer ===

message InferRequest {
  string model = 1;           // model name or ID
  repeated ChatMessage messages = 2;
  InferenceParams params = 3;
  string request_id = 4;
}

message ChatMessage {
  string role = 1;    // "system", "user", "assistant"
  string content = 2;
}

message InferenceParams {
  float temperature = 1;
  float top_p = 2;
  uint32 max_tokens = 3;
  repeated string stop = 4;
  float frequency_penalty = 5;
  float presence_penalty = 6;
}

message InferResponse {
  string request_id = 1;
  string model = 2;
  repeated Choice choices = 3;
  Usage usage = 4;
}

message Choice {
  uint32 index = 1;
  ChatMessage message = 2;
  string finish_reason = 3; // "stop", "length", "error"
}

message Usage {
  uint32 prompt_tokens = 1;
  uint32 completion_tokens = 2;
  uint32 total_tokens = 3;
}

// === InferStream ===

message InferStreamChunk {
  string request_id = 1;
  string model = 2;
  uint32 choice_index = 3;
  string delta_content = 4;
  string finish_reason = 5; // empty until done
  Usage usage = 6;          // only set on final chunk
}

// === ListModels ===

message ListModelsRequest {}

message ListModelsResponse {
  repeated ModelInfo models = 1;
}

message ModelInfo {
  string id = 1;
  string name = 2;
  string family = 3;
  repeated string available_runtimes = 4;
  bool ready = 5; // at least one instance loaded
}
```

**Step 2: Commit**

```bash
git add proto/galactica/gateway/v1/gateway.proto
git commit -m "feat(proto): add InferenceGateway service — infer, stream, list (DIG-59)"
```

---

### Task 7: Artifact Proto

**Files:**
- Create: `proto/galactica/artifact/v1/artifact.proto`

**Step 1: Create artifact.proto**

```protobuf
syntax = "proto3";

package galactica.artifact.v1;

import "galactica/common/v1/common.proto";

service ArtifactService {
  rpc GetModelManifest(GetModelManifestRequest) returns (GetModelManifestResponse);
  rpc ListModels(ListArtifactModelsRequest) returns (ListArtifactModelsResponse);
  rpc WatchDownload(WatchDownloadRequest) returns (stream DownloadProgress);
}

// === GetModelManifest ===

message GetModelManifestRequest {
  galactica.common.v1.ModelId model_id = 1;
}

message GetModelManifestResponse {
  galactica.common.v1.ModelManifest manifest = 1;
}

// === ListModels ===

message ListArtifactModelsRequest {
  string filter_runtime = 1;         // optional: filter by compatible runtime
  string filter_family = 2;          // optional: filter by model family
  uint32 page_size = 3;
  string page_token = 4;
}

message ListArtifactModelsResponse {
  repeated galactica.common.v1.ModelManifest models = 1;
  string next_page_token = 2;
}

// === WatchDownload ===

message WatchDownloadRequest {
  galactica.common.v1.ModelId model_id = 1;
  string variant_runtime = 2;
  string variant_quantization = 3;
}

message DownloadProgress {
  galactica.common.v1.ModelId model_id = 1;
  uint64 total_bytes = 2;
  uint64 downloaded_bytes = 3;
  float progress_percent = 4;
  DownloadStatus status = 5;
  string error_message = 6;
}

enum DownloadStatus {
  DOWNLOAD_STATUS_UNSPECIFIED = 0;
  DOWNLOAD_STATUS_QUEUED = 1;
  DOWNLOAD_STATUS_DOWNLOADING = 2;
  DOWNLOAD_STATUS_VERIFYING = 3;
  DOWNLOAD_STATUS_COMPLETE = 4;
  DOWNLOAD_STATUS_FAILED = 5;
}
```

**Step 2: Commit**

```bash
git add proto/galactica/artifact/v1/artifact.proto
git commit -m "feat(proto): add ArtifactService — manifests, listing, downloads (DIG-59)"
```

---

### Task 8: Runtime Backend Proto

**Files:**
- Create: `proto/galactica/runtime/v1/runtime.proto`

**Step 1: Create runtime.proto**

This implements the runtime backend contract from spec.md section 9.

```protobuf
syntax = "proto3";

package galactica.runtime.v1;

import "galactica/common/v1/common.proto";

service RuntimeBackend {
  rpc GetCapabilities(GetCapabilitiesRequest) returns (GetCapabilitiesResponse);
  rpc ListModels(ListRuntimeModelsRequest) returns (ListRuntimeModelsResponse);
  rpc EnsureModel(EnsureModelRequest) returns (EnsureModelResponse);
  rpc LoadModel(LoadRuntimeModelRequest) returns (LoadRuntimeModelResponse);
  rpc UnloadModel(UnloadRuntimeModelRequest) returns (UnloadRuntimeModelResponse);
  rpc Generate(GenerateRequest) returns (stream GenerateResponse);
  rpc Embed(EmbedRequest) returns (EmbedResponse);
  rpc Health(HealthRequest) returns (HealthResponse);
  rpc StreamEvents(StreamEventsRequest) returns (stream RuntimeEvent);
}

// === GetCapabilities ===

message GetCapabilitiesRequest {}

message GetCapabilitiesResponse {
  string runtime_name = 1;       // e.g. "mlx", "vllm"
  string runtime_version = 2;
  repeated galactica.common.v1.AcceleratorType supported_accelerators = 3;
  repeated string supported_quantizations = 4; // e.g. ["fp16", "q4_0", "q8_0"]
  bool supports_embedding = 5;
  bool supports_streaming = 6;
  uint64 max_model_size_bytes = 7; // 0 = no limit
}

// === ListModels ===

message ListRuntimeModelsRequest {}

message ListRuntimeModelsResponse {
  repeated RuntimeModelInfo models = 1;
}

message RuntimeModelInfo {
  galactica.common.v1.InstanceId instance_id = 1;
  galactica.common.v1.ModelId model_id = 2;
  string quantization = 3;
  uint64 memory_used_bytes = 4;
  bool ready = 5;
}

// === EnsureModel ===

message EnsureModelRequest {
  galactica.common.v1.ModelManifest manifest = 1;
  string variant_runtime = 2;
  string variant_quantization = 3;
}

message EnsureModelResponse {
  bool available = 1;
  bool download_required = 2;
  uint64 estimated_size_bytes = 3;
}

// === LoadModel ===

message LoadRuntimeModelRequest {
  galactica.common.v1.ModelId model_id = 1;
  string quantization = 2;
  uint64 max_memory_bytes = 3;
  map<string, string> runtime_options = 4;
}

message LoadRuntimeModelResponse {
  galactica.common.v1.InstanceId instance_id = 1;
  bool success = 2;
  string error_message = 3;
  uint64 memory_used_bytes = 4;
}

// === UnloadModel ===

message UnloadRuntimeModelRequest {
  galactica.common.v1.InstanceId instance_id = 1;
}

message UnloadRuntimeModelResponse {
  bool success = 1;
  string error_message = 2;
}

// === Generate ===

message GenerateRequest {
  galactica.common.v1.InstanceId instance_id = 1;
  string prompt = 2;
  GenerateParams params = 3;
}

message GenerateParams {
  float temperature = 1;
  float top_p = 2;
  uint32 max_tokens = 3;
  repeated string stop = 4;
}

message GenerateResponse {
  string text = 1;
  bool finished = 2;
  string finish_reason = 3; // "stop", "length", "error"
  GenerateUsage usage = 4;  // only set when finished=true
}

message GenerateUsage {
  uint32 prompt_tokens = 1;
  uint32 completion_tokens = 2;
  float tokens_per_second = 3;
}

// === Embed ===

message EmbedRequest {
  galactica.common.v1.InstanceId instance_id = 1;
  repeated string inputs = 2;
}

message EmbedResponse {
  repeated Embedding embeddings = 1;
  uint32 total_tokens = 2;
}

message Embedding {
  repeated float values = 1;
}

// === Health ===

message HealthRequest {}

message HealthResponse {
  RuntimeStatus status = 1;
  string runtime_name = 2;
  string runtime_version = 3;
  uint64 uptime_seconds = 4;
  uint32 loaded_model_count = 5;
}

enum RuntimeStatus {
  RUNTIME_STATUS_UNSPECIFIED = 0;
  RUNTIME_STATUS_HEALTHY = 1;
  RUNTIME_STATUS_DEGRADED = 2;
  RUNTIME_STATUS_UNHEALTHY = 3;
}

// === StreamEvents ===

message StreamEventsRequest {}

message RuntimeEvent {
  string event_id = 1;
  google.protobuf.Timestamp timestamp = 2;
  oneof event {
    RuntimeModelLoadedEvent model_loaded = 3;
    RuntimeModelUnloadedEvent model_unloaded = 4;
    RuntimeErrorEvent error = 5;
    RuntimeMemoryPressureEvent memory_pressure = 6;
  }
}

message RuntimeModelLoadedEvent {
  galactica.common.v1.InstanceId instance_id = 1;
  galactica.common.v1.ModelId model_id = 2;
  uint64 memory_used_bytes = 3;
}

message RuntimeModelUnloadedEvent {
  galactica.common.v1.InstanceId instance_id = 1;
}

message RuntimeErrorEvent {
  string message = 1;
  string severity = 2; // "warning", "error", "fatal"
}

message RuntimeMemoryPressureEvent {
  uint64 available_bytes = 1;
  uint64 total_bytes = 2;
  float pressure_percent = 3;
}
```

**Step 2: Commit**

```bash
git add proto/galactica/runtime/v1/runtime.proto
git commit -m "feat(proto): add RuntimeBackend service — 9 RPCs per spec (DIG-59)"
```

---

### Task 9: Wire Up Rust Codegen via tonic-build

**Files:**
- Modify: `rust/galactica-common/Cargo.toml`
- Create: `rust/galactica-common/build.rs`
- Modify: `rust/galactica-common/src/lib.rs`

**Step 1: Add dependencies to `rust/galactica-common/Cargo.toml`**

Add `prost`, `prost-types`, and `tonic` as dependencies. Add `tonic-build` as a build dependency:

```toml
[package]
name = "galactica-common"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
uuid = { workspace = true }
chrono = { workspace = true }
thiserror = { workspace = true }
toml = { workspace = true }
prost = { workspace = true }
prost-types = { workspace = true }
tonic = { workspace = true }

[build-dependencies]
tonic-build = { workspace = true }

[lints]
workspace = true
```

**Step 2: Create `rust/galactica-common/build.rs`**

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = &[
        "../../proto/galactica/common/v1/common.proto",
        "../../proto/galactica/control/v1/auth.proto",
        "../../proto/galactica/control/v1/control.proto",
        "../../proto/galactica/node/v1/node.proto",
        "../../proto/galactica/gateway/v1/gateway.proto",
        "../../proto/galactica/artifact/v1/artifact.proto",
        "../../proto/galactica/runtime/v1/runtime.proto",
    ];

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(protos, &["../../proto"])?;

    Ok(())
}
```

**Step 3: Update `rust/galactica-common/src/lib.rs`**

```rust
// Galactica common types, errors, and proto-generated code.

pub mod proto {
    pub mod common {
        pub mod v1 {
            tonic::include_proto!("galactica.common.v1");
        }
    }
    pub mod control {
        pub mod v1 {
            tonic::include_proto!("galactica.control.v1");
        }
    }
    pub mod node {
        pub mod v1 {
            tonic::include_proto!("galactica.node.v1");
        }
    }
    pub mod gateway {
        pub mod v1 {
            tonic::include_proto!("galactica.gateway.v1");
        }
    }
    pub mod artifact {
        pub mod v1 {
            tonic::include_proto!("galactica.artifact.v1");
        }
    }
    pub mod runtime {
        pub mod v1 {
            tonic::include_proto!("galactica.runtime.v1");
        }
    }
}
```

**Step 4: Build and verify**

Run: `cargo build -p galactica-common`
Expected: Compiles successfully, proto stubs generated

**Step 5: Run full workspace build and clippy**

Run: `cargo build --workspace && cargo clippy --workspace -- -D warnings`
Expected: All crates compile, no warnings

**Step 6: Commit**

```bash
git add rust/galactica-common/Cargo.toml rust/galactica-common/build.rs rust/galactica-common/src/lib.rs
git commit -m "feat(proto): wire tonic-build codegen in galactica-common (DIG-59)"
```

---

### Task 10: Final Verification and Squash Commit

**Step 1: Run full build**

```bash
just build
just check
```

Expected: All pass

**Step 2: Verify proto structure**

```bash
find proto -name '*.proto' | sort
```

Expected output:
```
proto/galactica/artifact/v1/artifact.proto
proto/galactica/common/v1/common.proto
proto/galactica/control/v1/auth.proto
proto/galactica/control/v1/control.proto
proto/galactica/gateway/v1/gateway.proto
proto/galactica/node/v1/node.proto
proto/galactica/runtime/v1/runtime.proto
```
