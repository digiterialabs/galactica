# Proto Definitions Design (DIG-59)

**Status:** Approved
**Date:** 2026-03-08

## Overview

Define all gRPC service boundaries for the Galactica distributed inference platform using Protocol Buffers v3. This establishes the typed contract between control plane, node agents, gateways, artifact services, and runtime backends.

## Files

| File | Purpose |
|------|---------|
| `proto/buf.yaml` | Buf module config (lint + breaking rules) |
| `proto/buf.gen.yaml` | Codegen config for Rust (prost/tonic) and Python (grpcio-tools) |
| `proto/galactica/common/v1/common.proto` | Shared types: IDs, enums, capabilities |
| `proto/galactica/control/v1/control.proto` | ControlPlane service (7 RPCs) |
| `proto/galactica/control/v1/auth.proto` | Security types for enrollment/auth |
| `proto/galactica/node/v1/node.proto` | NodeAgent service (4 RPCs) |
| `proto/galactica/gateway/v1/gateway.proto` | InferenceGateway service (3 RPCs) |
| `proto/galactica/artifact/v1/artifact.proto` | ArtifactService (3 RPCs) |
| `proto/galactica/runtime/v1/runtime.proto` | RuntimeBackend service (9 RPCs) |

## Design Decisions

1. **All IDs as `string`** — UUIDs serialized as strings for cross-language compatibility
2. **Timestamps as `google.protobuf.Timestamp`** — standard proto well-known type
3. **Streaming RPCs** for: WatchEvents, InferStream, WatchDownload, StreamEvents, Generate (server-streaming)
4. **Enums** use `_UNSPECIFIED = 0` sentinel per proto3 convention
5. **Service-specific request/response wrappers** rather than bare message types

## Codegen Strategy

- **Rust**: `tonic-build` in `rust/galactica-common/build.rs` compiles all protos. Adds `prost`, `tonic`, `tonic-build` as dependencies.
- **Python**: `grpcio-tools` via `just proto-gen-python`, output to `python/galactica_runtime/proto/`

## Services Summary

### ControlPlane (control.proto)
- RegisterNode, Heartbeat, ReportCapabilities, GetClusterState, WatchEvents, EnrollNode, Authenticate

### NodeAgent (node.proto)
- ExecuteTask, LoadModel, UnloadModel, GetStatus

### InferenceGateway (gateway.proto)
- Infer, InferStream, ListModels

### ArtifactService (artifact.proto)
- GetModelManifest, ListModels, WatchDownload

### RuntimeBackend (runtime.proto)
- GetCapabilities, ListModels, EnsureModel, LoadModel, UnloadModel, Generate, Embed, Health, StreamEvents
