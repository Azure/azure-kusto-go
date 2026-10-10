# Ingest V2 — Developer Implementation Guide

This guide explains how to approach implementing the Ingest V2 feature for `azure-kusto-go`,
ported from the Java `Azure/azure-kusto-java` repository's `feature/IngestV2` branch.

---

## Table of Contents

1. [Understanding the Java Source](#1-understanding-the-java-source)
2. [Module Setup](#2-module-setup)
3. [Dependency Chart](#3-dependency-chart)
4. [Build Order (Bottom-Up)](#4-build-order-bottom-up)
5. [Java → Go Translation Patterns](#5-java--go-translation-patterns)
6. [Key Design Decisions](#6-key-design-decisions)
7. [Testing Strategy](#7-testing-strategy)
8. [HTTP Stubs & Integration](#8-http-stubs--integration)
9. [Estimated Effort](#9-estimated-effort)

---

## 1. Understanding the Java Source

### Where to Start

1. **Clone** `Azure/azure-kusto-java`, checkout branch `feature/IngestV2`
2. All source lives under `ingest/src/main/java/com/microsoft/azure/kusto/ingest/v2/`
3. Read files in this order:

| Order | File                          | What It Teaches You                                    |
|-------|-------------------------------|--------------------------------------------------------|
| 1     | `IngestV2.kt`                 | Constants, enums, error types — the module's vocabulary |
| 2     | `IngestionSource.kt`          | Source abstractions (Blob, File, Stream)                |
| 3     | `ConfigurationResponse.kt`    | Data model for DM configuration endpoint response       |
| 4     | `ConfigurationClient.kt`      | HTTP client that fetches container/endpoint config       |
| 5     | `ConfigurationCache.kt`       | Caching layer with background refresh                   |
| 6     | `ContainerUploaderBase.kt`    | Upload pipeline: retry, compression, container cycling   |
| 7     | `ManagedUploader.kt`          | Storage vs Lake container selection                      |
| 8     | `QueuedIngestClient.kt`       | Queued ingestion (upload blob → post to DM queue)        |
| 9     | `StreamingIngestClient.kt`    | Streaming ingestion (direct push to Engine)              |
| 10    | `ManagedStreamingIngestClient.kt` | Streaming-first with policy-driven fallback to queued |

### Draw a Mental Model

The architecture has three layers:

```
┌─────────────────────────────────────────────┐
│           Client Layer (top)                │
│  ManagedStreamingIngestClient               │
│    ├── StreamingIngestClient                │
│    └── QueuedIngestClient                   │
│           └── Uploader (ManagedUploader)    │
├─────────────────────────────────────────────┤
│          Infrastructure Layer (middle)       │
│  ConfigurationCache ── ConfigurationClient  │
│  RoundRobinContainerList                    │
│  ManagedStreamingPolicy                     │
│  IngestRetryPolicy                          │
├─────────────────────────────────────────────┤
│          Foundation Layer (bottom)           │
│  Constants, Enums, Errors                   │
│  IngestionSource (Blob/File/Stream)         │
│  DataFormat, CompressionType                │
│  UploadResult, IngestResponse               │
│  S2SToken, ClientDetails                    │
└─────────────────────────────────────────────┘
```

---

## 2. Module Setup

```bash
# From the azure-kusto-go repo root
mkdir azkustoingestv2
cd azkustoingestv2
go mod init github.com/Azure/azure-kusto-go/azkustoingestv2

# Add to workspace
# Edit go.work and add "azkustoingestv2" to the use() block
```

**Critical rule:** This module must be fully self-contained. **Zero imports** from
`azkustoingest` or `azkustodata`. The only external dependency is `github.com/google/uuid`.

---

## 3. Dependency Chart

The chart below shows which files depend on which. Build from bottom to top.

```
                        ┌──────────┐
                        │options.go│  ← Factory: wires everything together
                        └────┬─────┘
                             │ depends on all below
          ┌──────────────────┼──────────────────┐
          │                  │                  │
   ┌──────▼───────┐   ┌──────▼──────┐   ┌──────▼───────────────┐
   │  queued.go   │   │streaming.go │   │    managed.go        │
   │ (APIClient)  │   │             │   │ (streaming+queued    │
   └──────┬───────┘   └──────┬──────┘   │  + policy)           │
          │                  │          └──────┬───────────────┘
          │                  │                 │
          │           ┌──────┴─────────────────┘
          │           │
   ┌──────▼───────────▼───────┐
   │  uploader.go             │ ← Upload pipeline
   │  uploader_managed.go     │
   └──────┬───────────────────┘
          │
   ┌──────▼───────────────────────────────────────────────┐
   │  Infrastructure Layer                                │
   │                                                      │
   │  config_cache.go ──► config_client.go                │
   │       │                    │                         │
   │       ▼                    ▼                         │
   │  config_models.go    container.go                    │
   │                                                      │
   │  policy.go    retry.go    request_properties.go      │
   │  endpoint_utils.go    endpoints.go                   │
   └──────┬───────────────────────────────────────────────┘
          │
   ┌──────▼───────────────────────────────────────────────┐
   │  Foundation Layer (no internal deps)                 │
   │                                                      │
   │  constants.go          errors.go                     │
   │  compression.go        upload_method.go              │
   │  source.go             upload_result.go              │
   │  s2s_token.go          client_details.go             │
   │  client.go (interfaces only)                         │
   │  path_utils.go         ingestion_utils.go            │
   └──────────────────────────────────────────────────────┘
```

### File-Level Dependency Matrix

```
File                    │ Depends On (internal)
────────────────────────┼──────────────────────────────────────────────
constants.go            │ (none — stdlib only)
errors.go               │ (none)
compression.go          │ (none)
upload_method.go        │ (none)
s2s_token.go            │ (none)
client_details.go       │ (none)
source.go               │ compression.go
upload_result.go        │ compression.go, source.go
client.go               │ source.go, upload_result.go, request_properties.go
config_models.go        │ upload_method.go
container.go            │ config_models.go
endpoint_utils.go       │ (none)
endpoints.go            │ (none)
path_utils.go           │ compression.go
ingestion_utils.go      │ compression.go, source.go
retry.go                │ constants.go
policy.go               │ constants.go
request_properties.go   │ compression.go
config_client.go        │ config_models.go, constants.go, s2s_token.go, client_details.go
config_cache.go         │ config_client.go, config_models.go, container.go, constants.go
uploader.go             │ source.go, compression.go, container.go, config_cache.go, retry.go, constants.go
uploader_managed.go     │ uploader.go, config_cache.go, upload_method.go
queued.go               │ source.go, upload_result.go, uploader.go, config_cache.go
streaming.go            │ source.go, upload_result.go, constants.go, errors.go
managed.go              │ streaming.go, queued.go, source.go, policy.go, upload_result.go
options.go              │ queued.go, streaming.go, managed.go, config_client.go, config_cache.go, uploader_managed.go, policy.go, constants.go
```

---

## 4. Build Order (Bottom-Up)

Implement in this order. Run `go build ./...` after each phase to catch errors early.

### Phase 1 — Foundation (zero internal deps)

| File               | Contents                                                        |
|--------------------|-----------------------------------------------------------------|
| `constants.go`     | All sizing, timeout, header, and policy default constants       |
| `errors.go`        | Error types: `IngestError`, `IngestRequestError`, `IngestClientError`, `IngestSizeLimitExceededError` |
| `compression.go`   | `CompressionType` enum, `DataFormat` string enum with `String()` method |
| `upload_method.go` | `UploadMethod` enum (Default, Storage, Lake)                    |
| `s2s_token.go`     | `S2SToken` struct for Fabric Private Link                       |
| `client_details.go`| `ClientDetails` struct with user/app/version info               |

### Phase 2 — Source Types & Results

| File               | Contents                                                        |
|--------------------|-----------------------------------------------------------------|
| `source.go`        | `IngestionSource`/`LocalSource` interfaces, `BlobSource`, `FileSource`, `StreamSource` |
| `upload_result.go` | `UploadResult`, `BatchOperationResult`, `IngestKind`, `IngestResponse`, `ExtendedIngestResponse` |

### Phase 3 — Configuration & Containers

| File               | Contents                                                        |
|--------------------|-----------------------------------------------------------------|
| `config_models.go` | `ContainerInfo`, `ContainerSettings`, `IngestionSettings`, `ConfigurationResponse` |
| `config_client.go` | `ConfigurationClient` — HTTP GET to DM `/v2/rest/configuration` |
| `config_cache.go`  | `CachedConfigurationData`, `DefaultConfigurationCache` with `sync.Once` lazy init + background refresh goroutine |
| `container.go`     | `ExtendedContainerInfo`, `RoundRobinContainerList` with `atomic.Int64` |

### Phase 4 — Endpoints & Security

| File                 | Contents                                                      |
|----------------------|---------------------------------------------------------------|
| `endpoint_utils.go`  | `GetIngestionEndpoint()`, `GetQueryEndpoint()`, `IsReservedHostname()` |
| `endpoints.go`       | `FastSuffixMatcher`, `WellKnownKustoEndpoints`, `KustoTrustedEndpoints` |

### Phase 5 — Policies & Properties

| File                     | Contents                                                  |
|--------------------------|-----------------------------------------------------------|
| `retry.go`               | `IngestRetryPolicy` interface, `SimpleRetryPolicy`, `CustomRetryPolicy` |
| `policy.go`              | `ManagedStreamingPolicy` interface, `DefaultManagedStreamingPolicy` with per-table error tracking |
| `request_properties.go`  | `IngestRequestProperties`, `ColumnMapping`, `ValidationPolicy`, builder |

### Phase 6 — Uploader Layer

| File                   | Contents                                                    |
|------------------------|-------------------------------------------------------------|
| `uploader.go`          | `Uploader` interface, `ContainerUploaderBase` with retry loop + container cycling |
| `uploader_managed.go`  | `ManagedUploader` — selects storage vs lake containers based on server preference |

### Phase 7 — Client Layer

| File            | Contents                                                           |
|-----------------|--------------------------------------------------------------------|
| `client.go`     | `IngestClient`/`MultiIngestClient` interfaces, `OperationStatus`, `StatusResponse` |
| `queued.go`     | `QueuedIngestClient` + `APIClient` (HTTP stubs for DM/Engine)      |
| `streaming.go`  | `StreamingIngestClient` — direct push, size validation              |
| `managed.go`    | `ManagedStreamingIngestClient` — streaming-first + policy fallback  |

### Phase 8 — Factory & Utilities

| File                | Contents                                                         |
|---------------------|------------------------------------------------------------------|
| `options.go`        | `ClientOption` functional options, `NewQueuedClient()`, `NewStreamingClient()`, `NewManagedStreamingClient()` |
| `path_utils.go`     | `GenerateBlobName()`, `GenerateBlobPath()`, `GetBlobExtension()`  |
| `ingestion_utils.go`| `BuildIngestionMessage()`, `ParseIngestResponseJSON()`            |
| `doc.go`            | Package-level documentation                                       |

---

## 5. Java → Go Translation Patterns

| Java / Kotlin                      | Go Equivalent                                        |
|------------------------------------|------------------------------------------------------|
| `suspend fun doX(): T`            | `func DoX(ctx context.Context) (T, error)`           |
| `CompletableFuture<T>`            | Return `(T, error)` synchronously                    |
| `XBuilder.withY(v).build()`       | Functional options: `NewX(WithY(v))`                 |
| `AtomicLong`                       | `sync/atomic.Int64`                                  |
| `ConcurrentHashMap<K,V>`          | `map[K]V` + `sync.RWMutex`                           |
| `throw CustomException(msg)`      | `return NewCustomError(msg)`                          |
| Exception class hierarchy          | Struct embedding: `type XError struct { IngestError }` |
| `AutoCloseable` / `close()`       | `io.Closer` interface                                 |
| Kotlin coroutines                  | `goroutine` + `context.Context`                       |
| `enum class X { A, B }`           | `type X string` + `const` block (for JSON compat)    |
| `enum class X { A, B }` (numeric) | `type X int` + `iota`                                 |
| `@JvmStatic val logger`           | `*slog.Logger` field or `slog.Default()`              |
| `synchronized { ... }`            | `sync.Mutex` `.Lock()` / `.Unlock()`                  |
| `lazy { ... }`                    | `sync.Once` + `Do(func(){})`                          |
| `Timer.scheduleAtFixedRate()`     | `goroutine` + `time.Ticker`                           |
| `null` / nullable types           | Pointer types (`*T`) or zero values                   |
| Interface default methods          | Embed a base struct in the interface implementor       |

---

## 6. Key Design Decisions

### 6.1 Source Type Interfaces

Use **interfaces with unexported fields** and accessor methods:

```go
type IngestionSource interface {
    Format() DataFormat
    CompressionType() CompressionType
    Name() string
    SourceID() uuid.UUID
}

type BlobSource struct {
    blobPath string  // unexported
    format   DataFormat
    // ...
}

func (s *BlobSource) BlobPath() string { return s.blobPath }
```

This prevents direct field mutation and enables future source type additions.

### 6.2 Round-Robin Container Selection

```go
type RoundRobinContainerList struct {
    containers []*ExtendedContainerInfo
    counter    atomic.Int64  // initialized to -1
}

func (r *RoundRobinContainerList) Next() *ExtendedContainerInfo {
    idx := r.counter.Add(1)
    return r.containers[idx % int64(len(r.containers))]
}
```

### 6.3 Managed Streaming Policy

Track per-table streaming errors behind a `sync.RWMutex`:

```go
type tableState struct {
    lastError     error
    lastErrorTime time.Time
    errorCode     string
}

type DefaultManagedStreamingPolicy struct {
    mu                   sync.RWMutex
    defaultToQueuedByTable map[string]*tableState
    // ...
}
```

The policy decides streaming vs queued based on:
- Error code (`STREAMING_INGESTION_OFF` → long backoff)
- Throttle (`THROTTLED` → short backoff)
- Time elapsed since last error

### 6.4 Config Cache Lifecycle

```go
type DefaultConfigurationCache struct {
    client          *ConfigurationClient
    cached          *CachedConfigurationData  // protected by sync.RWMutex
    mu              sync.RWMutex
    refreshInterval time.Duration
    stopCh          chan struct{}              // closed on Close()
}
```

- `GetConfiguration()` returns cached data or fetches on first call
- Background goroutine refreshes on `time.Ticker` interval
- `Close()` sends stop signal, goroutine exits

### 6.5 Uploader Polymorphism (No Inheritance)

Java uses class inheritance (`ManagedUploader extends ContainerUploaderBase`).
Go doesn't have inheritance. Use a **function field** instead:

```go
type ContainerUploaderBase struct {
    selectContainersFunc func(config *CachedConfigurationData) *RoundRobinContainerList
    // ...
}
```

`ManagedUploader` sets this function to choose storage vs lake containers
based on the server's `PreferredUploadMethod`.

---

## 7. Testing Strategy

### What to Test (High Value)

| Component                    | Test Focus                                                  |
|------------------------------|-------------------------------------------------------------|
| `RoundRobinContainerList`    | Even distribution across containers, wrap-around behavior    |
| `FastSuffixMatcher`          | Trusted endpoint matching, edge cases                        |
| `SimpleRetryPolicy`          | Delay calculations, attempt limits                           |
| `DefaultManagedStreamingPolicy` | Throttle backoff, per-table state isolation, time-based resume |
| Error types                  | `IsPermanent()`, `errors.As()` unwrapping                    |
| `EndpointUtils`              | URL prefix manipulation (ingest- prefix add/remove)          |

### Test After Each Phase

```bash
# After implementing each phase:
go build ./...    # Does it compile?
go test ./...     # Do tests pass?
go vet ./...      # Any suspicious code?
```

---

## 8. HTTP Stubs & Integration

The following methods need real HTTP implementations using the Azure SDK.
Start with stubs that return `errors.New("not implemented")`:

| Method                    | What It Does                          | Azure SDK Needed       |
|---------------------------|---------------------------------------|------------------------|
| `APIClient.PostQueuedIngest()`    | POST ingestion message to DM queue   | `azqueue`              |
| `APIClient.PostStreamingIngest()` | POST data directly to Engine         | `net/http` (custom)    |
| `APIClient.GetIngestStatus()`     | GET operation status from DM         | `net/http` (custom)    |
| `ContainerUploaderBase.uploadToBlob()` | Upload data to Azure Blob Storage | `azblob`               |

Wire these in **last**, once the architecture is solid and unit tests pass.

---

## 9. Estimated Effort

| Phase                              | Effort        |
|------------------------------------|---------------|
| Foundation + Source types           | ~0.5 day      |
| Configuration + Containers         | ~0.5 day      |
| Endpoints + Policies               | ~0.5 day      |
| Uploader + Clients + Factory       | ~1 day        |
| Unit tests                         | ~0.5-1 day    |
| Azure SDK integration (HTTP calls) | ~1-1.5 days   |
| **Total**                          | **~3-5 days** |

For a developer familiar with both Go and the Java Kusto SDK. The hardest parts are
the managed streaming fallback logic and the config cache lifecycle management.
