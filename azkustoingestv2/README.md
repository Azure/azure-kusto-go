# azkustoingestv2 — Azure Data Explorer Ingestion SDK v2

The `azkustoingestv2` module provides the next-generation ingestion client for Azure Data Explorer (Kusto).
It implements queued, streaming, and managed streaming ingestion patterns with a clean, explicit API.

## Installation

```bash
go get github.com/Azure/azure-kusto-go/azkustoingestv2
```

## Overview

The SDK provides three client types for different ingestion scenarios:

| Client | Use Case | How It Works |
|--------|----------|--------------|
| `QueuedIngestClient` | Batch/high-volume ingestion | Uploads data to blob → submits to DM service → batched ingestion |
| `StreamingIngestClient` | Low-latency, small payloads | Sends data directly to engine endpoint |
| `ManagedStreamingIngestClient` | Best of both worlds | Tries streaming first, falls back to queued on failure/size limits |

## Quick Start

### Streaming Ingestion (simplest)

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/Azure/azure-kusto-go/azkustoingestv2"
    "github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func main() {
    ctx := context.Background()

    // Create a streaming client
    client, err := azkustoingestv2.NewStreamingClient(
        "https://mycluster.kusto.windows.net",
        azkustoingestv2.WithTokenProvider(myTokenProvider),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Ingest a local CSV file
    source := ingestoptions.NewFileSource("data.csv", ingestoptions.FormatCSV)
    props := &ingestoptions.IngestRequestProperties{
        Format: ingestoptions.FormatCSV,
    }

    resp, err := client.Ingest(ctx, "mydb", "mytable", source, props)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Ingested via %s, operationID: %s\n", resp.Kind, resp.Response.OperationID)
}
```

Direct streaming sends local file and stream data incrementally, applying gzip
when eligible without buffering the complete input. Like legacy Go, it delegates
streaming size enforcement to the service. Managed ingestion still uses a bounded
prefix to choose streaming or queued ingestion.

### Managed Streaming (recommended for production)

```go
client, err := azkustoingestv2.NewManagedStreamingClient(
    "https://ingest-mycluster.kusto.windows.net",  // DM URL
    "https://mycluster.kusto.windows.net",          // Engine URL
    azkustoingestv2.WithTokenProvider(myTokenProvider),
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Small data → streaming; large data → queued automatically
source := ingestoptions.NewFileSource("data.json", ingestoptions.FormatJSON)
props := &ingestoptions.IngestRequestProperties{
    Format:              ingestoptions.FormatJSON,
    IngestionMappingRef: "MyJsonMapping",
}

resp, err := client.Ingest(ctx, "mydb", "mytable", source, props)
fmt.Printf("Used %s path\n", resp.Kind) // "Streaming" or "Queued"
```

### Queued Ingestion with Status Tracking

```go
client, err := azkustoingestv2.NewQueuedClient(
    "https://ingest-mycluster.kusto.windows.net",
    azkustoingestv2.WithTokenProvider(myTokenProvider),
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Ingest from an existing blob
blobSource, _ := ingestoptions.NewBlobSource(
    "https://storage.blob.core.windows.net/container/data.json?sas=...",
    ingestoptions.FormatJSON,
)

props := &ingestoptions.IngestRequestProperties{
    Format:           ingestoptions.FormatJSON,
    SkipBatching:     true,
    EnableTracking:   true,
}

resp, err := client.IngestBlobs(ctx, "mydb", "mytable",
    []*ingestoptions.BlobSource{blobSource}, props)
if err != nil {
    log.Fatal(err)
}

// Poll for completion
op := &azkustoingestv2.IngestionOperation{
    Database:    "mydb",
    Table:       "mytable",
    OperationID: resp.Response.OperationID,
    IngestKind:  resp.Kind,
}

result, err := client.PollForCompletion(ctx, op,
    azkustoingestv2.WithPollingInterval(5*time.Second),
    azkustoingestv2.WithPollingTimeout(5*time.Minute),
)
if err != nil {
    log.Fatalf("Ingestion did not complete successfully: %v; last status: %+v", err, result)
}
fmt.Printf("Ingestion completed: %+v\n", result)
```

`PollForCompletion` is explicit: `EnableTracking` only enables service-side tracking,
not background polling. Queued and managed clients implement `OperationPoller`;
managed requests that used direct streaming cannot be polled. Defaults are a
30-second interval and a 5-minute timeout. Context cancellation or an earlier
context deadline stops polling, including an in-flight status request. Failed or
canceled ingestions return their final status with an error; status-request errors
stop polling without automatically retrying or resubmitting ingestion.

## Source Types

| Source | Description | Usage |
|--------|-------------|-------|
| `FileSource` | Local file on disk | `ingestoptions.NewFileSource(path, format)` |
| `StreamSource` | In-memory stream (io.ReadCloser) | `ingestoptions.NewStreamSource(reader, format)` |
| `BlobSource` | Existing blob in Azure Storage | `ingestoptions.NewBlobSource(url, format)` |

Clients close SDK-opened `FileSource` handles when `Ingest` returns, on success
or failure and after any managed retries or queued fallback. All ingestion modes
close `StreamSource` readers when the complete call finishes unless
`WithLeaveOpen(true)` is set. Readers stay available through uploads, retries,
and managed fallback; the ownership rule does not depend on the ingestion route.

### Source Options

```go
// File with explicit compression
source := ingestoptions.NewFileSource("data.csv.gz", ingestoptions.FormatCSV,
    ingestoptions.WithFileCompression(ingestoptions.CompressionGZip),
)

// Blob with known size
blobSource, _ := ingestoptions.NewBlobSource(blobURL, ingestoptions.FormatJSON,
    ingestoptions.WithBlobCompression(ingestoptions.CompressionGZip),
)
blobSource.BlobExactSize = 1024000

// Stream whose reader should remain open after ingestion and source.Close()
source := ingestoptions.NewStreamSource(myReader, ingestoptions.FormatCSV,
    ingestoptions.WithLeaveOpen(true),
)
```

## Ingestion Properties

Use `IngestRequestProperties` to configure ingestion behavior:

```go
props := &ingestoptions.IngestRequestProperties{
    Format:              ingestoptions.FormatJSON,
    IngestionMappingRef: "MyMapping",     // Reference to pre-defined mapping
    SkipBatching:        true,            // Skip batching (queued only)
    EnableTracking:      true,            // Enable service-side status tracking
    IgnoreFirstRecord:   true,            // Skip header row (CSV)
    IngestIfNotExists:   []string{"ingest-by:unique-tag"}, // Idempotent ingestion
    DropByTags:          []string{"v1"},  // Extent drop-by tags
    IngestByTags:        []string{"unique-tag"}, // Ingest-by dedup tags
}
```

Mapping behavior follows legacy Go: queued ingestion accepts inline mappings and
mapping references together and forwards both to the service. Direct streaming
rejects inline mappings and supports mapping references only. Managed ingestion
accepts both, uses only the reference during streaming, and retains both for queued
routing or fallback. The SDK does not guarantee that the service accepts both
mapping options in a queued request.

Or use the builder pattern:

```go
props, err := ingestoptions.NewIngestRequestPropertiesBuilder().
    WithFormat(ingestoptions.FormatJSON).
    WithMappingRef("MyMapping").
    WithSkipBatching(true).
    WithTracking(true).
    Build()
```

## Authentication

The SDK accepts a `TokenProvider` function for flexible authentication:

```go
// Static token
azkustoingestv2.WithTokenProvider(func(ctx context.Context) (string, error) {
    return "eyJ0eX...", nil
})

// Azure Identity SDK
import "github.com/Azure/azure-sdk-for-go/sdk/azidentity"

cred, _ := azidentity.NewDefaultAzureCredential(nil)
azkustoingestv2.WithTokenProvider(func(ctx context.Context) (string, error) {
    token, err := cred.GetToken(ctx, policy.TokenRequestOptions{
        Scopes: []string{"https://kusto.kusto.windows.net/.default"},
    })
    return token.Token, err
})

// Service Principal via azkustodata
import "github.com/Azure/azure-kusto-go/azkustodata"

kcsb := azkustodata.NewConnectionStringBuilder(endpoint).
    WithAadAppKey(clientID, clientSecret, tenantID)
client, _ := azkustodata.New(kcsb)
tkp := client.Auth().TokenProvider
azkustoingestv2.WithTokenProvider(func(ctx context.Context) (string, error) {
    token, _, err := tkp.AcquireToken(ctx)
    return token, err
})
```

### OneLake uploads

Local files and streams are uploaded using the Blob or Data Lake API according
to the selected container's upload method. For OneLake bearer authentication,
pass an Azure Identity credential with
`azkustoingestv2.WithUploadTokenCredential(cred)`. The uploader requests a storage
token separately from the Kusto token supplied by `WithTokenProvider`.
Without an upload credential, the uploader uses the destination's SAS token,
if present, or sends the request without bearer authentication.

For Fabric Private Link, configure both `WithS2STokenProvider` and a nonblank
`WithS2SFabricAccessContext`. These headers are also applied to OneLake upload
requests, including retries. `WithHTTPClient` applies to both API and upload
requests.

Azure SDKs retry individual transfer requests. Retrying an entire upload against
another container additionally requires a seekable source; non-seekable sources
return an error rather than retrying with partially consumed data.

## Client Options

Client factories correct endpoint hostnames by default, like legacy Go ingestion:
queued clients add `ingest-`, streaming clients remove it, and managed clients
correct both URLs to their respective roles. For example,
`NewManagedStreamingClient(clusterURL, clusterURL)` derives the DM endpoint from
the same Engine URL. Correction also applies to `WithDMURL` and `WithEngineURL`
overrides. Ports, paths, and query strings are preserved; localhost, IP, and onebox
development endpoints are unchanged. Use `WithoutEndpointCorrection()` to preserve
the supplied URLs exactly. This option does not disable trusted-host validation.

Kusto API endpoints are validated against the SDK's trusted-host rules by default.
Localhost and IP endpoints remain supported for development.
For a trusted custom hostname, explicitly opt out with `WithSkipSecurityChecks(true)`.
This option disables hostname trust checks, not TLS certificate verification.

`WithMaxDataSize` limits local source bytes before SDK-added compression, including
streams of unknown length. `WithIgnoreSizeLimit(true)` bypasses that maximum, but
empty input and source read errors still fail ingestion.

```go
azkustoingestv2.NewQueuedClient(dmURL,
    azkustoingestv2.WithTokenProvider(tokenFunc),
    azkustoingestv2.WithMaxConcurrency(4),         // Parallel uploads
    azkustoingestv2.WithIgnoreSizeLimit(true),      // Allow large blobs
    azkustoingestv2.WithConfigRefreshInterval(5*time.Minute),
    azkustoingestv2.WithHTTPClient(customClient),   // Custom HTTP client
)
```

## Data Formats

The SDK supports all Kusto ingestion formats:

| Format | Constant |
|--------|----------|
| CSV | `FormatCSV` |
| JSON | `FormatJSON` |
| Multi-line JSON | `FormatMultiJSON` |
| Avro | `FormatAvro` |
| Parquet | `FormatParquet` |
| ORC | `FormatORC` |
| TSV | `FormatTSV` |
| Single JSON | `FormatSingleJSON` |

## Error Handling

The SDK provides typed errors for precise error handling:

```go
resp, err := client.Ingest(ctx, db, table, source, props)
if err != nil {
    var sizeErr *ingestoptions.IngestSizeLimitExceededError
    var clientErr *ingestoptions.IngestClientError
    var reqErr *ingestoptions.IngestRequestError

    switch {
    case errors.As(err, &sizeErr):
        fmt.Printf("Data too large: %d > %d\n", sizeErr.ActualSize, sizeErr.MaxAllowedSize)
    case errors.As(err, &reqErr):
        fmt.Printf("Request error (permanent=%v): %s\n", reqErr.IsPermanent, reqErr.ErrorMessage)
    case errors.As(err, &clientErr):
        fmt.Printf("Client error: %s\n", clientErr.Message)
    default:
        fmt.Printf("Unknown error: %v\n", err)
    }
}
```

## Service Configuration

Like Java V2's `ConfigurationClient.getConfigurationDetails()`, Go exposes a
standalone configuration client. Each getter call makes a fresh authenticated
request; caching is separate from this client.

```go
client, err := azkustoingestv2.NewConfigurationClient(
    "https://ingest-mycluster.kusto.windows.net",
    azkustoingestv2.WithTokenProvider(myTokenProvider),
)
if err != nil {
    log.Fatal(err)
}
configuration, err := client.GetConfigurationDetails(ctx)
if err != nil {
    log.Fatal(err)
}
```

Connectors can inspect `configuration.IngestionSettings.PreferredIngestionMethod`
(`Rest`) and `configuration.ContainerSettings.PreferredUploadMethod` (`Lake`),
checking for nil settings, to select an ingestion path. These preferences do not
automatically switch the SDK to legacy ingestion. Upload target paths may contain
SAS tokens; do not log the configuration.

## Managed Streaming Behavior

The `ManagedStreamingIngestClient` uses built-in routing and retry behavior,
as in legacy Go. Custom policy injection and callbacks are not part of the public API.

1. **Size check** — Data exceeding the built-in streaming threshold routes directly to queued
2. **Streaming attempt** — Sends to engine endpoint
3. **Retry on transient errors** — Retries with backoff
4. **Fallback after transient retries are exhausted** — Routes to queued ingestion

Permanent streaming errors are returned immediately, without retries or queued
fallback, as in legacy Go. This includes permanent authorization, schema,
streaming-disabled, and payload-too-large errors. These failures do not cause
subsequent requests to bypass streaming. Size-based routing before streaming is
unchanged.

## Running Tests

```bash
# Unit tests (no cluster needed)
cd azkustoingestv2 && go test ./... -count=1

# E2E tests (requires cluster access)
export ENGINE_CONNECTION_STRING="https://cluster.kusto.windows.net"
export TEST_DATABASE="testdb"
export KUSTO_ACCESS_TOKEN="eyJ..."  # or AZURE_CLIENT_ID/SECRET/TENANT_ID
export BLOB_URI_FOR_TEST="https://storage.blob.core.windows.net/container/demo.json?sas"
go test ./test/etoe/ -v -count=1
```

## Samples

See the [`samples/`](./samples/) directory for complete working examples:
- [`samples/streaming/`](./samples/streaming/streaming_ingest.go) — Streaming ingestion from file and stream
- [`samples/queued/`](./samples/queued/queued_ingest.go) — Queued ingestion from blob with status tracking
- [`samples/managed/`](./samples/managed/managed_streaming.go) — Managed streaming with automatic fallback
