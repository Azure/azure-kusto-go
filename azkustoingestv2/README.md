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
    FlushImmediately: true,
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

summary, _ := client.GetOperationSummary(ctx, op)
fmt.Printf("Succeeded: %d, Failed: %d\n", summary.Succeeded, summary.Failed)
```

## Source Types

| Source | Description | Usage |
|--------|-------------|-------|
| `FileSource` | Local file on disk | `ingestoptions.NewFileSource(path, format)` |
| `StreamSource` | In-memory stream (io.ReadCloser) | `ingestoptions.NewStreamSource(reader, format)` |
| `BlobSource` | Existing blob in Azure Storage | `ingestoptions.NewBlobSource(url, format)` |

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

// Stream that should be left open after ingestion
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
    FlushImmediately:    true,            // Skip batching (queued only)
    EnableTracking:      true,            // Enable status polling
    IgnoreFirstRecord:   true,            // Skip header row (CSV)
    IngestIfNotExists:   "unique-tag",    // Idempotent ingestion
    DropByTags:          []string{"v1"},  // Extent drop-by tags
    IngestByTags:        []string{"batch1"}, // Ingest-by dedup tags
}
```

Or use the builder pattern:

```go
props, err := ingestoptions.NewIngestRequestPropertiesBuilder().
    WithFormat(ingestoptions.FormatJSON).
    WithMappingRef("MyMapping").
    WithFlushImmediately(true).
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

## Client Options

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

## Managed Streaming Behavior

The `ManagedStreamingIngestClient` makes intelligent routing decisions:

1. **Size check** — If data exceeds ~4MB (configurable via policy), routes directly to queued
2. **Streaming attempt** — Sends to engine endpoint
3. **Retry on transient errors** — Retries with backoff
4. **Fallback on permanent errors** — Routes to queued ingestion

Error categories that trigger fallback:
- `STREAMING_INGESTION_OFF` — Streaming disabled on cluster
- `TABLE_CONFIGURATION_PREVENTS_STREAMING` — Update policy or schema issues
- `REQUEST_PROPERTIES_PREVENT_STREAMING` — Payload too large (413)
- `THROTTLED` — Request throttled (429)

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
