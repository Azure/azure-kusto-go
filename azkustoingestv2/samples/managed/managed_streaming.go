// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// This sample demonstrates managed streaming ingestion, which automatically
// chooses between streaming and queued ingestion based on data size and server
// response. This is the recommended client for production use.
//
// Usage:
//
//	export KUSTO_ENDPOINT="https://mycluster.kusto.windows.net"
//	export KUSTO_DM_ENDPOINT="https://ingest-mycluster.kusto.windows.net"
//	export KUSTO_DATABASE="mydb"
//	export KUSTO_TABLE="mytable"
//	export KUSTO_TOKEN="eyJ0eX..."
//	go run managed_streaming.go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func main() {
	endpoint := mustEnv("KUSTO_ENDPOINT")
	dmEndpoint := mustEnv("KUSTO_DM_ENDPOINT")
	database := mustEnv("KUSTO_DATABASE")
	table := mustEnv("KUSTO_TABLE")
	token := mustEnv("KUSTO_TOKEN")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Create managed streaming client — needs both DM and Engine URLs
	client, err := azkustoingestv2.NewManagedStreamingClient(
		dmEndpoint,  // Data Management endpoint (for queued fallback)
		endpoint,    // Engine endpoint (for streaming)
		azkustoingestv2.WithTokenProvider(staticTokenProvider(token)),
	)
	if err != nil {
		log.Fatalf("Failed to create managed streaming client: %v", err)
	}
	defer client.Close()

	// --- Example 1: Small data → goes through streaming path ---
	fmt.Println("=== Example 1: Small data (streaming path) ===")
	ingestSmallData(ctx, client, database, table)

	// --- Example 2: Large data → automatically falls back to queued ---
	fmt.Println("\n=== Example 2: Large data (queued fallback) ===")
	ingestLargeData(ctx, client, database, table)

	// --- Example 3: JSON with mapping reference ---
	fmt.Println("\n=== Example 3: JSON with mapping reference ===")
	ingestJSON(ctx, client, database, table)

	// --- Example 4: Using the properties builder ---
	fmt.Println("\n=== Example 4: Using IngestRequestPropertiesBuilder ===")
	ingestWithBuilder(ctx, client, database, table)

	fmt.Println("\nAll managed streaming operations completed!")
}

func ingestSmallData(ctx context.Context, client *azkustoingestv2.ManagedStreamingIngestClient, database, table string) {
	// Small CSV data — will be streamed directly to engine
	csvData := "2024-01-15T10:30:00Z,abc-123,v1.0,Hello World,user1\n"

	source := ingestoptions.NewStreamSource(
		io.NopCloser(strings.NewReader(csvData)),
		ingestoptions.FormatCSV,
	)
	defer source.Close()

	props := &ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV}

	resp, err := client.Ingest(ctx, database, table, source, props)
	if err != nil {
		log.Fatalf("Small data ingestion failed: %v", err)
	}

	fmt.Printf("  Success! Used %s path, OperationID=%s\n", resp.Kind, resp.Response.OperationID)
}

func ingestLargeData(ctx context.Context, client *azkustoingestv2.ManagedStreamingIngestClient, database, table string) {
	// Generate data larger than streaming limit (~4MB)
	// This will automatically fall back to queued ingestion
	bigRow := fmt.Sprintf("2024-01-15T12:00:00Z,big-data,v1.0,%s,user5\n",
		strings.Repeat("X", 5*1024*1024)) // 5MB payload

	source := ingestoptions.NewStreamSource(
		io.NopCloser(strings.NewReader(bigRow)),
		ingestoptions.FormatCSV,
	)
	defer source.Close()

	props := &ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV}

	resp, err := client.Ingest(ctx, database, table, source, props)
	if err != nil {
		// Note: This may fail if blob upload isn't configured.
		// In production, the queued path uploads to blob storage first.
		fmt.Printf("  Expected: large data triggers queued fallback. Error: %v\n", err)
		return
	}

	fmt.Printf("  Success! Used %s path (queued fallback), OperationID=%s\n",
		resp.Kind, resp.Response.OperationID)
}

func ingestJSON(ctx context.Context, client *azkustoingestv2.ManagedStreamingIngestClient, database, table string) {
	// JSON data with a mapping reference
	jsonData := `{"header":{"time":"2024-01-15T13:00:00Z","id":"mno-345","api_version":"v3.0"},"payload":{"data":"JSON ingestion","user":"user6"}}
{"header":{"time":"2024-01-15T13:01:00Z","id":"pqr-678","api_version":"v3.0"},"payload":{"data":"More JSON","user":"user7"}}
`

	source := ingestoptions.NewStreamSource(
		io.NopCloser(strings.NewReader(jsonData)),
		ingestoptions.FormatMultiJSON,
	)
	defer source.Close()

	props := &ingestoptions.IngestRequestProperties{
		Format:              ingestoptions.FormatMultiJSON,
		IngestionMappingRef: "Logs_mapping", // Must exist on the target table
	}

	resp, err := client.Ingest(ctx, database, table, source, props)
	if err != nil {
		log.Fatalf("JSON ingestion failed: %v", err)
	}

	fmt.Printf("  Success! Used %s path, OperationID=%s\n", resp.Kind, resp.Response.OperationID)
}

func ingestWithBuilder(ctx context.Context, client *azkustoingestv2.ManagedStreamingIngestClient, database, table string) {
	// Use the builder pattern for cleaner property construction
	props, err := ingestoptions.NewIngestRequestPropertiesBuilder().
		WithFormat(ingestoptions.FormatCSV).
		WithIgnoreFirstRecord(true).          // Skip header row
		WithIngestIfNotExists("unique-run-1"). // Idempotent ingestion
		WithDropByTags([]string{"v1.0"}).      // Tag for later drop-by
		Build()
	if err != nil {
		log.Fatalf("Failed to build properties: %v", err)
	}

	csvData := "header_time,header_id,api_version,data,user\n" + // Header (will be skipped)
		"2024-01-15T14:00:00Z,stu-901,v4.0,Builder pattern,user8\n"

	source := ingestoptions.NewStreamSource(
		io.NopCloser(strings.NewReader(csvData)),
		ingestoptions.FormatCSV,
	)
	defer source.Close()

	resp, err := client.Ingest(ctx, database, table, source, props)
	if err != nil {
		log.Fatalf("Builder pattern ingestion failed: %v", err)
	}

	fmt.Printf("  Success! Used %s path, OperationID=%s\n", resp.Kind, resp.Response.OperationID)
}

func staticTokenProvider(token string) func(ctx context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		return token, nil
	}
}

func mustEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Environment variable %s is required", key)
	}
	return val
}
