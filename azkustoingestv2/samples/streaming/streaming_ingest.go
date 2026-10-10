// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// This sample demonstrates streaming ingestion using the v2 SDK.
// Streaming ingestion provides low-latency data ingestion for small payloads (<4MB).
//
// Usage:
//
//	export KUSTO_ENDPOINT="https://mycluster.kusto.windows.net"
//	export KUSTO_DATABASE="mydb"
//	export KUSTO_TABLE="mytable"
//	export KUSTO_TOKEN="eyJ0eX..."
//	go run streaming_ingest.go
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
	database := mustEnv("KUSTO_DATABASE")
	table := mustEnv("KUSTO_TABLE")
	token := mustEnv("KUSTO_TOKEN")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Create streaming client with static token authentication
	client, err := azkustoingestv2.NewStreamingClient(
		endpoint,
		azkustoingestv2.WithTokenProvider(staticTokenProvider(token)),
	)
	if err != nil {
		log.Fatalf("Failed to create streaming client: %v", err)
	}
	defer client.Close()

	// --- Example 1: Ingest from a local CSV file ---
	fmt.Println("=== Example 1: Streaming from local CSV file ===")
	ingestFromFile(ctx, client, database, table)

	// --- Example 2: Ingest from an in-memory stream ---
	fmt.Println("\n=== Example 2: Streaming from in-memory data ===")
	ingestFromStream(ctx, client, database, table)

	fmt.Println("\nAll ingestion operations completed successfully!")
}

func ingestFromFile(ctx context.Context, client *azkustoingestv2.StreamingIngestClient, database, table string) {
	// Create a temporary CSV file
	tmpFile, err := os.CreateTemp("", "sample-*.csv")
	if err != nil {
		log.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	_, _ = tmpFile.WriteString("2024-01-15T10:30:00Z,abc-123,v1.0,Hello World,user1\n")
	_, _ = tmpFile.WriteString("2024-01-15T10:31:00Z,def-456,v1.0,Sample Data,user2\n")
	tmpFile.Close()

	// Create a FileSource pointing to our CSV
	source := ingestoptions.NewFileSource(tmpFile.Name(), ingestoptions.FormatCSV)
	defer source.Close()

	// Set ingestion properties
	props := &ingestoptions.IngestRequestProperties{
		Format: ingestoptions.FormatCSV,
	}

	// Execute the ingestion
	resp, err := client.Ingest(ctx, database, table, source, props)
	if err != nil {
		log.Fatalf("Streaming ingestion from file failed: %v", err)
	}

	fmt.Printf("  Success! Kind=%s, OperationID=%s\n", resp.Kind, resp.Response.OperationID)
}

func ingestFromStream(ctx context.Context, client *azkustoingestv2.StreamingIngestClient, database, table string) {
	// Create in-memory CSV data
	csvData := "2024-01-15T11:00:00Z,ghi-789,v2.0,Stream Data,user3\n" +
		"2024-01-15T11:01:00Z,jkl-012,v2.0,More Data,user4\n"

	// Create a StreamSource from the in-memory data
	source := ingestoptions.NewStreamSource(
		io.NopCloser(strings.NewReader(csvData)),
		ingestoptions.FormatCSV,
	)
	defer source.Close()

	props := &ingestoptions.IngestRequestProperties{
		Format: ingestoptions.FormatCSV,
	}

	resp, err := client.Ingest(ctx, database, table, source, props)
	if err != nil {
		log.Fatalf("Streaming ingestion from stream failed: %v", err)
	}

	fmt.Printf("  Success! Kind=%s, OperationID=%s\n", resp.Kind, resp.Response.OperationID)
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
