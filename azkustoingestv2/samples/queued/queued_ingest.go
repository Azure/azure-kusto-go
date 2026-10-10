// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// This sample demonstrates queued ingestion from blob storage with status tracking.
// Queued ingestion is suitable for high-volume, batch data loading scenarios.
//
// Usage:
//
//	export KUSTO_DM_ENDPOINT="https://ingest-mycluster.kusto.windows.net"
//	export KUSTO_DATABASE="mydb"
//	export KUSTO_TABLE="mytable"
//	export KUSTO_TOKEN="eyJ0eX..."
//	export BLOB_URL="https://storage.blob.core.windows.net/container/data.json?sas=..."
//	go run queued_ingest.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func main() {
	dmEndpoint := mustEnv("KUSTO_DM_ENDPOINT")
	database := mustEnv("KUSTO_DATABASE")
	table := mustEnv("KUSTO_TABLE")
	token := mustEnv("KUSTO_TOKEN")
	blobURL := mustEnv("BLOB_URL")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Create queued client pointing to the DM (Data Management) endpoint
	client, err := azkustoingestv2.NewQueuedClient(
		dmEndpoint,
		azkustoingestv2.WithTokenProvider(staticTokenProvider(token)),
	)
	if err != nil {
		log.Fatalf("Failed to create queued client: %v", err)
	}
	defer client.Close()

	// --- Example 1: Ingest a single blob with status tracking ---
	fmt.Println("=== Example 1: Queued ingestion from blob with tracking ===")
	ingestBlobWithTracking(ctx, client, database, table, blobURL)

	// --- Example 2: Ingest multiple blobs in one request ---
	fmt.Println("\n=== Example 2: Multi-blob queued ingestion ===")
	ingestMultipleBlobs(ctx, client, database, table, blobURL)

	fmt.Println("\nAll queued ingestion operations completed!")
}

func ingestBlobWithTracking(ctx context.Context, client *azkustoingestv2.QueuedIngestClient, database, table, blobURL string) {
	// Create a blob source
	blobSource, err := ingestoptions.NewBlobSource(blobURL, ingestoptions.FormatJSON)
	if err != nil {
		log.Fatalf("Failed to create blob source: %v", err)
	}

	// Enable tracking and flush immediately for faster results
	props := &ingestoptions.IngestRequestProperties{
		Format:              ingestoptions.FormatJSON,
		IngestionMappingRef: "Logs_mapping", // Must exist on the target table
		FlushImmediately:    true,
		EnableTracking:      true,
	}

	// Submit ingestion
	resp, err := client.IngestBlobs(ctx, database, table,
		[]*ingestoptions.BlobSource{blobSource}, props)
	if err != nil {
		log.Fatalf("IngestBlobs failed: %v", err)
	}

	fmt.Printf("  Submitted! OperationID=%s\n", resp.Response.OperationID)

	// Poll for status
	op := &azkustoingestv2.IngestionOperation{
		Database:    database,
		Table:       table,
		OperationID: resp.Response.OperationID,
		IngestKind:  resp.Kind,
	}

	fmt.Println("  Polling for status...")
	for {
		summary, err := client.GetOperationSummary(ctx, op)
		if err != nil {
			fmt.Printf("  Status poll error (retrying): %v\n", err)
			time.Sleep(5 * time.Second)
			continue
		}

		fmt.Printf("  Status: succeeded=%d, failed=%d, inProgress=%d\n",
			summary.Succeeded, summary.Failed, summary.InProgress)

		if summary.Succeeded > 0 || summary.Failed > 0 {
			if summary.Failed > 0 {
				// Get detailed failure information
				details, _ := client.GetOperationDetails(ctx, op)
				fmt.Printf("  FAILURE details: %+v\n", details)
			} else {
				fmt.Println("  Ingestion succeeded!")
			}
			break
		}

		time.Sleep(5 * time.Second)
	}
}

func ingestMultipleBlobs(ctx context.Context, client *azkustoingestv2.QueuedIngestClient, database, table, blobURL string) {
	// Create multiple blob sources (in practice these would be different blobs)
	blob1, _ := ingestoptions.NewBlobSource(blobURL+"&part=1", ingestoptions.FormatJSON)
	blob2, _ := ingestoptions.NewBlobSource(blobURL+"&part=2", ingestoptions.FormatJSON)

	props := &ingestoptions.IngestRequestProperties{
		Format:              ingestoptions.FormatJSON,
		IngestionMappingRef: "Logs_mapping",
		FlushImmediately:    true,
	}

	// Check how many blobs can be submitted in one request
	maxBlobs, err := client.MaxSourcesPerMultiIngest(ctx)
	if err != nil {
		log.Printf("Warning: could not get max blobs limit: %v", err)
	} else {
		fmt.Printf("  Max blobs per request: %d\n", maxBlobs)
	}

	// Submit multiple blobs in one request
	resp, err := client.IngestBlobs(ctx, database, table,
		[]*ingestoptions.BlobSource{blob1, blob2}, props)
	if err != nil {
		log.Fatalf("Multi-blob IngestBlobs failed: %v", err)
	}

	fmt.Printf("  Multi-blob submitted! OperationID=%s\n", resp.Response.OperationID)
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
