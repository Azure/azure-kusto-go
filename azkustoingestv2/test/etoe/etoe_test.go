// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package etoe

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/testshared"
	azkustoingestv2 "github.com/Azure/azure-kusto-go/azkustoingestv2"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

var countStatement = kql.New("table(tableName) | count")

type CountResult struct {
	Count int64
}

// TestStreamingIngestion tests streaming ingestion via the v2 StreamingIngestClient.
func TestStreamingIngestion(t *testing.T) {
	t.Parallel()
	if skipETOE || testing.Short() {
		t.Skip("end to end tests disabled: missing config.json file in etoe directory")
	}

	tokenProvider, cleanup, err := testConfig.newTokenProvider()
	if err != nil {
		t.Fatalf("failed to create token provider: %v", err)
	}
	defer cleanup()

	queryClient, err := azkustodata.New(testConfig.kcsb)
	if err != nil {
		t.Fatalf("failed to create query client: %v", err)
	}
	defer queryClient.Close()
	testshared.SetDefaultDatabase(testConfig.Database)

	streamingClient, err := azkustoingestv2.NewStreamingClient(
		testConfig.engineURL,
		azkustoingestv2.WithTokenProvider(tokenProvider),
	)
	if err != nil {
		t.Fatalf("failed to create streaming client: %v", err)
	}
	defer streamingClient.Close()

	tests := []struct {
		desc      string
		source    func(t *testing.T) ingestoptions.IngestionSource
		props     *ingestoptions.IngestRequestProperties
		wantCount int64
		wantErr   bool
		skipNoBlob bool
	}{
		{
			desc: "Streaming from local CSV file",
			source: func(t *testing.T) ingestoptions.IngestionSource {
				return ingestoptions.NewFileSource(csvFileFromString(t), ingestoptions.FormatCSV)
			},
			props:     &ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV},
			wantCount: 3,
		},
		{
			desc: "Streaming from StreamSource CSV",
			source: func(t *testing.T) ingestoptions.IngestionSource {
				data := "2020-03-10T20:59:30.694177Z,11196991-b193-4610-ae12-bcc03d092927,v0.0.1,Hello world!,Daniel Dubovski\n" +
					"2020-03-10T20:59:30.694177Z,,v0.0.2,,\n"
				return ingestoptions.NewStreamSource(io.NopCloser(strings.NewReader(data)), ingestoptions.FormatCSV)
			},
			props:     &ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV},
			wantCount: 2,
		},
		{
			desc: "Streaming from local JSON with mapping ref",
			source: func(t *testing.T) ingestoptions.IngestionSource {
				return ingestoptions.NewFileSource("testdata/demo.json", ingestoptions.FormatMultiJSON)
			},
			props: &ingestoptions.IngestRequestProperties{
				Format:              ingestoptions.FormatMultiJSON,
				IngestionMappingRef: "Logs_mapping",
			},
			wantCount: 500,
		},
		{
			desc: "Streaming from blob with mapping ref",
			source: func(t *testing.T) ingestoptions.IngestionSource {
				if testConfig.Blob == "" {
					t.Skip("no blob URL configured")
				}
				bs, err := ingestoptions.NewBlobSource(testConfig.Blob, ingestoptions.FormatMultiJSON)
				if err != nil {
					t.Fatalf("failed to create blob source: %v", err)
				}
				return bs
			},
			props: &ingestoptions.IngestRequestProperties{
				Format:              ingestoptions.FormatMultiJSON,
				IngestionMappingRef: "Logs_mapping",
			},
			wantCount:  500,
			skipNoBlob: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			tableName := uniqueTableName("goe2e_v2_streaming")
			if err := testshared.CreateTestTable(t, queryClient, tableName); err != nil {
				t.Fatalf("failed to create test table: %v", err)
			}

			source := test.source(t)
			defer source.Close()

			resp, err := streamingClient.Ingest(ctx, testConfig.Database, tableName, source, test.props)
			if test.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Ingest failed: %v", err)
			}

			if resp.Kind != ingestoptions.IngestKindStreaming {
				t.Errorf("expected IngestKindStreaming, got %v", resp.Kind)
			}

			waitForRowCount(t, ctx, queryClient, testConfig.Database, tableName, test.wantCount)
		})
	}
}

// TestManagedStreamingIngestion tests the managed streaming client (streaming with queued fallback).
func TestManagedStreamingIngestion(t *testing.T) {
	t.Parallel()
	if skipETOE || testing.Short() {
		t.Skip("end to end tests disabled: missing config.json file in etoe directory")
	}

	tokenProvider, cleanup, err := testConfig.newTokenProvider()
	if err != nil {
		t.Fatalf("failed to create token provider: %v", err)
	}
	defer cleanup()

	queryClient, err := azkustodata.New(testConfig.kcsb)
	if err != nil {
		t.Fatalf("failed to create query client: %v", err)
	}
	defer queryClient.Close()
	testshared.SetDefaultDatabase(testConfig.Database)

	managedClient, err := azkustoingestv2.NewManagedStreamingClient(
		testConfig.dmURL, testConfig.engineURL,
		azkustoingestv2.WithTokenProvider(tokenProvider),
	)
	if err != nil {
		t.Fatalf("failed to create managed streaming client: %v", err)
	}
	defer managedClient.Close()

	tests := []struct {
		desc      string
		source    func(t *testing.T) ingestoptions.IngestionSource
		props     *ingestoptions.IngestRequestProperties
		wantCount int64
	}{
		{
			desc: "Small CSV file via managed streaming",
			source: func(t *testing.T) ingestoptions.IngestionSource {
				return ingestoptions.NewFileSource(csvFileFromString(t), ingestoptions.FormatCSV)
			},
			props:     &ingestoptions.IngestRequestProperties{Format: ingestoptions.FormatCSV},
			wantCount: 3,
		},
		{
			desc: "JSON file with mapping via managed streaming",
			source: func(t *testing.T) ingestoptions.IngestionSource {
				return ingestoptions.NewFileSource("testdata/demo.json", ingestoptions.FormatMultiJSON)
			},
			props: &ingestoptions.IngestRequestProperties{
				Format:              ingestoptions.FormatMultiJSON,
				IngestionMappingRef: "Logs_mapping",
			},
			wantCount: 500,
		},
		{
			desc: "Blob via managed streaming",
			source: func(t *testing.T) ingestoptions.IngestionSource {
				if testConfig.Blob == "" {
					t.Skip("no blob URL configured")
				}
				bs, err := ingestoptions.NewBlobSource(testConfig.Blob, ingestoptions.FormatMultiJSON)
				if err != nil {
					t.Fatalf("failed to create blob source: %v", err)
				}
				return bs
			},
			props: &ingestoptions.IngestRequestProperties{
				Format:              ingestoptions.FormatMultiJSON,
				IngestionMappingRef: "Logs_mapping",
			},
			wantCount: 500,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			tableName := uniqueTableName("goe2e_v2_managed")
			if err := testshared.CreateTestTable(t, queryClient, tableName); err != nil {
				t.Fatalf("failed to create test table: %v", err)
			}

			source := test.source(t)
			defer source.Close()

			resp, err := managedClient.Ingest(ctx, testConfig.Database, tableName, source, test.props)
			if err != nil {
				t.Fatalf("Ingest failed: %v", err)
			}
			t.Logf("Ingestion completed with kind=%s, operationID=%s", resp.Kind, resp.Response.OperationID)

			waitForRowCount(t, ctx, queryClient, testConfig.Database, tableName, test.wantCount)
		})
	}
}

// TestQueuedIngestion tests the queued ingestion client with blob sources.
func TestQueuedIngestion(t *testing.T) {
	t.Parallel()
	if skipETOE || testing.Short() {
		t.Skip("end to end tests disabled: missing config.json file in etoe directory")
	}
	if testConfig.Blob == "" {
		t.Skip("no blob URL configured for queued ingestion test")
	}

	tokenProvider, cleanup, err := testConfig.newTokenProvider()
	if err != nil {
		t.Fatalf("failed to create token provider: %v", err)
	}
	defer cleanup()

	queryClient, err := azkustodata.New(testConfig.kcsb)
	if err != nil {
		t.Fatalf("failed to create query client: %v", err)
	}
	defer queryClient.Close()
	testshared.SetDefaultDatabase(testConfig.Database)

	queuedClient, err := azkustoingestv2.NewQueuedClient(
		testConfig.dmURL,
		azkustoingestv2.WithTokenProvider(tokenProvider),
	)
	if err != nil {
		t.Fatalf("failed to create queued client: %v", err)
	}
	defer queuedClient.Close()

	tests := []struct {
		desc      string
		source    func(t *testing.T) []*ingestoptions.BlobSource
		props     *ingestoptions.IngestRequestProperties
		wantCount int64
		wantErr   bool
	}{
		{
			desc: "Queued from blob with mapping ref",
			source: func(t *testing.T) []*ingestoptions.BlobSource {
				bs, err := ingestoptions.NewBlobSource(testConfig.Blob, ingestoptions.FormatMultiJSON)
				if err != nil {
					t.Fatalf("failed to create blob source: %v", err)
				}
				return []*ingestoptions.BlobSource{bs}
			},
			props: &ingestoptions.IngestRequestProperties{
				Format:              ingestoptions.FormatMultiJSON,
				IngestionMappingRef: "Logs_mapping",
				FlushImmediately:    true,
				EnableTracking:      true,
			},
			wantCount: 500,
		},
		{
			desc: "Queued from blob with bad mapping",
			source: func(t *testing.T) []*ingestoptions.BlobSource {
				bs, err := ingestoptions.NewBlobSource(testConfig.Blob, ingestoptions.FormatMultiJSON)
				if err != nil {
					t.Fatalf("failed to create blob source: %v", err)
				}
				return []*ingestoptions.BlobSource{bs}
			},
			props: &ingestoptions.IngestRequestProperties{
				Format:              ingestoptions.FormatMultiJSON,
				IngestionMappingRef: "NonExistentMapping",
				FlushImmediately:    true,
				EnableTracking:      true,
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			tableName := uniqueTableName("goe2e_v2_queued")
			if err := testshared.CreateTestTable(t, queryClient, tableName); err != nil {
				t.Fatalf("failed to create test table: %v", err)
			}

			// Reduce batching time for faster test completion
			batchingStmt := kql.New(".alter table ").AddTable(tableName).AddLiteral(
				` policy ingestionbatching @'{ "MaximumBatchingTimeSpan": "00:00:10", "MaximumNumberOfItems": 500, "MaximumRawDataSizeMB": 1024 }' `)
			_, err := queryClient.Mgmt(ctx, testConfig.Database, batchingStmt)
			if err != nil {
				t.Logf("Warning: failed to reduce batching time: %v", err)
			}

			sources := test.source(t)

			resp, err := queuedClient.IngestBlobs(ctx, testConfig.Database, tableName, sources, test.props)
			if test.wantErr {
				// For queued ingestion, the initial POST may succeed even with bad mapping.
				// The error surfaces during status polling.
				if err != nil {
					t.Logf("Immediate error (expected): %v", err)
					return
				}
				// If no immediate error, poll for failure via status tracking
				if resp != nil && test.props.EnableTracking {
					op := &azkustoingestv2.IngestionOperation{
						Database:    testConfig.Database,
						Table:       tableName,
						OperationID: resp.Response.OperationID,
						IngestKind:  resp.Kind,
					}
					waitForOperationFailure(t, ctx, queuedClient, op)
				}
				return
			}
			if err != nil {
				t.Fatalf("IngestBlobs failed: %v", err)
			}

			t.Logf("Queued ingestion submitted, operationID=%s", resp.Response.OperationID)
			waitForRowCount(t, ctx, queryClient, testConfig.Database, tableName, test.wantCount)
		})
	}
}

// TestQueuedStatusTracking tests operation status tracking for queued ingestion.
func TestQueuedStatusTracking(t *testing.T) {
	t.Parallel()
	if skipETOE || testing.Short() {
		t.Skip("end to end tests disabled: missing config.json file in etoe directory")
	}
	if testConfig.Blob == "" {
		t.Skip("no blob URL configured for status tracking test")
	}

	tokenProvider, cleanup, err := testConfig.newTokenProvider()
	if err != nil {
		t.Fatalf("failed to create token provider: %v", err)
	}
	defer cleanup()

	queryClient, err := azkustodata.New(testConfig.kcsb)
	if err != nil {
		t.Fatalf("failed to create query client: %v", err)
	}
	defer queryClient.Close()
	testshared.SetDefaultDatabase(testConfig.Database)

	queuedClient, err := azkustoingestv2.NewQueuedClient(
		testConfig.dmURL,
		azkustoingestv2.WithTokenProvider(tokenProvider),
	)
	if err != nil {
		t.Fatalf("failed to create queued client: %v", err)
	}
	defer queuedClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	tableName := uniqueTableName("goe2e_v2_status")
	if err := testshared.CreateTestTable(t, queryClient, tableName); err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	// Reduce batching time
	batchingStmt := kql.New(".alter table ").AddTable(tableName).AddLiteral(
		` policy ingestionbatching @'{ "MaximumBatchingTimeSpan": "00:00:10", "MaximumNumberOfItems": 500, "MaximumRawDataSizeMB": 1024 }' `)
	_, _ = queryClient.Mgmt(ctx, testConfig.Database, batchingStmt)

	bs, err := ingestoptions.NewBlobSource(testConfig.Blob, ingestoptions.FormatMultiJSON)
	if err != nil {
		t.Fatalf("failed to create blob source: %v", err)
	}

	props := &ingestoptions.IngestRequestProperties{
		Format:              ingestoptions.FormatMultiJSON,
		IngestionMappingRef: "Logs_mapping",
		FlushImmediately:    true,
		EnableTracking:      true,
	}

	resp, err := queuedClient.IngestBlobs(ctx, testConfig.Database, tableName, []*ingestoptions.BlobSource{bs}, props)
	if err != nil {
		t.Fatalf("IngestBlobs failed: %v", err)
	}

	op := &azkustoingestv2.IngestionOperation{
		Database:    testConfig.Database,
		Table:       tableName,
		OperationID: resp.Response.OperationID,
		IngestKind:  resp.Kind,
	}

	t.Logf("Polling operation status for operationID=%s", op.OperationID)

	// Poll until we see a non-zero summary
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		summary, err := queuedClient.GetOperationSummary(ctx, op)
		if err != nil {
			t.Logf("GetOperationSummary error (will retry): %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		t.Logf("Status: succeeded=%d, failed=%d, inProgress=%d",
			summary.Succeeded, summary.Failed, summary.InProgress)

		if summary.Succeeded > 0 {
			t.Logf("Operation succeeded")
			return
		}
		if summary.Failed > 0 {
			// Get details for debugging
			details, _ := queuedClient.GetOperationDetails(ctx, op)
			t.Fatalf("Operation failed. Details: %+v", details)
		}

		time.Sleep(5 * time.Second)
	}

	t.Fatal("Timed out waiting for operation to complete")
}

// TestClientValidation tests client-side validation errors.
func TestClientValidation(t *testing.T) {
	t.Parallel()
	if skipETOE || testing.Short() {
		t.Skip("end to end tests disabled")
	}

	tokenProvider, cleanup, err := testConfig.newTokenProvider()
	if err != nil {
		t.Fatalf("failed to create token provider: %v", err)
	}
	defer cleanup()

	streamingClient, err := azkustoingestv2.NewStreamingClient(
		testConfig.engineURL,
		azkustoingestv2.WithTokenProvider(tokenProvider),
	)
	if err != nil {
		t.Fatalf("failed to create streaming client: %v", err)
	}
	defer streamingClient.Close()

	t.Run("EmptyDatabase", func(t *testing.T) {
		ctx := context.Background()
		source := ingestoptions.NewFileSource(csvFileFromString(t), ingestoptions.FormatCSV)
		_, err := streamingClient.Ingest(ctx, "", "table", source, nil)
		if err == nil {
			t.Error("expected error for empty database")
		}
	})

	t.Run("EmptyTable", func(t *testing.T) {
		ctx := context.Background()
		source := ingestoptions.NewFileSource(csvFileFromString(t), ingestoptions.FormatCSV)
		_, err := streamingClient.Ingest(ctx, "db", "", source, nil)
		if err == nil {
			t.Error("expected error for empty table")
		}
	})

	t.Run("NonExistentFile", func(t *testing.T) {
		ctx := context.Background()
		source := ingestoptions.NewFileSource("/nonexistent/file.csv", ingestoptions.FormatCSV)
		_, err := streamingClient.Ingest(ctx, testConfig.Database, "table", source, nil)
		if err == nil {
			t.Error("expected error for non-existent file")
		}
	})

	t.Run("MappingRefAndInlineMappingConflict", func(t *testing.T) {
		props := &ingestoptions.IngestRequestProperties{
			Format:              ingestoptions.FormatJSON,
			IngestionMappingRef: "SomeMapping",
			IngestionMapping: []ingestoptions.ColumnMapping{
				{Name: "col1", MappingKind: "JsonMapping"},
			},
		}
		if err := props.Validate(); err == nil {
			t.Error("expected validation error for conflicting mapping ref and inline mapping")
		}
	})
}

// --- Helper functions ---

func uniqueTableName(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), rand.Int())
}

func csvFileFromString(t *testing.T) string {
	t.Helper()
	raw := `,,,,
2020-03-10T20:59:30.694177Z,11196991-b193-4610-ae12-bcc03d092927,v0.0.1,Hello world!,Daniel Dubovski
2020-03-10T20:59:30.694177Z,,v0.0.2,,`

	fname := fmt.Sprintf("%s/etoe_csv_%d.csv", t.TempDir(), time.Now().UnixNano())
	if err := os.WriteFile(fname, []byte(raw), 0644); err != nil {
		t.Fatalf("failed to write CSV file: %v", err)
	}
	return fname
}

// waitForRowCount polls the table until the expected row count appears.
func waitForRowCount(t *testing.T, ctx context.Context, client *azkustodata.Client, database, tableName string, wantCount int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)

	for time.Now().Before(deadline) {
		params := azkustodata.QueryParameters(kql.NewParameters().AddString("tableName", tableName))
		dataset, err := client.Query(ctx, database, countStatement, params)
		if err != nil {
			t.Logf("Query error (will retry): %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		rows := dataset.Tables()[0].Rows()
		if len(rows) > 0 {
			count := extractCount(t, rows[0])
			if count >= wantCount {
				t.Logf("Got expected row count: %d (wanted >= %d)", count, wantCount)
				return
			}
			t.Logf("Row count so far: %d (waiting for %d)", count, wantCount)
		}

		time.Sleep(2 * time.Second)
	}

	t.Fatalf("Timed out waiting for %d rows in table %s", wantCount, tableName)
}

func extractCount(t *testing.T, row query.Row) int64 {
	t.Helper()
	var result CountResult
	if err := row.ToStruct(&result); err != nil {
		t.Fatalf("failed to parse count result: %v", err)
	}
	return result.Count
}

// waitForOperationFailure polls until the operation reports a failure.
func waitForOperationFailure(t *testing.T, ctx context.Context, client *azkustoingestv2.QueuedIngestClient, op *azkustoingestv2.IngestionOperation) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)

	for time.Now().Before(deadline) {
		summary, err := client.GetOperationSummary(ctx, op)
		if err != nil {
			t.Logf("GetOperationSummary error (will retry): %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if summary.Failed > 0 {
			t.Logf("Operation failed as expected (failed=%d)", summary.Failed)
			return
		}
		if summary.Succeeded > 0 {
			t.Fatal("Expected operation to fail, but it succeeded")
		}

		time.Sleep(5 * time.Second)
	}

	t.Fatal("Timed out waiting for operation failure")
}
