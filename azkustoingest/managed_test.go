package azkustoingest

import (
	"bytes"
	standardgzip "compress/gzip"
	"context"
	"encoding/base64"
	goErrors "errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	v1 "github.com/Azure/azure-kusto-go/azkustodata/query/v1"
	"github.com/Azure/azure-kusto-go/azkustoingest/internal/gzip"
	"github.com/Azure/azure-kusto-go/azkustoingest/internal/properties"
	"github.com/Azure/azure-kusto-go/azkustoingest/internal/resources"
	"github.com/cenkalti/backoff/v4"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testMgmtFunc func(t *testing.T, ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (v1.Dataset, error)

func failIfQueuedCalled(t *testing.T, _ context.Context, _ string, query azkustodata.Statement, _ ...azkustodata.QueryOption) (v1.Dataset, error) {
	// .get ingestion resources is always called in the ctor
	if query.String() == ".get ingestion resources" {
		return nil, nil
	}
	require.Fail(t, "Queued ingest should not be called")
	return nil, nil
}

func TestManagedAlternativeCompressionFallback(t *testing.T) {
	t.Parallel()

	random := make([]byte, 5*mb)
	_, err := rand.New(rand.NewSource(1)).Read(random)
	require.NoError(t, err)
	largePayload := []byte(base64.StdEncoding.EncodeToString(random))

	for _, scenario := range []struct {
		name             string
		payload          []byte
		expectedAttempts int
	}{
		{"compressed output exceeds limit", largePayload, 0},
		{"streaming retries exhausted", compressionTestPayload(), retryCount + 1},
	} {
		expected, err := io.ReadAll(gzip.Compress(bytes.NewReader(scenario.payload), false))
		require.NoError(t, err)
		standard, err := io.ReadAll(gzip.Compress(bytes.NewReader(scenario.payload), true))
		require.NoError(t, err)
		require.False(t, bytes.Equal(standard, expected), "payload must distinguish the compression providers")
		if scenario.expectedAttempts == 0 {
			require.Greater(t, int64(len(expected)), maxStreamingSize+1)
		} else {
			require.LessOrEqual(t, int64(len(expected)), maxStreamingSize)
		}

		for _, fromFile := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fromFile=%t", scenario.name, fromFile), func(t *testing.T) {
				transport := &compressionTransport{}
				client, err := NewManaged(azkustodata.NewConnectionStringBuilder("https://compression.kusto.windows.net").
					WithApplicationToken("app", "test-token"),
					WithDefaultDatabase("db"), WithDefaultTable("table"),
					WithHttpClient(&http.Client{Transport: transport}),
					WithStandardLibraryCompression(false))
				require.NoError(t, err)
				defer client.Close()

				backoffPolicy := backoff.NewExponentialBackOff()
				backoffPolicy.InitialInterval = time.Millisecond
				options := []FileOption{FileFormat(CSV), backOff(backoffPolicy)}
				var result *Result
				if fromFile {
					path := filepath.Join(t.TempDir(), "input.csv")
					require.NoError(t, os.WriteFile(path, scenario.payload, 0600))
					result, err = client.FromFile(context.Background(), path, options...)
				} else {
					result, err = client.FromReader(context.Background(), bytes.NewReader(scenario.payload), options...)
				}
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, Queued, result.record.Status)

				transport.mu.Lock()
				defer transport.mu.Unlock()
				require.Len(t, transport.streaming, scenario.expectedAttempts)
				for _, body := range transport.streaming {
					require.Equal(t, expected, body, "each streaming retry must replay the complete gzip stream")
				}
				require.Len(t, transport.uploads, 1)
				require.True(t, bytes.Equal(expected, transport.uploads[0]),
					"queued fallback must preserve the complete gzip stream without recompression (expected %d bytes, got %d)",
					len(expected), len(transport.uploads[0]))

				reader, err := standardgzip.NewReader(bytes.NewReader(transport.uploads[0]))
				require.NoError(t, err)
				defer reader.Close()
				actual, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.Equal(t, scenario.payload, actual)
			})
		}
	}
}

func TestManaged(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	filePath, reader := csvFileAndReader()
	initialData, compressedBytes := initFile(t, reader)

	bigFilePath, bigReader := bigCsvFileAndReader()
	bigData, _ := initFile(t, bigReader)
	counter := 0

	someBlobPath := "https://some-blob.blob.core.windows.net/some-container/some-blob;Managed_Identity="

	tests := []struct {
		name            string
		options         []FileOption
		onStreamIngest  testStreamIngestFunc
		onMgmt          testMgmtFunc
		expectedStatus  StatusCode
		expectedError   error
		expectedCounter int
		onLocal         func(t *testing.T, ctx context.Context, from string, props properties.All) (string, int64, error)
		onReader        func(t *testing.T, ctx context.Context, reader io.Reader, props properties.All) (string, int64, error)
		onBlob          func(t *testing.T, ctx context.Context, from string, fileSize int64, props properties.All) error
		isBigFile       bool
		blobPath        string
	}{
		{
			name:    "TestManagedStreamingDefault",
			options: []FileOption{},
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string,
				clientRequestId string, isBlobUri bool) error {
				assert.Equal(t, "defaultDb", db)
				assert.Equal(t, "defaultTable", table)
				payloadBytes, err := io.ReadAll(payload)
				assert.NoError(t, err)
				assert.Equal(t, compressedBytes, payloadBytes)
				assert.Equal(t, properties.CSV, format)
				assert.Equal(t, "", mappingName)
				parts := strings.Split(clientRequestId, ";")
				assert.Equal(t, "KGC.executeManagedStreamingIngest", parts[0])
				_, err = uuid.Parse(parts[1])
				assert.NoError(t, err)
				return nil
			},
			onMgmt:          failIfQueuedCalled,
			expectedCounter: 1,
		},
		{
			name: "TestManagedStreamingWithDatabaseAndTable",
			options: []FileOption{
				Database("otherDb"),
				Table("otherTable"),
			},
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string,
				clientRequestId string, isBlobUri bool) error {
				assert.Equal(t, "otherDb", db)
				assert.Equal(t, "otherTable", table)
				payloadBytes, err := io.ReadAll(payload)
				assert.NoError(t, err)
				assert.Equal(t, compressedBytes, payloadBytes)
				assert.Equal(t, properties.CSV, format)
				assert.Equal(t, "", mappingName)
				parts := strings.Split(clientRequestId, ";")
				assert.Equal(t, "KGC.executeManagedStreamingIngest", parts[0])
				_, err = uuid.Parse(parts[1])
				return err
			},
			onMgmt:          failIfQueuedCalled,
			expectedCounter: 1,
		},
		{
			name: "TestManagedStreamingWithFormat",
			options: []FileOption{
				FileFormat(properties.JSON),
			},
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string,
				clientRequestId string, isBlobUri bool) error {
				assert.Equal(t, "defaultDb", db)
				assert.Equal(t, "defaultTable", table)
				payloadBytes, err := io.ReadAll(payload)
				assert.NoError(t, err)
				assert.Equal(t, compressedBytes, payloadBytes)
				assert.Equal(t, properties.JSON, format)
				assert.Equal(t, "", mappingName)
				parts := strings.Split(clientRequestId, ";")
				assert.Equal(t, "KGC.executeManagedStreamingIngest", parts[0])
				_, err = uuid.Parse(parts[1])
				return err
			},
			onMgmt:          failIfQueuedCalled,
			expectedCounter: 1,
		},
		{
			name: "TestManagedWithMappingAndClientRequestId",
			options: []FileOption{
				IngestionMappingRef("mapping", properties.CSV),
				ClientRequestId("clientRequestId"),
			},
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string,
				clientRequestId string, isBlobUri bool) error {
				assert.Equal(t, "defaultDb", db)
				assert.Equal(t, "defaultTable", table)
				payloadBytes, err := io.ReadAll(payload)
				assert.NoError(t, err)
				assert.Equal(t, compressedBytes, payloadBytes)
				assert.Equal(t, properties.CSV, format)
				assert.Equal(t, "mapping", mappingName)
				assert.Equal(t, "clientRequestId", clientRequestId)
				return nil
			},
			onMgmt:          failIfQueuedCalled,
			expectedCounter: 1,
		},
		{
			name:    "TestPermanentError",
			options: []FileOption{},
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string,
				clientRequestId string, isBlobUri bool) error {
				assert.Equal(t, "defaultDb", db)
				assert.Equal(t, "defaultTable", table)
				payloadBytes, err := io.ReadAll(payload)
				assert.NoError(t, err)
				assert.Equal(t, compressedBytes, payloadBytes)
				assert.Equal(t, properties.CSV, format)
				assert.Equal(t, "", mappingName)
				parts := strings.Split(clientRequestId, ";")
				assert.Equal(t, "KGC.executeManagedStreamingIngest", parts[0])
				_, err = uuid.Parse(parts[1])
				assert.NoError(t, err)
				return errors.E(errors.OpIngestStream, errors.KHTTPError, fmt.Errorf("error")).SetNoRetry()
			},
			expectedError:   errors.E(errors.OpIngestStream, errors.KHTTPError, fmt.Errorf("error")).SetNoRetry(),
			onMgmt:          failIfQueuedCalled,
			expectedCounter: 1,
		},
		{
			name:    "TestPermanentErrorNotKusto",
			options: []FileOption{},
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string,
				clientRequestId string, isBlobUri bool) error {
				assert.Equal(t, "defaultDb", db)
				assert.Equal(t, "defaultTable", table)
				payloadBytes, err := io.ReadAll(payload)
				assert.NoError(t, err)
				assert.Equal(t, compressedBytes, payloadBytes)
				assert.Equal(t, properties.CSV, format)
				assert.Equal(t, "", mappingName)
				parts := strings.Split(clientRequestId, ";")
				assert.Equal(t, "KGC.executeManagedStreamingIngest", parts[0])
				_, err = uuid.Parse(parts[1])
				assert.NoError(t, err)
				return goErrors.New("some error")
			},
			expectedError:   errors.E(errors.OpIngestStream, errors.KClientArgs, goErrors.New("some error")),
			onMgmt:          failIfQueuedCalled,
			expectedCounter: 1,
		},
		{
			name:    "TestSingleTransientError",
			options: []FileOption{},
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string,
				clientRequestId string, isBlobUri bool) error {
				assert.Equal(t, "defaultDb", db)
				assert.Equal(t, "defaultTable", table)
				payloadBytes, err := io.ReadAll(payload)
				assert.NoError(t, err)
				assert.Equal(t, compressedBytes, payloadBytes)
				assert.Equal(t, properties.CSV, format)
				assert.Equal(t, "", mappingName)
				parts := strings.Split(clientRequestId, ";")
				assert.Equal(t, "KGC.executeManagedStreamingIngest", parts[0])
				_, err = uuid.Parse(parts[1])
				assert.NoError(t, err)
				if counter == 0 {
					return errors.E(errors.OpIngestStream, errors.KHTTPError, fmt.Errorf("error"))
				}
				return nil
			},
			onMgmt:          failIfQueuedCalled,
			expectedCounter: 2,
		},
		{
			name:    "TestMultipleTransientErrors",
			options: []FileOption{},
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string,
				clientRequestId string, isBlobUri bool) error {
				assert.Equal(t, "defaultDb", db)
				assert.Equal(t, "defaultTable", table)
				payloadBytes, err := io.ReadAll(payload)
				assert.NoError(t, err)
				assert.Equal(t, compressedBytes, payloadBytes)
				assert.Equal(t, properties.CSV, format)
				assert.Equal(t, "", mappingName)
				parts := strings.Split(clientRequestId, ";")
				assert.Equal(t, "KGC.executeManagedStreamingIngest", parts[0])
				_, err = uuid.Parse(parts[1])
				assert.NoError(t, err)
				return errors.E(errors.OpIngestStream, errors.KHTTPError, fmt.Errorf("error"))
			},
			onMgmt: func(t *testing.T, ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (v1.Dataset, error) {
				// .get ingestion resources is always called in the ctor
				if query.String() == ".get ingestion resources" {
					return resources.SuccessfulFakeResources().Mgmt(ctx, db, query, options...)
				}
				if query.String() == ".get kusto identity token" {
					return nil, nil
				}

				require.Fail(t, "Unexpected queued ingest call")
				return nil, nil
			},
			onReader: func(t *testing.T, ctx context.Context, reader io.Reader, props properties.All) (string, int64, error) {
				counter++
				assert.Equal(t, "defaultDb", props.Ingestion.DatabaseName)
				assert.Equal(t, "defaultTable", props.Ingestion.TableName)
				all, err := io.ReadAll(reader)
				assert.NoError(t, err)
				assert.Equal(t, compressedBytes, all)
				return "", 0, nil
			},
			expectedCounter: 4,
			expectedStatus:  Queued,
		},
		{
			name:      "TestBigFile",
			options:   []FileOption{},
			isBigFile: true,
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string,
				clientRequestId string, isBlobUri bool) error {
				require.Fail(t, "Big file shouldn't try to stream")
				return errors.E(errors.OpIngestStream, errors.KHTTPError, fmt.Errorf("error"))
			},
			onMgmt: func(t *testing.T, ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (v1.Dataset, error) {
				// .get ingestion resources is always called in the ctor
				if query.String() == ".get ingestion resources" {
					return resources.SuccessfulFakeResources().Mgmt(ctx, db, query, options...)
				}
				if query.String() == ".get kusto identity token" {
					return nil, nil
				}

				require.Fail(t, "Unexpected queued ingest call")
				return nil, nil
			},
			onReader: func(t *testing.T, ctx context.Context, reader io.Reader, props properties.All) (string, int64, error) {
				counter++
				assert.Equal(t, "defaultDb", props.Ingestion.DatabaseName)
				assert.Equal(t, "defaultTable", props.Ingestion.TableName)
				all, err := io.ReadAll(reader)
				assert.NoError(t, err)
				assert.Equal(t, bigData, all)
				return "", 0, nil
			},
			expectedCounter: 1,
			expectedStatus:  Queued,
		},
		{
			name:     "TestBlob",
			blobPath: someBlobPath,
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string,
				clientRequestId string, isBlobUri bool) error {
				return errors.E(errors.OpIngestStream, errors.KHTTPError, fmt.Errorf("error"))
			},
			onMgmt: func(t *testing.T, ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (v1.Dataset, error) {
				// .get ingestion resources is always called in the ctor
				if query.String() == ".get ingestion resources" {
					return resources.SuccessfulFakeResources().Mgmt(ctx, db, query, options...)
				}
				if query.String() == ".get kusto identity token" {
					return nil, nil
				}

				require.Fail(t, "Unexpected queued ingest call")
				return nil, nil
			},
			onBlob: func(t *testing.T, ctx context.Context, from string, fileSize int64, props properties.All) error {
				counter++
				assert.Equal(t, "defaultDb", props.Ingestion.DatabaseName)
				assert.Equal(t, "defaultTable", props.Ingestion.TableName)
				assert.Equal(t, someBlobPath, from)
				return nil
			},
			expectedCounter: 1,
			expectedStatus:  Queued,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			streamIngestor := fakeStreamIngestor{
				onStreamIngest: func(ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string, clientRequestId string, isBlobUri bool) error {
					err := test.onStreamIngest(t, ctx, db, table, payload, format, mappingName, clientRequestId, isBlobUri)
					counter++
					return err
				},
			}
			mockClient := mockClient{
				endpoint: "https://test.kusto.windows.net",
				auth:     azkustodata.Authorization{},
				onMgmt: func(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (v1.Dataset, error) {
					if test.onMgmt == nil {
						return nil, nil
					}
					return test.onMgmt(t, ctx, db, query, options...)
				},
			}

			ingestion, err := newFromClient(mockClient, &Ingestion{db: "defaultDb", table: "defaultTable"})
			ingestion.fs = resources.FsMock{
				OnLocal: func(ctx context.Context, from string, props properties.All) (string, int64, error) {
					if test.onLocal == nil {
						return "", 0, nil
					}
					return test.onLocal(t, ctx, from, props)
				},
				OnReader: func(ctx context.Context, reader io.Reader, props properties.All) (string, int64, error) {
					if test.onReader == nil {
						return "", 0, nil
					}
					return test.onReader(t, ctx, reader, props)
				},
				OnBlob: func(ctx context.Context, from string, fileSize int64, props properties.All) error {
					if test.onBlob == nil {
						return nil
					}
					return test.onBlob(t, ctx, from, fileSize, props)
				},
			}
			require.NoError(t, err)
			managed := Managed{
				queued: ingestion,
				streaming: &Streaming{
					useStandardLibraryCompression: true,
					db:                            "defaultDb",
					table:                         "defaultTable",
					client:                        mockClient,
					streamConn:                    streamIngestor,
				},
			}

			off := backoff.NewExponentialBackOff()
			off.InitialInterval = time.Millisecond
			test.options = append([]FileOption{backOff(off)}, test.options...)

			counter = 0

			if test.blobPath != "" {
				result, err := managed.FromFile(ctx, test.blobPath, test.options...)
				assert.NoError(t, err)
				assert.Equal(t, result.record.Status, test.expectedStatus)
				return
			}

			var path string
			var fileData []byte
			if test.isBigFile {
				path = bigFilePath
				fileData = bigData
				test.options = append([]FileOption{DontCompress()}, test.options...)
			} else {
				path = filePath
				fileData = initialData
			}
			result, err := managed.FromFile(ctx, path, test.options...)
			if test.expectedError != nil {
				assert.Equal(t, test.expectedError, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				if test.expectedStatus == "" {
					test.expectedStatus = Succeeded
				}
				assert.Equal(t, result.record.Status, test.expectedStatus)
			}

			assert.Equal(t, test.expectedCounter, counter)

			counter = 0
			result, err = managed.FromReader(ctx, bytes.NewReader(fileData), test.options...)
			if test.expectedError != nil {
				assert.Equal(t, test.expectedError, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				if test.expectedStatus == "" {
					test.expectedStatus = Succeeded
				}
				assert.Equal(t, result.record.Status, test.expectedStatus)
			}
			assert.Equal(t, test.expectedCounter, counter)

		})
	}

}

func initFile(t *testing.T, reader *bytes.Reader) ([]byte, []byte) {
	data, err := io.ReadAll(reader)

	require.NoError(t, err)

	compressedBuffer := gzip.New()
	compressedBuffer.Reset(io.NopCloser(bytes.NewReader(data)))
	compressedBytes, err := io.ReadAll(compressedBuffer)
	require.NoError(t, err)

	seek, err := reader.Seek(0, io.SeekStart)
	require.Equal(t, int64(0), seek)
	require.NoError(t, err)
	return data, compressedBytes
}
