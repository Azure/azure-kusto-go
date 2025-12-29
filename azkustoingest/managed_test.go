package azkustoingest

import (
	"bytes"
	"context"
	goErrors "errors"
	"fmt"
	"io"
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
					db:         "defaultDb",
					table:      "defaultTable",
					client:     mockClient,
					streamConn: streamIngestor,
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
