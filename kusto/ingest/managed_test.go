package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/gzip"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/resources"
	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testMgmtFunc func(t *testing.T, ctx context.Context, db string, query kusto.Stmt, options ...kusto.MgmtOption) (*kusto.RowIterator, error)

func failIfQueuedCalled(t *testing.T, _ context.Context, _ string, query kusto.Stmt, _ ...kusto.MgmtOption) (*kusto.RowIterator, error) {
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

	filePath, reader := fileAndReaderFromString()
	data, err := ioutil.ReadAll(reader)

	require.NoError(t, err)

	compressedBuffer := gzip.New()
	compressedBuffer.Reset(io.NopCloser(bytes.NewReader(data)))
	compressedBytes, err := ioutil.ReadAll(compressedBuffer)
	require.NoError(t, err)

	seek, err := reader.Seek(0, io.SeekStart)
	require.Equal(t, int64(0), seek)
	require.NoError(t, err)

	counter := 0

	tests := []struct {
		name            string
		options         []FileOption
		onStreamIngest  testStreamIngestFunc
		onMgmt          testMgmtFunc
		expectedError   error
		expectedCounter int
	}{
		{
			name:    "TestManagedStreamingDefault",
			options: []FileOption{},
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format properties.DataFormat, mappingName string,
				clientRequestId string) error {
				assert.Equal(t, "defaultDb", db)
				assert.Equal(t, "defaultTable", table)
				payloadBytes, err := ioutil.ReadAll(payload)
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
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format properties.DataFormat, mappingName string,
				clientRequestId string) error {
				assert.Equal(t, "otherDb", db)
				assert.Equal(t, "otherTable", table)
				payloadBytes, err := ioutil.ReadAll(payload)
				assert.NoError(t, err)
				assert.Equal(t, compressedBytes, payloadBytes)
				assert.Equal(t, properties.CSV, format)
				assert.Equal(t, "", mappingName)
				parts := strings.Split(clientRequestId, ";")
				assert.Equal(t, "KGC.executeManagedStreamingIngest", parts[0])
				_, err = uuid.Parse(parts[1])
				return nil
			},
			onMgmt:          failIfQueuedCalled,
			expectedCounter: 1,
		},
		{
			name: "TestManagedStreamingWithFormat",
			options: []FileOption{
				FileFormat(properties.JSON),
			},
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format properties.DataFormat, mappingName string,
				clientRequestId string) error {
				assert.Equal(t, "defaultDb", db)
				assert.Equal(t, "defaultTable", table)
				payloadBytes, err := ioutil.ReadAll(payload)
				assert.NoError(t, err)
				assert.Equal(t, compressedBytes, payloadBytes)
				assert.Equal(t, properties.JSON, format)
				assert.Equal(t, "", mappingName)
				parts := strings.Split(clientRequestId, ";")
				assert.Equal(t, "KGC.executeManagedStreamingIngest", parts[0])
				_, err = uuid.Parse(parts[1])
				return nil
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
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format properties.DataFormat, mappingName string,
				clientRequestId string) error {
				assert.Equal(t, "defaultDb", db)
				assert.Equal(t, "defaultTable", table)
				payloadBytes, err := ioutil.ReadAll(payload)
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
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format properties.DataFormat, mappingName string,
				clientRequestId string) error {
				return errors.E(errors.OpIngestStream, errors.KHTTPError, fmt.Errorf("error")).SetNoRetry()
			},
			expectedError:   errors.E(errors.OpIngestStream, errors.KHTTPError, fmt.Errorf("error")).SetNoRetry(),
			onMgmt:          failIfQueuedCalled,
			expectedCounter: 1,
		},
		{
			name:    "TestSingleTransientError",
			options: []FileOption{},
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format properties.DataFormat, mappingName string,
				clientRequestId string) error {
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
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format properties.DataFormat, mappingName string,
				clientRequestId string) error {
				return errors.E(errors.OpIngestStream, errors.KHTTPError, fmt.Errorf("error"))
			},
			expectedError: errors.E(errors.OpFileIngest, errors.KBlobstore, fmt.Errorf("no Blob Storage container resources are defined, "+
				"there is no container to upload to")).SetNoRetry(),
			onMgmt: func(t *testing.T, ctx context.Context, db string, query kusto.Stmt, options ...kusto.MgmtOption) (*kusto.RowIterator, error) {
				// .get ingestion resources is always called in the ctor
				if query.String() == ".get ingestion resources" {
					return resources.SuccessfulFakeResources().Mgmt(ctx, db, query, options...)
				}
				if query.String() == ".get kusto identity token" {
					counter++
					return nil, nil
				}

				require.Fail(t, "Unexpected queued ingest call")
				return nil, nil
			},
			expectedCounter: 4,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			streamIngestor := fakeStreamIngestor{
				onStreamIngest: func(ctx context.Context, db, table string, payload io.Reader, format properties.DataFormat, mappingName string, clientRequestId string) error {
					err := test.onStreamIngest(t, ctx, db, table, payload, format, mappingName, clientRequestId)
					counter++
					return err
				},
			}
			mockClient := mockClient{
				endpoint: "https://test.kusto.windows.net",
				auth:     kusto.Authorization{},
				onMgmt: func(ctx context.Context, db string, query kusto.Stmt, options ...kusto.MgmtOption) (*kusto.RowIterator, error) {
					if test.onMgmt == nil {
						return nil, nil
					}
					return test.onMgmt(t, ctx, db, query, options...)
				},
			}

			ingestion, err := New(mockClient, "defaultDb", "defaultTable")
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
			test.options = append([]FileOption{BackOff(off)}, test.options...)

			counter = 0
			result, err := managed.FromFile(ctx, filePath, test.options...)
			if test.expectedError != nil {
				assert.Equal(t, test.expectedError, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, result.record.Status, StatusCode("Success"))
			}

			assert.Equal(t, test.expectedCounter, counter)

			counter = 0
			test.options = append([]FileOption{FileFormat(properties.CSV)}, test.options...)
			result, err = managed.FromReader(ctx, bytes.NewReader(data), test.options...)
			if test.expectedError != nil {
				assert.Equal(t, test.expectedError, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, result.record.Status, StatusCode("Success"))
			}
			assert.Equal(t, test.expectedCounter, counter)

		})
	}

}
