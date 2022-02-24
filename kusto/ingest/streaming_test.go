package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/gzip"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type streamIngestFunc func(ctx context.Context, db, table string, payload io.Reader, format properties.DataFormat, mappingName string, clientRequestId string) error
type testStreamIngestFunc func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format properties.DataFormat, mappingName string,
	clientRequestId string) error

type fakeStreamIngestor struct {
	onStreamIngest streamIngestFunc
}

func (f fakeStreamIngestor) StreamIngest(ctx context.Context, db, table string, payload io.Reader, format properties.DataFormat, mappingName string, clientRequestId string) error {
	return f.onStreamIngest(ctx, db, table, payload, format, mappingName, clientRequestId)
}

func bigCsvFileAndReader() (string, *bytes.Reader) {
	return fileAndReaderFromString(`,,,,
	2020-03-10T20:59:30.694177Z,11196991-b193-4610-ae12-bcc03d092927,v0.0.1,` + strings.Repeat("Hello world!", 4*1024*1024) + `,Daniel Dubovski
	2020-03-10T20:59:30.694177Z,,v0.0.2,,`)
}

func csvFileAndReader() (string, *bytes.Reader) {
	return fileAndReaderFromString(`,,,,
	2020-03-10T20:59:30.694177Z,11196991-b193-4610-ae12-bcc03d092927,v0.0.1,Hello world!,Daniel Dubovski
	2020-03-10T20:59:30.694177Z,,v0.0.2,,`)
}

func fileAndReaderFromString(data string) (string, *bytes.Reader) {
	file, err := os.CreateTemp(".", "*.csv")
	if err != nil {
		panic(err)
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)

	writer := io.StringWriter(file)
	if _, err := writer.WriteString(data); err != nil {
		panic(err)
	}

	return file.Name(), bytes.NewReader([]byte(data))
}

func TestStreaming(t *testing.T) {
	t.Parallel()

	mockClient := mockClient{
		endpoint: "https://test.kusto.windows.net",
		auth:     kusto.Authorization{},
	}
	ctx := context.Background()

	filePath, reader := csvFileAndReader()
	data, err := ioutil.ReadAll(reader)

	require.NoError(t, err)

	compressedBuffer := gzip.New()
	compressedBuffer.Reset(io.NopCloser(bytes.NewReader(data)))
	compressedBytes, err := ioutil.ReadAll(compressedBuffer)
	require.NoError(t, err)

	seek, err := reader.Seek(0, io.SeekStart)
	require.Equal(t, int64(0), seek)
	require.NoError(t, err)

	tests := []struct {
		name           string
		options        []FileOption
		onStreamIngest testStreamIngestFunc
		expectedError  error
	}{
		{
			name:    "TestStreamingDefault",
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
				assert.Equal(t, "KGC.executeStreaming", parts[0])
				_, err = uuid.Parse(parts[1])
				assert.NoError(t, err)
				return nil
			},
		},
		{
			name: "TestStreamingWithDatabaseAndTable",
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
				assert.Equal(t, "KGC.executeStreaming", parts[0])
				_, err = uuid.Parse(parts[1])
				return nil
			},
		},
		{
			name: "TestStreamingWithFormat",
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
				assert.Equal(t, "KGC.executeStreaming", parts[0])
				_, err = uuid.Parse(parts[1])
				return nil
			},
		},
		{
			name: "TestWithMappingAndClientRequestId",
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
		},
		{
			name:    "TestStreamFailure",
			options: []FileOption{},
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format properties.DataFormat, mappingName string,
				clientRequestId string) error {
				return errors.E(errors.OpIngestStream, errors.KHTTPError, fmt.Errorf("error"))
			},
			expectedError: errors.E(errors.OpIngestStream, errors.KHTTPError, fmt.Errorf("error")),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			streamIngestor := fakeStreamIngestor{
				onStreamIngest: func(ctx context.Context, db, table string, payload io.Reader, format properties.DataFormat, mappingName string, clientRequestId string) error {
					return test.onStreamIngest(t, ctx, db, table, payload, format, mappingName, clientRequestId)
				},
			}

			streaming := Streaming{
				db:         "defaultDb",
				table:      "defaultTable",
				client:     mockClient,
				streamConn: streamIngestor,
			}

			result, err := streaming.FromFile(ctx, filePath, test.options...)
			if test.expectedError != nil {
				assert.Equal(t, test.expectedError, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, result.record.Status, StatusCode("Success"))
			}

			test.options = append([]FileOption{FileFormat(properties.CSV)}, test.options...)
			result, err = streaming.FromReader(ctx, bytes.NewReader(data), test.options...)
			if test.expectedError != nil {
				assert.Equal(t, test.expectedError, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, result.record.Status, StatusCode("Success"))
			}

		})
	}

}
