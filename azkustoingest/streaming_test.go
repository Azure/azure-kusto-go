package azkustoingest

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustoingest/internal/gzip"
	"github.com/Azure/azure-kusto-go/azkustoingest/internal/properties"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type streamIngestFunc func(ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string, clientRequestId string, isBlobUri bool) error
type testStreamIngestFunc func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string,
	clientRequestId string, isBlobUri bool) error

type fakeStreamIngestor struct {
	onStreamIngest streamIngestFunc
}

func (f fakeStreamIngestor) Close() error {
	return nil
}

func (f fakeStreamIngestor) StreamIngest(ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string, clientRequestId string, isBlobUri bool) error {
	return f.onStreamIngest(ctx, db, table, payload, format, mappingName, clientRequestId, isBlobUri)
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

func TestIngestionCompressionProviders(t *testing.T) {
	t.Parallel()

	payload := []byte(strings.Repeat("gzip ingestion payload,12345\n", 1024))
	providers := []struct {
		name        string
		options     []Option
		useStandard bool
	}{
		{"default", nil, true},
		{"standard library", []Option{WithStandardLibraryCompression(true)}, true},
		{"alternative", []Option{WithStandardLibraryCompression(false)}, false},
	}
	for _, provider := range providers {
		for _, managed := range []bool{false, true} {
			for _, dontCompress := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/managed=%t/dontCompress=%t", provider.name, managed, dontCompress), func(t *testing.T) {
					t.Parallel()

					options := []Option{WithDefaultDatabase("db"), WithDefaultTable("table")}
					options = append(options, provider.options...)
					expected := payload
					if !dontCompress {
						var err error
						expected, err = io.ReadAll(gzip.Compress(bytes.NewReader(payload), provider.useStandard))
						require.NoError(t, err)
					}

					mock := mockClient{endpoint: "https://test.kusto.windows.net"}
					streaming, err := newStreamingFromClient(mock, getOptions(options))
					require.NoError(t, err)
					var calls int
					streaming.streamConn = fakeStreamIngestor{
						onStreamIngest: func(_ context.Context, _, _ string, body io.Reader, _ azkustodata.DataFormatForStreaming, _, _ string, isBlobUri bool) error {
							calls++
							require.False(t, isBlobUri)
							got, err := io.ReadAll(body)
							require.NoError(t, err)
							require.Equal(t, expected, got)
							return nil
						},
					}

					var client Ingestor = streaming
					if managed {
						ingestion, err := newFromClient(mock, getOptions(options))
						require.NoError(t, err)
						client = newManagedFromClients(ingestion, streaming)
					}
					defer client.Close()

					var fileOptions []FileOption
					if dontCompress {
						fileOptions = append(fileOptions, DontCompress())
					}
					_, err = client.FromReader(context.Background(), bytes.NewReader(payload), fileOptions...)
					require.NoError(t, err)

					path := filepath.Join(t.TempDir(), "input.csv")
					require.NoError(t, os.WriteFile(path, payload, 0600))
					_, err = client.FromFile(context.Background(), path, fileOptions...)
					require.NoError(t, err)

					expectedCalls := 2
					if !dontCompress {
						compressedPath := path + ".gz"
						require.NoError(t, os.WriteFile(compressedPath, expected, 0600))
						_, err = client.FromFile(context.Background(), compressedPath)
						require.NoError(t, err)
						expectedCalls++
					}
					require.Equal(t, expectedCalls, calls)
				})
			}
		}
	}
}

func TestStreaming(t *testing.T) {
	t.Parallel()

	mockClient := mockClient{
		endpoint: "https://test.kusto.windows.net",
		auth:     azkustodata.Authorization{},
	}
	ctx := context.Background()

	filePath, reader := csvFileAndReader()
	internalData, err := io.ReadAll(reader)

	require.NoError(t, err)

	compressedBuffer := gzip.New()
	compressedBuffer.Reset(io.NopCloser(bytes.NewReader(internalData)))
	compressedBytes, err := io.ReadAll(compressedBuffer)
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
				assert.Equal(t, "KGC.executeStreaming", parts[0])
				_, err = uuid.Parse(parts[1])
				return err
			},
		},
		{
			name: "TestStreamingWithFormat",
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
				assert.Equal(t, "KGC.executeStreaming", parts[0])
				_, err = uuid.Parse(parts[1])
				return err
			},
		},
		{
			name: "TestWithMappingAndClientRequestId",
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
		},
		{
			name:    "TestStreamFailure",
			options: []FileOption{},
			onStreamIngest: func(t *testing.T, ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string,
				clientRequestId string, isBlobUri bool) error {
				return errors.E(errors.OpIngestStream, errors.KHTTPError, fmt.Errorf("error"))
			},
			expectedError: errors.E(errors.OpIngestStream, errors.KHTTPError, fmt.Errorf("error")),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			streamIngestor := fakeStreamIngestor{
				onStreamIngest: func(ctx context.Context, db, table string, payload io.Reader, format azkustodata.DataFormatForStreaming, mappingName string, clientRequestId string, isBlobUri bool) error {
					return test.onStreamIngest(t, ctx, db, table, payload, format, mappingName, clientRequestId, isBlobUri)
				},
			}

			streaming := Streaming{
				useStandardLibraryCompression: true,
				db:                            "defaultDb",
				table:                         "defaultTable",
				client:                        mockClient,
				streamConn:                    streamIngestor,
			}

			result, err := streaming.FromFile(ctx, filePath, test.options...)
			if test.expectedError != nil {
				assert.Equal(t, test.expectedError, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, result.record.Status, Succeeded)
			}

			result, err = streaming.FromReader(ctx, bytes.NewReader(internalData), test.options...)
			if test.expectedError != nil {
				assert.Equal(t, test.expectedError, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, result.record.Status, Succeeded)
			}

		})
	}

}
