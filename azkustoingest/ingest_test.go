package azkustoingest

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustoingest/internal/gzip"
	"github.com/Azure/azure-kusto-go/azkustoingest/internal/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type compressionTransport struct {
	mu        sync.Mutex
	blocks    map[string][]byte
	uploads   [][]byte
	streaming [][]byte
}

func (c *compressionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		defer req.Body.Close()
		var err error
		body, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	status := http.StatusOK
	response := ""
	switch {
	case req.URL.Path == "/v1/rest/auth/metadata":
	case req.URL.Path == "/v1/rest/mgmt":
		var command struct {
			CSL string `json:"csl"`
		}
		if err := json.Unmarshal(body, &command); err != nil {
			return nil, err
		}
		switch command.CSL {
		case ".get ingestion resources":
			response = `{"Tables":[{"TableName":"Table","Columns":[{"ColumnName":"ResourceTypeName","ColumnType":"string"},{"ColumnName":"StorageRoot","ColumnType":"string"}],"Rows":[["TempStorage","https://compression.blob.core.windows.net/container"],["SecuredReadyForAggregationQueue","https://compression.queue.core.windows.net/queue"]]}]}`
		case ".get kusto identity token":
			response = `{"Tables":[{"TableName":"Table","Columns":[{"ColumnName":"AuthorizationContext","ColumnType":"string"}],"Rows":[["test-auth-context"]]}]}`
		default:
			return nil, fmt.Errorf("unexpected management command: %s", command.CSL)
		}
	case strings.HasPrefix(req.URL.Path, "/v1/rest/ingest/"):
		c.streaming = append(c.streaming, body)
		status = http.StatusServiceUnavailable
		response = `{"error":{"code":"ServiceUnavailable","message":"retryable streaming failure"}}`
	case req.URL.Host == "compression.blob.core.windows.net" && req.Method == http.MethodPut:
		status = http.StatusCreated
		switch req.URL.Query().Get("comp") {
		case "block":
			if c.blocks == nil {
				c.blocks = make(map[string][]byte)
			}
			c.blocks[req.URL.Query().Get("blockid")] = body
		case "blocklist":
			var list struct {
				Latest []string `xml:"Latest"`
			}
			if err := xml.Unmarshal(body, &list); err != nil {
				return nil, err
			}
			if len(list.Latest) == 0 {
				return nil, fmt.Errorf("upload committed an empty block list")
			}
			var uploaded []byte
			for _, id := range list.Latest {
				block, ok := c.blocks[id]
				if !ok {
					return nil, fmt.Errorf("upload committed unknown block %q", id)
				}
				uploaded = append(uploaded, block...)
			}
			c.uploads = append(c.uploads, uploaded)
		case "":
			c.uploads = append(c.uploads, body)
		default:
			return nil, fmt.Errorf("unexpected blob operation: %s", req.URL)
		}
	case req.URL.Host == "compression.queue.core.windows.net" && req.Method == http.MethodPost:
		status = http.StatusCreated
		response = "<QueueMessagesList/>"
	default:
		return nil, fmt.Errorf("unexpected request: %s %s", req.Method, req.URL)
	}

	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(response)),
		Request:    req,
	}, nil
}

func compressionTestPayload() []byte {
	var data bytes.Buffer
	for i := 0; i < 1024; i++ {
		fmt.Fprintf(&data, "%d,gzip ingestion payload,%d\n", i, i%17)
	}
	return data.Bytes()
}

func TestQueuedCompressionProviders(t *testing.T) {
	t.Parallel()

	payload := compressionTestPayload()
	standard, err := io.ReadAll(gzip.Compress(bytes.NewReader(payload), true))
	require.NoError(t, err)
	alternative, err := io.ReadAll(gzip.Compress(bytes.NewReader(payload), false))
	require.NoError(t, err)
	require.NotEqual(t, standard, alternative, "payload must distinguish the compression providers")

	for _, provider := range []struct {
		name     string
		options  []Option
		expected []byte
	}{
		{"default", nil, standard},
		{"alternative", []Option{WithStandardLibraryCompression(false)}, alternative},
	} {
		for _, fromFile := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fromFile=%t", provider.name, fromFile), func(t *testing.T) {
				t.Parallel()

				transport := &compressionTransport{}
				options := []Option{
					WithDefaultDatabase("db"), WithDefaultTable("table"),
					WithHttpClient(&http.Client{Transport: transport}),
				}
				options = append(options, provider.options...)
				client, err := New(azkustodata.NewConnectionStringBuilder("https://compression.kusto.windows.net").
					WithApplicationToken("app", "test-token"), options...)
				require.NoError(t, err)
				defer client.Close()

				if fromFile {
					path := filepath.Join(t.TempDir(), "input.csv")
					require.NoError(t, os.WriteFile(path, payload, 0600))
					_, err = client.FromFile(context.Background(), path, FileFormat(CSV))
				} else {
					_, err = client.FromReader(context.Background(), bytes.NewReader(payload), FileFormat(CSV))
				}
				require.NoError(t, err)

				transport.mu.Lock()
				defer transport.mu.Unlock()
				require.Len(t, transport.uploads, 1)
				require.Equal(t, provider.expected, transport.uploads[0])
			})
		}
	}
}

func TestIngestion(t *testing.T) {

	firstMockClient := mockClient{
		endpoint: "https://test.kusto.windows.net",
		auth:     azkustodata.Authorization{},
	}
	mockClientSame := mockClient{
		endpoint: "https://test.kusto.windows.net",
		auth:     azkustodata.Authorization{},
	}
	secondMockClient := mockClient{
		endpoint: "https://test2.kusto.windows.net",
		auth:     azkustodata.Authorization{},
	}

	tests := []struct {
		name    string
		clients []func() *Ingestion
	}{
		{
			name: "TestSameClient",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{db: "test2", table: "test2"})
					return ingestion
				},
			},
		},
		{
			name: "TestSameEndpoint",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(mockClientSame, &Ingestion{db: "test2", table: "test2"})
					return ingestion
				},
			},
		},
		{
			name: "TestDifferentEndpoint",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(secondMockClient, &Ingestion{db: "test2", table: "test2"})
					return ingestion
				},
			},
		},
		{
			name: "TestDifferentAndSameEndpoints",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(mockClientSame, &Ingestion{db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(secondMockClient, &Ingestion{db: "test2", table: "test2"})
					return ingestion
				},
			},
		},
	}

	for _, test := range tests {
		test := test // Capture
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mgrMap := make(map[*resources.Manager]bool)
			for _, client := range test.clients {
				mgr := client().mgr
				mgrMap[mgr] = true
			}

			assert.Equalf(t, len(mgrMap), len(test.clients), "Got duplicated managers, want %d managers, got %d", len(test.clients), len(mgrMap))
		})
	}
}
