package azkustoingest

import (
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"time"
)

func TestIsReservedHostname(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		input    string
		expected bool
	}{
		{"Test IP Address", "192.168.1.1", true},
		{"Test Localhost", "localhost", true},
		{"Test Onebox", "onebox.dev.kusto.windows.net", true},
		{"Test Random String", "randomString", false},
		{"Test Localhost IP as String", "127.0.0.1", true},
		{"Test IP Address With HTTPS prefix", "https://192.168.1.1", true},
		{"Test Localhost With HTTPS prefix", "https://localhost", true},
		{"Test Onebox With HTTPS prefix", "https://onebox.dev.kusto.windows.net", true},
		{"Test Random String With HTTPS prefix", "https://randomString", false},
		{"Test Localhost IP as String with HTTPS prefix", "https://127.0.0.1", true},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if output := isReservedHostname(tc.input); output != tc.expected {
				t.Fatalf("Expected %v, but got %v", tc.expected, output)
			}
		})
	}
}

func TestRemoveIngestPrefix(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"Test reserved hostname", "localhost", "localhost"},
		{"Test with prefix", "ingest-randomString", "randomString"},
		{"Test without prefix", "randomString", "randomString"},
		{"Test with IP as Prefix", "192.168.1.1", "192.168.1.1"},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if output := removeIngestPrefix(tc.input); output != tc.expected {
				t.Fatalf("Expected %v, but got %v", tc.expected, output)
			}
		})
	}
}

func TestAddIngestPrefix(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"Test with prefix", "ingest-randomString", "ingest-randomString"},
		{"Test without prefix", "randomString", "ingest-randomString"},
		{"Test reserved hostname", "localhost", "localhost"},
		{"Test with Domain Prefix", "http://mywebsite", "http://ingest-mywebsite"},
		{"Test IP as String", "192.168.1.1", "192.168.1.1"},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if output := addIngestPrefix(tc.input); output != tc.expected {
				t.Fatalf("Expected %v, but got %v", tc.expected, output)
			}
		})
	}
}

func TestCtorOptions(t *testing.T) {
	type TestClient struct {
		name                              string
		clientType                        string
		endpoint, defaultDB, defaultTable string
		autoCorrectEndpoint               bool
		managedIngestEndpoint             string
		expectedEndpoint                  string
		expectedIngestEndpoint            string
	}
	testCases := []TestClient{
		{
			name:                "Queued client with autocorrect endpoint, database and table",
			clientType:          "Queued",
			endpoint:            "https://help.kusto.windows.net",
			defaultDB:           "someDb",
			defaultTable:        "defaultTable",
			autoCorrectEndpoint: true,
			expectedEndpoint:    "https://ingest-help.kusto.windows.net",
		},
		{
			name:                "Queued client without autocorrect endpoint, database and table",
			clientType:          "Queued",
			endpoint:            "https://help.kusto.windows.net",
			defaultDB:           "someDb",
			defaultTable:        "defaultTable",
			autoCorrectEndpoint: false,
			expectedEndpoint:    "https://help.kusto.windows.net",
		},
		{
			name:                "Queued client with autocorrect endpoint, no database and a table",
			clientType:          "Queued",
			endpoint:            "https://help.kusto.windows.net",
			defaultDB:           "",
			defaultTable:        "defaultTable",
			autoCorrectEndpoint: true,
			expectedEndpoint:    "https://ingest-help.kusto.windows.net",
		},
		{
			name:                "Streaming client with autocorrect endpoint, database and table",
			clientType:          "Streaming",
			endpoint:            "https://ingest-help.kusto.windows.net",
			defaultDB:           "someDb",
			defaultTable:        "defaultTable",
			autoCorrectEndpoint: true,
			expectedEndpoint:    "https://help.kusto.windows.net",
		},
		{
			name:                "Streaming client without autocorrect endpoint, database and table",
			clientType:          "Streaming",
			endpoint:            "https://ingest-help.kusto.windows.net",
			defaultDB:           "someDb",
			defaultTable:        "defaultTable",
			autoCorrectEndpoint: false,
			expectedEndpoint:    "https://ingest-help.kusto.windows.net",
		},
		{
			name:                "Streaming client with autocorrect endpoint, no database and a table",
			clientType:          "Streaming",
			endpoint:            "https://ingest-help.kusto.windows.net",
			defaultDB:           "",
			defaultTable:        "defaultTable",
			autoCorrectEndpoint: true,
			expectedEndpoint:    "https://help.kusto.windows.net",
		},
		{
			name:                   "Managed Streaming client with autocorrect endpoint, database and table",
			clientType:             "Managed",
			endpoint:               "https://ingest-help.kusto.windows.net",
			defaultDB:              "someDb",
			defaultTable:           "defaultTable",
			autoCorrectEndpoint:    true,
			expectedEndpoint:       "https://help.kusto.windows.net",
			expectedIngestEndpoint: "https://ingest-help.kusto.windows.net",
		},
		{
			name:                   "Managed Streaming client without autocorrect endpoint, database and table",
			clientType:             "Managed",
			endpoint:               "https://ingest-help.kusto.windows.net",
			defaultDB:              "someDb",
			defaultTable:           "defaultTable",
			autoCorrectEndpoint:    false,
			expectedEndpoint:       "https://ingest-help.kusto.windows.net",
			expectedIngestEndpoint: "https://ingest-help.kusto.windows.net",
		},
		{
			name:                   "Managed Streaming client with autocorrect endpoint, no database and a table",
			clientType:             "Managed",
			endpoint:               "https://ingest-help.kusto.windows.net",
			defaultDB:              "",
			defaultTable:           "defaultTable",
			autoCorrectEndpoint:    true,
			expectedEndpoint:       "https://help.kusto.windows.net",
			expectedIngestEndpoint: "https://ingest-help.kusto.windows.net",
		},
		{
			name:                   "Managed Streaming client with custom ingest endpoint",
			clientType:             "Managed",
			endpoint:               "https://help.kusto.windows.net",
			defaultDB:              "someDb",
			defaultTable:           "defaultTable",
			autoCorrectEndpoint:    false,
			managedIngestEndpoint:  "https://ingest-custom.kusto.windows.net",
			expectedEndpoint:       "https://help.kusto.windows.net",
			expectedIngestEndpoint: "https://ingest-custom.kusto.windows.net",
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			kcsb := azkustodata.NewConnectionStringBuilder(tt.endpoint)

			var options []Option
			if tt.defaultDB != "" {
				options = append(options, WithDefaultDatabase(tt.defaultDB))
			}
			if tt.defaultTable != "" {
				options = append(options, WithDefaultTable(tt.defaultTable))
			}
			if !tt.autoCorrectEndpoint {
				options = append(options, WithoutEndpointCorrection())
			}
			if tt.managedIngestEndpoint != "" {
				options = append(options, WithCustomIngestConnectionString(azkustodata.NewConnectionStringBuilder(tt.managedIngestEndpoint)))
			}

			var client interface{}
			var err error

			switch tt.clientType {
			case "Queued":
				client, err = New(kcsb, options...)
			case "Streaming":
				client, err = NewStreaming(kcsb, options...)
			case "Managed":
				client, err = NewManaged(kcsb, options...)
			}

			assert.NoError(t, err)
			assert.NotNil(t, client)

			var endpoint, db, table string
			switch tt.clientType {
			case "Queued":
				endpoint = client.(*Ingestion).client.Endpoint()
				db = client.(*Ingestion).db
				table = client.(*Ingestion).table
			case "Streaming":
				endpoint = client.(*Streaming).client.Endpoint()
				db = client.(*Streaming).db
				table = client.(*Streaming).table
			case "Managed":
				endpoint = client.(*Managed).streaming.client.Endpoint()
				db = client.(*Managed).queued.db
				table = client.(*Managed).streaming.table
				assert.Equal(t, db, client.(*Managed).queued.db)
				assert.Equal(t, table, client.(*Managed).queued.table)
			}

			assert.Equal(t, tt.expectedEndpoint, endpoint)

			if tt.clientType == "Managed" {
				assert.Equal(t, tt.expectedIngestEndpoint, client.(*Managed).queued.client.Endpoint())
			}

			assert.Equal(t, tt.defaultDB, db)
			assert.Equal(t, tt.defaultTable, table)
		})
	}
}

func TestWithHttpClient(t *testing.T) {
	t.Parallel()

	// Create a custom HTTP client with a specific timeout
	customClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Create a connection string builder
	kcsb := azkustodata.NewConnectionStringBuilder("https://help.kusto.windows.net")

	t.Run("Queued", func(t *testing.T) {
		// Create an ingestion client with the custom HTTP client
		ingest, err := New(kcsb, WithHttpClient(customClient))
		assert.NoError(t, err)
		assert.NotNil(t, ingest)

		// Verify that the underlying client is using our custom HTTP client
		// by checking if the timeout matches
		actualClient := ingest.HttpClient()
		assert.Equal(t, customClient.Timeout, actualClient.Timeout)

		// Cleanup
		ingest.Close()
	})

	t.Run("Streaming", func(t *testing.T) {
		// Create a streaming ingestion client with the custom HTTP client
		streaming, err := NewStreaming(kcsb, WithHttpClient(customClient))
		assert.NoError(t, err)
		assert.NotNil(t, streaming)

		// Verify that the underlying client is using our custom HTTP client
		actualClient := streaming.HttpClient()
		assert.Equal(t, customClient.Timeout, actualClient.Timeout)

		// Cleanup
		streaming.Close()
	})

	t.Run("Managed", func(t *testing.T) {
		// Create a managed ingestion client with the custom HTTP client
		managed, err := NewManaged(kcsb, WithHttpClient(customClient))
		assert.NoError(t, err)
		assert.NotNil(t, managed)

		// Verify that both queued and streaming clients are using our custom HTTP client
		queuedClient := managed.QueuedHttpClient()
		streamingClient := managed.StreamingHttpClient()
		assert.Equal(t, customClient.Timeout, queuedClient.Timeout)
		assert.Equal(t, customClient.Timeout, streamingClient.Timeout)

		// Cleanup
		managed.Close()
	})
}
