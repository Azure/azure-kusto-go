// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package endpoints

import (
	"testing"
)

func TestGetIngestionEndpoint(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"https://mycluster.kusto.windows.net", "https://ingest-mycluster.kusto.windows.net"},
		{"https://ingest-mycluster.kusto.windows.net", "https://ingest-mycluster.kusto.windows.net"},
		{"https://localhost:8080", "https://localhost:8080"},
		{"https://127.0.0.1:8080", "https://127.0.0.1:8080"},
		{"https://[::1]:8080", "https://[::1]:8080"},
		{"https://[fe80::1%25eth0]:8080", "https://[fe80::1%25eth0]:8080"},
		{"https://onebox.dev.kusto.windows.net", "https://onebox.dev.kusto.windows.net"},
		{"https://mycluster.kusto.windows.net:8443/path%2Fpart?name=ingest-value#fragment", "https://ingest-mycluster.kusto.windows.net:8443/path%2Fpart?name=ingest-value#fragment"},
		{"https://my-ingest-cluster.kusto.windows.net/ingest-data", "https://ingest-my-ingest-cluster.kusto.windows.net/ingest-data"},
		{"https://INGEST-mycluster.kusto.windows.net", "https://INGEST-mycluster.kusto.windows.net"},
		{"https://my-localhost.kusto.windows.net", "https://ingest-my-localhost.kusto.windows.net"},
		{"not-a-url", "not-a-url"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := GetIngestionEndpoint(tt.input)
			if result != tt.expected {
				t.Errorf("GetIngestionEndpoint(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestGetQueryEndpoint(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"https://ingest-mycluster.kusto.windows.net", "https://mycluster.kusto.windows.net"},
		{"https://mycluster.kusto.windows.net", "https://mycluster.kusto.windows.net"},
		{"https://localhost:8080", "https://localhost:8080"},
		{"https://127.0.0.1:8080", "https://127.0.0.1:8080"},
		{"https://[::1]:8080", "https://[::1]:8080"},
		{"https://[fe80::1%25eth0]:8080", "https://[fe80::1%25eth0]:8080"},
		{"https://onebox.dev.kusto.windows.net", "https://onebox.dev.kusto.windows.net"},
		{"https://INGEST-mycluster.kusto.windows.net:8443/path%2Fpart?name=ingest-value#fragment", "https://mycluster.kusto.windows.net:8443/path%2Fpart?name=ingest-value#fragment"},
		{"https://mycluster.kusto.windows.net/ingest-data?name=ingest-value", "https://mycluster.kusto.windows.net/ingest-data?name=ingest-value"},
		{"https://my-ingest-cluster.kusto.windows.net", "https://my-ingest-cluster.kusto.windows.net"},
		{"not-a-url", "not-a-url"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := GetQueryEndpoint(tt.input)
			if result != tt.expected {
				t.Errorf("GetQueryEndpoint(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestIsReservedHostname(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"https://localhost:8080", true},
		{"https://127.0.0.1:8080", true},
		{"https://192.168.1.1", true},
		{"https://[::1]", true},
		{"https://onebox.dev.kusto.windows.net", true},
		{"https://mycluster.kusto.windows.net", false},
		{"not-a-url", true}, // non-absolute URI
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := IsReservedHostname(tt.input)
			if result != tt.expected {
				t.Errorf("IsReservedHostname(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}
