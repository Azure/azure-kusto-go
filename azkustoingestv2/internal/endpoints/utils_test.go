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
