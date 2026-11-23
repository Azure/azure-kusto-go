// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package endpoints

import (
	"testing"
)

func TestFastSuffixMatcher(t *testing.T) {
	matcher := NewFastSuffixMatcher([]MatchRule{
		{Suffix: ".kusto.windows.net"},
		{Suffix: ".kusto.chinacloudapi.cn"},
	})

	tests := []struct {
		input    string
		expected bool
	}{
		{"mycluster.kusto.windows.net", true},
		{"mycluster.kusto.chinacloudapi.cn", true},
		{"mycluster.other.net", false},
		{"", false},
		{"kusto.windows.net", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := matcher.Matches(tt.input)
			if result != tt.expected {
				t.Errorf("FastSuffixMatcher.Matches(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestWellKnownKustoEndpoints(t *testing.T) {
	endpoints := DefaultKustoTrustedEndpoints()

	// Well-known endpoints should match
	if !endpoints.IsTrustedEndpoint("mycluster.kusto.windows.net") {
		t.Error("expected mycluster.kusto.windows.net to be well-known")
	}

	// Unknown endpoints should not match
	if endpoints.IsTrustedEndpoint("mycluster.unknown.net") {
		t.Error("expected mycluster.unknown.net to not be well-known")
	}
}

func TestKustoTrustedEndpointsValidateEndpoint(t *testing.T) {
	trusted := NewKustoTrustedEndpoints(nil)

	// Well-known endpoint should pass
	err := trusted.ValidateEndpoint("mycluster.kusto.windows.net", false)
	if err != nil {
		t.Errorf("unexpected error for well-known endpoint: %v", err)
	}

	// Localhost should be trusted
	err = trusted.ValidateEndpoint("localhost:8080", false)
	if err != nil {
		t.Errorf("unexpected error for localhost: %v", err)
	}
}

func TestKustoTrustedEndpointsDisabled(t *testing.T) {
	trusted := NewKustoTrustedEndpoints(nil)

	// With security checks skipped, any endpoint should pass
	err := trusted.ValidateEndpoint("unknown-cluster.random.com", true)
	if err != nil {
		t.Errorf("unexpected error when validation disabled: %v", err)
	}
}

func TestKustoTrustedEndpointsValidateEndpointURL(t *testing.T) {
	trusted := DefaultKustoTrustedEndpoints()
	for _, tc := range []struct {
		url     string
		skip    bool
		wantErr bool
	}{
		{"https://ingest-cluster.kusto.windows.net:443/v1/rest/ingestion", false, false},
		{"https://CLUSTER.KUSTO.WINDOWS.NET", false, false},
		{"http://localhost:8080", false, false},
		{"http://127.0.0.1:8080", false, false},
		{"http://[::1]:8080", false, false},
		{"https://custom.example", false, true},
		{"https://custom.example", true, false},
		{"https://localhost.example", false, true},
		{"https://notlocalhost.example", false, true},
		{"https://cluster.kusto.windows.net.example", false, true},
		{"https://cluster.kusto.windows.net@untrusted.example", false, true},
		{"https://user@cluster.kusto.windows.net", false, true},
		{"cluster.kusto.windows.net", false, true},
		{"https://", false, true},
		{"https://%", false, true},
		{"file:///cluster.kusto.windows.net", false, true},
		{"file:///custom.example", true, true},
	} {
		t.Run(tc.url, func(t *testing.T) {
			err := trusted.ValidateEndpointURL(tc.url, tc.skip)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateEndpointURL(%q, %v) = %v, want error %v", tc.url, tc.skip, err, tc.wantErr)
			}
		})
	}
}
