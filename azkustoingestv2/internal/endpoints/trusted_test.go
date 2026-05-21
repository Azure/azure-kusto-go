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
