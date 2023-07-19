package azkustoingest

import (
	"testing"
)

func TestIsReservedHostname(t *testing.T) {
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
		t.Run(tc.name, func(t *testing.T) {
			if output := isReservedHostname(tc.input); output != tc.expected {
				t.Fatalf("Expected %v, but got %v", tc.expected, output)
			}
		})
	}
}

func TestRemoveIngestPrefix(t *testing.T) {
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
		t.Run(tc.name, func(t *testing.T) {
			if output := removeIngestPrefix(tc.input); output != tc.expected {
				t.Fatalf("Expected %v, but got %v", tc.expected, output)
			}
		})
	}
}

func TestAddIngestPrefix(t *testing.T) {
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
		t.Run(tc.name, func(t *testing.T) {
			if output := addIngestPrefix(tc.input); output != tc.expected {
				t.Fatalf("Expected %v, but got %v", tc.expected, output)
			}
		})
	}
}
