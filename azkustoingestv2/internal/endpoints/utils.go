// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package endpoints

import (
	"net"
	"net/url"
	"strconv"
	"strings"
)

const ingestPrefix = "ingest-"

// GetIngestionEndpoint converts a cluster URL to an ingestion endpoint URL
// by adding the "ingest-" prefix.
//
// Special URLs (localhost, IP addresses, onebox.dev.kusto.windows.net) are
// returned unchanged to support local development and testing scenarios.
func GetIngestionEndpoint(clusterURL string) string {
	return correctEndpoint(clusterURL, true)
}

// GetQueryEndpoint converts an ingestion endpoint URL to a query endpoint URL
// by removing the "ingest-" prefix.
func GetQueryEndpoint(clusterURL string) string {
	return correctEndpoint(clusterURL, false)
}

func correctEndpoint(clusterURL string, ingestion bool) string {
	u, err := url.Parse(clusterURL)
	if err != nil || IsReservedHostname(clusterURL) {
		return clusterURL
	}
	host := u.Hostname()
	hasPrefix := strings.HasPrefix(strings.ToLower(host), ingestPrefix)
	if hasPrefix == ingestion {
		return clusterURL
	}
	if ingestion {
		host = ingestPrefix + host
	} else {
		host = host[len(ingestPrefix):]
	}
	if port := u.Port(); port != "" {
		host = net.JoinHostPort(host, port)
	}
	u.Host = host
	return u.String()
}

// IsReservedHostname checks if the given URL points to a reserved hostname
// that should not have the "ingest-" prefix added.
//
// Reserved hostnames include:
//   - localhost
//   - IPv4 addresses
//   - IPv6 addresses
//   - onebox.dev.kusto.windows.net (development environment)
//   - Non-absolute URIs
func IsReservedHostname(rawURI string) bool {
	u, err := url.Parse(rawURI)
	if err != nil {
		return true
	}

	if !u.IsAbs() {
		return true
	}

	host := u.Hostname()
	if host == "" {
		return true
	}

	hostLower := strings.ToLower(host)

	// Check for IPv6 address (wrapped in brackets)
	if strings.HasPrefix(u.Host, "[") || net.ParseIP(host) != nil {
		return true
	}

	// Check for IPv4 address
	if isIPv4Address(host) {
		return true
	}

	// Check for localhost
	if hostLower == "localhost" || strings.HasSuffix(hostLower, ".localhost") {
		return true
	}

	// Check for onebox dev environment
	if hostLower == "onebox.dev.kusto.windows.net" {
		return true
	}

	return false
}

// isIPv4Address checks if the given string is a valid IPv4 address.
// It handles addresses with ports (e.g., "127.0.0.1:8080").
func isIPv4Address(address string) bool {
	// Remove port if present
	hostPart := address
	if idx := strings.LastIndex(address, ":"); idx != -1 {
		hostPart = address[:idx]
	}

	parts := strings.Split(hostPart, ".")
	if len(parts) != 4 {
		return false
	}

	for _, part := range parts {
		num, err := strconv.Atoi(part)
		if err != nil || num < 0 || num > 255 {
			return false
		}
	}
	return true
}
