// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package endpoints

import (
	"strings"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

// MatchRule represents a matching rule for endpoint validation.
type MatchRule struct {
	// Suffix is the suffix or hostname to match.
	Suffix string
	// Exact indicates whether an exact match is required (true) or suffix match (false).
	Exact bool
}

// FastSuffixMatcher provides efficient suffix matching for endpoint validation.
// It organizes rules by their top-level domain for faster lookup.
type FastSuffixMatcher struct {
	// rules maps TLD to a list of match rules
	rules map[string][]MatchRule
}

// NewFastSuffixMatcher creates a new FastSuffixMatcher from the given rules.
func NewFastSuffixMatcher(rules []MatchRule) *FastSuffixMatcher {
	m := &FastSuffixMatcher{
		rules: make(map[string][]MatchRule),
	}
	for _, rule := range rules {
		tld := extractTLD(rule.Suffix)
		m.rules[tld] = append(m.rules[tld], rule)
	}
	return m
}

// Matches checks if the given hostname matches any of the rules.
func (m *FastSuffixMatcher) Matches(hostname string) bool {
	hostname = strings.ToLower(hostname)
	tld := extractTLD(hostname)

	rules, ok := m.rules[tld]
	if !ok {
		return false
	}

	for _, rule := range rules {
		suffix := strings.ToLower(rule.Suffix)
		if rule.Exact {
			if hostname == suffix {
				return true
			}
		} else {
			if strings.HasSuffix(hostname, suffix) {
				return true
			}
		}
	}
	return false
}

// extractTLD extracts the top-level domain from a hostname.
func extractTLD(hostname string) string {
	parts := strings.Split(hostname, ".")
	if len(parts) == 0 {
		return hostname
	}
	return parts[len(parts)-1]
}

// WellKnownKustoEndpoints contains the well-known Kusto endpoint suffixes.
var WellKnownKustoEndpoints = []MatchRule{
	{Suffix: ".kusto.windows.net", Exact: false},
	{Suffix: ".kustomfa.windows.net", Exact: false},
	{Suffix: ".kusto.chinacloudapi.cn", Exact: false},
	{Suffix: ".kusto.cloudapi.de", Exact: false},
	{Suffix: ".kusto.usgovcloudapi.net", Exact: false},
	{Suffix: ".kustomfa.chinacloudapi.cn", Exact: false},
	{Suffix: ".kustomfa.cloudapi.de", Exact: false},
	{Suffix: ".kustomfa.usgovcloudapi.net", Exact: false},
	{Suffix: ".kustodev.windows.net", Exact: false},
	{Suffix: ".kustodev.chinacloudapi.cn", Exact: false},
	{Suffix: ".kustodev.cloudapi.de", Exact: false},
	{Suffix: ".kustodev.usgovcloudapi.net", Exact: false},
	{Suffix: ".aria.microsoft.com", Exact: false},
	{Suffix: ".playfab.com", Exact: false},
	// Synapse endpoints
	{Suffix: ".synapse.azure.net", Exact: false},
	{Suffix: ".synapse.azure.cn", Exact: false},
	{Suffix: ".synapse.azure.de", Exact: false},
	{Suffix: ".synapse.usgovcloudapi.net", Exact: false},
}

// KustoTrustedEndpoints validates that a Kusto endpoint is trusted.
type KustoTrustedEndpoints struct {
	matcher *FastSuffixMatcher
	additionalRules []MatchRule
}

// DefaultKustoTrustedEndpoints creates a KustoTrustedEndpoints with the well-known endpoints.
func DefaultKustoTrustedEndpoints() *KustoTrustedEndpoints {
	return &KustoTrustedEndpoints{
		matcher: NewFastSuffixMatcher(WellKnownKustoEndpoints),
	}
}

// NewKustoTrustedEndpoints creates a KustoTrustedEndpoints with additional rules.
func NewKustoTrustedEndpoints(additionalRules []MatchRule) *KustoTrustedEndpoints {
	allRules := make([]MatchRule, 0, len(WellKnownKustoEndpoints)+len(additionalRules))
	allRules = append(allRules, WellKnownKustoEndpoints...)
	allRules = append(allRules, additionalRules...)
	return &KustoTrustedEndpoints{
		matcher:         NewFastSuffixMatcher(allRules),
		additionalRules: additionalRules,
	}
}

// IsTrustedEndpoint checks if the given hostname is a trusted Kusto endpoint.
func (e *KustoTrustedEndpoints) IsTrustedEndpoint(hostname string) bool {
	return e.matcher.Matches(hostname)
}

// ValidateEndpoint validates that the endpoint is trusted or security checks are skipped.
// Returns an error if the endpoint is not trusted and security checks are not skipped.
func (e *KustoTrustedEndpoints) ValidateEndpoint(hostname string, skipSecurityChecks bool) error {
	if skipSecurityChecks {
		return nil
	}
	if IsReservedHostname("https://" + hostname) {
		return nil
	}
	if !e.IsTrustedEndpoint(hostname) {
		return ingestoptions.NewIngestClientError(
			"endpoint is not a trusted Kusto endpoint: "+hostname+". Set skipSecurityChecks to bypass.",
			nil, true)
	}
	return nil
}
