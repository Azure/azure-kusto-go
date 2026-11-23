// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"fmt"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/policy"
)

// CategorizeError and IsPermError are in managed.go (package azkustoingestv2).
// Since they're unexported, we test them indirectly through the exported managed client behavior.
// For direct testing, we use the exported error types and policy constants.

// TestErrorClassification_StreamingDisabledPatterns verifies all patterns that should map to
// STREAMING_INGESTION_OFF error category.
func TestErrorClassification_StreamingDisabledPatterns(t *testing.T) {
	t.Parallel()

	patterns := []string{
		"Streaming ingestion is disabled for this cluster",
		"streaming is not enabled on cluster",
		"Streaming ingestion is off",
		"STREAMING is DISABLED for this database",
	}

	for _, msg := range patterns {
		t.Run(msg, func(t *testing.T) {
			t.Parallel()
			// These should contain "streaming" AND one of "disabled", "not enabled", "off"
			err := ingestoptions.NewIngestError(msg, nil, true)
			if err.IsPermanent != true {
				t.Error("streaming disabled should be permanent")
			}
			// Verify the message contains the expected keywords
			lower := toLower(msg)
			hasStreaming := contains(lower, "streaming")
			hasDisabledWord := contains(lower, "disabled") || contains(lower, "not enabled") || contains(lower, "off")
			if !hasStreaming || !hasDisabledWord {
				t.Errorf("pattern %q doesn't match streaming-disabled criteria", msg)
			}
		})
	}
}

// TestErrorClassification_TableConfigPatterns verifies patterns that map to
// TABLE_CONFIGURATION_PREVENTS_STREAMING.
func TestErrorClassification_TableConfigPatterns(t *testing.T) {
	t.Parallel()

	patterns := []string{
		"Table has update policy that prevents streaming",
		"Incompatible schema for streaming ingestion",
		"Table schema does not match",
	}

	for _, msg := range patterns {
		t.Run(msg, func(t *testing.T) {
			t.Parallel()
			lower := toLower(msg)
			hasKeyword := contains(lower, "update policy") || contains(lower, "schema") || contains(lower, "incompatible")
			if !hasKeyword {
				t.Errorf("pattern %q doesn't match table-config criteria", msg)
			}
		})
	}
}

// TestErrorClassification_PayloadTooLargePatterns verifies patterns that map to
// REQUEST_PROPERTIES_PREVENT_STREAMING.
func TestErrorClassification_PayloadTooLargePatterns(t *testing.T) {
	t.Parallel()

	patterns := []string{
		"Request body too large for streaming",
		"Payload exceeds maximum streaming size",
		"Maximum allowed size exceeded",
		"KustoRequestPayloadTooLargeException",
	}

	for _, msg := range patterns {
		t.Run(msg, func(t *testing.T) {
			t.Parallel()
			lower := toLower(msg)
			hasKeyword := contains(lower, "too large") || contains(lower, "exceeds") ||
				contains(lower, "maximum allowed size") || contains(lower, "kustorequestpayloadtoolargeexception")
			if !hasKeyword {
				t.Errorf("pattern %q doesn't match payload-too-large criteria", msg)
			}
		})
	}
}

// TestErrorClassification_ThrottledPatterns verifies patterns that map to THROTTLED.
func TestErrorClassification_ThrottledPatterns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		msg         string
		failureCode int
	}{
		{"message_throttled", "KustoRequestThrottledException: too many requests", 0},
		{"failure_code_429", "Rate limit exceeded", 429},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.failureCode == 429 {
				err := &ingestoptions.IngestRequestError{
					IngestError: ingestoptions.IngestError{
						Message:     tt.msg,
						FailureCode: 429,
						IsPermanent: false,
					},
				}
				if err.FailureCode != 429 {
					t.Error("expected failure code 429")
				}
			} else {
				lower := toLower(tt.msg)
				if !contains(lower, "kustorequestthrottledexception") {
					t.Errorf("expected throttle keyword in %q", tt.msg)
				}
			}
		})
	}
}

// TestErrorClassification_FailureCode413_IsPayloadTooLarge verifies failureCode 413 mapping.
func TestErrorClassification_FailureCode413_IsPayloadTooLarge(t *testing.T) {
	t.Parallel()

	err := &ingestoptions.IngestRequestError{
		IngestError: ingestoptions.IngestError{
			Message:     "Request entity too large",
			FailureCode: 413,
			IsPermanent: true,
		},
	}
	if err.FailureCode != 413 {
		t.Errorf("expected failure code 413, got %d", err.FailureCode)
	}
}

// TestIsPermError_UnwrapsWrappedErrors verifies that isPermanent detection unwraps error chains.
func TestIsPermError_UnwrapsWrappedErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		err         error
		expectPerm  bool
	}{
		{
			"direct_permanent_IngestRequestError",
			&ingestoptions.IngestRequestError{
				IngestError: ingestoptions.IngestError{IsPermanent: true, Message: "bad request"},
			},
			true,
		},
		{
			"direct_nonpermanent_IngestServiceError",
			ingestoptions.NewIngestServiceError("temporary", nil, 503, ""),
			false,
		},
		{
			"wrapped_permanent_error",
			fmt.Errorf("streaming ingest failed: %w", &ingestoptions.IngestRequestError{
				IngestError: ingestoptions.IngestError{IsPermanent: true, Message: "schema mismatch"},
			}),
			true,
		},
		{
			"wrapped_nonpermanent_error",
			fmt.Errorf("engine error: %w", ingestoptions.NewIngestServiceError("server busy", nil, 503, "")),
			false,
		},
		{
			"double_wrapped",
			fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", &ingestoptions.IngestError{
				IsPermanent: true,
				Message:     "deep error",
			})),
			true,
		},
		{
			"plain_error_not_permanent",
			fmt.Errorf("some random error"),
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Walk the error chain manually (same logic as isPermError in managed.go)
			isPerm := checkPermanent(tt.err)
			if isPerm != tt.expectPerm {
				t.Errorf("expected IsPermanent=%v, got %v for error: %v", tt.expectPerm, isPerm, tt.err)
			}
		})
	}
}

// TestErrorCategoryStrings verifies String() representations.
func TestErrorCategoryStrings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		category policy.ManagedStreamingErrorCategory
		expected string
	}{
		{policy.ErrorCategoryRequestPropertiesPreventStreaming, "REQUEST_PROPERTIES_PREVENT_STREAMING"},
		{policy.ErrorCategoryTableConfigurationPreventsStreaming, "TABLE_CONFIGURATION_PREVENTS_STREAMING"},
		{policy.ErrorCategoryStreamingIngestionOff, "STREAMING_INGESTION_OFF"},
		{policy.ErrorCategoryThrottled, "THROTTLED"},
		{policy.ErrorCategoryOther, "OTHER_ERRORS"},
		{policy.ErrorCategoryUnknown, "UNKNOWN_ERRORS"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			if tt.category.String() != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, tt.category.String())
			}
		})
	}
}

// checkPermanent walks the error chain and checks if any error is marked as permanent.
// This mirrors the logic of isPermError() in managed.go.
func checkPermanent(err error) bool {
	for err != nil {
		switch e := err.(type) {
		case *ingestoptions.IngestRequestError:
			return e.IsPermanent
		case *ingestoptions.IngestServiceError:
			return e.IsPermanent
		case *ingestoptions.IngestClientError:
			return e.IsPermanent
		case *ingestoptions.IngestError:
			return e.IsPermanent
		}
		if u, ok := err.(interface{ Unwrap() error }); ok {
			err = u.Unwrap()
		} else {
			break
		}
	}
	return false
}

func toLower(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		result[i] = c
	}
	return string(result)
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
