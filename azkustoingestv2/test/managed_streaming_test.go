// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"fmt"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/policy"
)

// We test the categorizeError and isPermError functions indirectly by
// creating errors and testing them through the policy.

func TestManagedStreamingPolicy_ErrorCallback_StreamingOff(t *testing.T) {
	p := policy.NewDefaultManagedStreamingPolicy()

	// Simulate streaming off error
	p.StreamingErrorCallback(nil, "testdb", "testtable", policy.ManagedStreamingRequestFailureDetails{
		IsPermanent:   true,
		ErrorCategory: policy.ErrorCategoryStreamingIngestionOff,
	})

	// By default, continueWhenUnavailable is false, so ShouldDefaultToQueued returns false
	// (it should NOT fall back silently, it should error)
	if p.ShouldDefaultToQueuedIngestion(nil, "testdb", "testtable") {
		t.Error("when continueWhenUnavailable=false, should NOT default to queued for streaming off")
	}
}

func TestManagedStreamingPolicy_ErrorCallback_StreamingOff_WithContinue(t *testing.T) {
	p := policy.NewDefaultManagedStreamingPolicy(policy.WithContinueWhenUnavailable(true))

	p.StreamingErrorCallback(nil, "testdb", "testtable", policy.ManagedStreamingRequestFailureDetails{
		IsPermanent:   true,
		ErrorCategory: policy.ErrorCategoryStreamingIngestionOff,
	})

	if !p.ShouldDefaultToQueuedIngestion(nil, "testdb", "testtable") {
		t.Error("when continueWhenUnavailable=true, should default to queued for streaming off")
	}
}

func TestManagedStreamingPolicy_ErrorCallback_TableConfig(t *testing.T) {
	p := policy.NewDefaultManagedStreamingPolicy()

	p.StreamingErrorCallback(nil, "testdb", "testtable", policy.ManagedStreamingRequestFailureDetails{
		IsPermanent:   true,
		ErrorCategory: policy.ErrorCategoryTableConfigurationPreventsStreaming,
	})

	if !p.ShouldDefaultToQueuedIngestion(nil, "testdb", "testtable") {
		t.Error("should default to queued after table config error")
	}
}

func TestManagedStreamingPolicy_ErrorCallback_Throttled(t *testing.T) {
	p := policy.NewDefaultManagedStreamingPolicy()

	p.StreamingErrorCallback(nil, "testdb", "testtable", policy.ManagedStreamingRequestFailureDetails{
		IsPermanent:   false,
		ErrorCategory: policy.ErrorCategoryThrottled,
	})

	if !p.ShouldDefaultToQueuedIngestion(nil, "testdb", "testtable") {
		t.Error("should default to queued after throttle")
	}
}

func TestManagedStreamingPolicy_SuccessClears(t *testing.T) {
	p := policy.NewDefaultManagedStreamingPolicy()

	// Set error state
	p.StreamingErrorCallback(nil, "testdb", "testtable", policy.ManagedStreamingRequestFailureDetails{
		IsPermanent:   true,
		ErrorCategory: policy.ErrorCategoryTableConfigurationPreventsStreaming,
	})

	// Clear via success
	p.StreamingSuccessCallback(nil, "testdb", "testtable", policy.ManagedStreamingRequestSuccessDetails{})

	if p.ShouldDefaultToQueuedIngestion(nil, "testdb", "testtable") {
		t.Error("success callback should clear error state")
	}
}

func TestManagedStreamingPolicy_DifferentTables(t *testing.T) {
	p := policy.NewDefaultManagedStreamingPolicy()

	p.StreamingErrorCallback(nil, "testdb", "table1", policy.ManagedStreamingRequestFailureDetails{
		IsPermanent:   true,
		ErrorCategory: policy.ErrorCategoryTableConfigurationPreventsStreaming,
	})

	// table2 should not be affected
	if p.ShouldDefaultToQueuedIngestion(nil, "testdb", "table2") {
		t.Error("error state should be per-table")
	}
}

func TestIngestServiceError_IsNonPermanent(t *testing.T) {
	err := ingestoptions.NewIngestServiceError("transient", nil, 500, "")
	if err.IsPermanent {
		t.Error("IngestServiceError should be non-permanent")
	}
}

func TestIngestError_Unwrap(t *testing.T) {
	cause := fmt.Errorf("root cause")
	err := ingestoptions.NewIngestError("wrapped", cause, false)
	if err.Unwrap() != cause {
		t.Error("Unwrap should return the cause")
	}
}
