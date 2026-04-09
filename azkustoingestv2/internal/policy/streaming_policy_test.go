// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package policy

import (
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func TestDefaultManagedStreamingPolicy(t *testing.T) {
	policy := NewDefaultManagedStreamingPolicy()

	if policy.ContinueWhenStreamingIngestionUnavailable() != ingestoptions.ManagedStreamingContinueWhenUnavailableDefault {
		t.Errorf("unexpected ContinueWhenUnavailable: %v", policy.ContinueWhenStreamingIngestionUnavailable())
	}

	if policy.DataSizeFactor() != ingestoptions.ManagedStreamingDataSizeFactorDefault {
		t.Errorf("unexpected DataSizeFactor: %f", policy.DataSizeFactor())
	}

	if policy.RetryPolicy() == nil {
		t.Error("RetryPolicy should not be nil")
	}
}

func TestPolicyShouldDefaultToQueuedByTable(t *testing.T) {
	policy := NewDefaultManagedStreamingPolicy(
		WithContinueWhenUnavailable(true),
	)

	// Initially, no table should default to queued
	if policy.ShouldDefaultToQueuedIngestion(nil, "db", "table1") {
		t.Error("should not default to queued initially")
	}

	// Simulate a streaming ingestion off error
	policy.StreamingErrorCallback(nil, "db", "table1", ManagedStreamingRequestFailureDetails{
		Duration:      100 * time.Millisecond,
		IsPermanent:   true,
		ErrorCategory: ErrorCategoryStreamingIngestionOff,
	})

	// Now should default to queued for this table
	if !policy.ShouldDefaultToQueuedIngestion(nil, "db", "table1") {
		t.Error("should default to queued after streaming off error")
	}

	// Different table should not be affected
	if policy.ShouldDefaultToQueuedIngestion(nil, "db", "table2") {
		t.Error("different table should not be affected")
	}
}

func TestPolicyThrottleBackoff(t *testing.T) {
	policy := NewDefaultManagedStreamingPolicy(
		WithThrottleBackoff(50 * time.Millisecond),
	)

	// Simulate throttle error
	policy.StreamingErrorCallback(nil, "db", "table1", ManagedStreamingRequestFailureDetails{
		Duration:      10 * time.Millisecond,
		IsPermanent:   false,
		ErrorCategory: ErrorCategoryThrottled,
	})

	// Should default to queued
	if !policy.ShouldDefaultToQueuedIngestion(nil, "db", "table1") {
		t.Error("should default to queued after throttle")
	}

	// Wait for backoff to expire
	time.Sleep(60 * time.Millisecond)

	// Should no longer default to queued
	if policy.ShouldDefaultToQueuedIngestion(nil, "db", "table1") {
		t.Error("should resume streaming after backoff period")
	}
}

func TestPolicyWithOptions(t *testing.T) {
	policy := NewDefaultManagedStreamingPolicy(
		WithContinueWhenUnavailable(true),
		WithPolicyDataSizeFactor(2.0),
	)

	if !policy.ContinueWhenStreamingIngestionUnavailable() {
		t.Error("expected ContinueWhenUnavailable to be true")
	}
	if policy.DataSizeFactor() != 2.0 {
		t.Errorf("expected DataSizeFactor 2.0, got %f", policy.DataSizeFactor())
	}
}
