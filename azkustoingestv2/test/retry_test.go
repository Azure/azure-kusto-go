// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package test

import (
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func TestSimpleRetryPolicy(t *testing.T) {
	p := ingestoptions.NewSimpleRetryPolicy(100*time.Millisecond, 3)

	if p.MaxRetries() != 3 {
		t.Errorf("expected max retries 3, got %d", p.MaxRetries())
	}
	if !p.ShouldRetry(0) {
		t.Error("should retry on attempt 0")
	}
	if !p.ShouldRetry(2) {
		t.Error("should retry on attempt 2")
	}
	if p.ShouldRetry(3) {
		t.Error("should not retry on attempt 3")
	}

	delay := p.GetDelay(0)
	if delay != 100*time.Millisecond {
		t.Errorf("expected 100ms delay, got %v", delay)
	}
}

func TestCustomRetryPolicy(t *testing.T) {
	delays := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
	}
	p := ingestoptions.NewCustomRetryPolicy(delays)

	if p.MaxRetries() != 3 {
		t.Errorf("expected max retries 3, got %d", p.MaxRetries())
	}
	if !p.ShouldRetry(0) {
		t.Error("should retry on attempt 0")
	}
	if !p.ShouldRetry(2) {
		t.Error("should retry on attempt 2")
	}
	if p.ShouldRetry(3) {
		t.Error("should not retry on attempt 3")
	}

	if p.GetDelay(0) != 100*time.Millisecond {
		t.Errorf("expected 100ms, got %v", p.GetDelay(0))
	}
	if p.GetDelay(1) != 200*time.Millisecond {
		t.Errorf("expected 200ms, got %v", p.GetDelay(1))
	}
	if p.GetDelay(2) != 400*time.Millisecond {
		t.Errorf("expected 400ms, got %v", p.GetDelay(2))
	}
}

func TestCreateDefaultManagedStreamingRetryPolicy(t *testing.T) {
	p := ingestoptions.CreateDefaultManagedStreamingRetryPolicy()
	if p.MaxRetries() != len(ingestoptions.ManagedStreamingRetryDelays) {
		t.Errorf("expected %d retries, got %d", len(ingestoptions.ManagedStreamingRetryDelays), p.MaxRetries())
	}
}
