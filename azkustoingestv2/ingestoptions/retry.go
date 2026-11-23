// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package ingestoptions

import (
	"math/rand"
	"time"
)

// IngestRetryPolicy defines the interface for retry policies.
type IngestRetryPolicy interface {
	// ShouldRetry returns true if the operation should be retried for the given attempt.
	ShouldRetry(attempt int) bool
	// GetDelay returns the delay before the next retry attempt.
	GetDelay(attempt int) time.Duration
	// MaxRetries returns the maximum number of retries.
	MaxRetries() int
}

// SimpleRetryPolicy implements a simple retry policy with a fixed interval.
type SimpleRetryPolicy struct {
	interval     time.Duration
	totalRetries int
}

// NewSimpleRetryPolicy creates a new SimpleRetryPolicy.
func NewSimpleRetryPolicy(interval time.Duration, totalRetries int) *SimpleRetryPolicy {
	return &SimpleRetryPolicy{
		interval:     interval,
		totalRetries: totalRetries,
	}
}

// DefaultSimpleRetryPolicy creates a SimpleRetryPolicy with default settings.
func DefaultSimpleRetryPolicy() *SimpleRetryPolicy {
	return NewSimpleRetryPolicy(IngestRetryPolicyDefaultInterval, IngestRetryPolicyDefaultTotalRetries)
}

func (p *SimpleRetryPolicy) ShouldRetry(attempt int) bool {
	return attempt < p.totalRetries
}

func (p *SimpleRetryPolicy) GetDelay(attempt int) time.Duration {
	return p.interval
}

func (p *SimpleRetryPolicy) MaxRetries() int {
	return p.totalRetries
}

// CustomRetryPolicy implements a retry policy with custom delay sequences.
type CustomRetryPolicy struct {
	delays []time.Duration
}

// NewCustomRetryPolicy creates a new CustomRetryPolicy with the given delays.
func NewCustomRetryPolicy(delays []time.Duration) *CustomRetryPolicy {
	return &CustomRetryPolicy{delays: delays}
}

func (p *CustomRetryPolicy) ShouldRetry(attempt int) bool {
	return attempt < len(p.delays)
}

func (p *CustomRetryPolicy) GetDelay(attempt int) time.Duration {
	if attempt < len(p.delays) {
		return p.delays[attempt]
	}
	return p.delays[len(p.delays)-1]
}

func (p *CustomRetryPolicy) MaxRetries() int {
	return len(p.delays)
}

// CreateDefaultManagedStreamingRetryPolicy creates the default retry policy
// for managed streaming with exponential backoff and jitter.
func CreateDefaultManagedStreamingRetryPolicy() IngestRetryPolicy {
	delays := make([]time.Duration, len(ManagedStreamingRetryDelays))
	for i, base := range ManagedStreamingRetryDelays {
		jitter := time.Duration(rand.Int63n(int64(ManagedStreamingRetryJitter)))
		delays[i] = base + jitter
	}
	return NewCustomRetryPolicy(delays)
}
