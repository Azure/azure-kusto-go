// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package policy

import (
	"sync"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

// ManagedStreamingErrorCategory represents categories of errors during managed streaming.
type ManagedStreamingErrorCategory int

const (
	// ErrorCategoryRequestPropertiesPreventStreaming indicates streaming cannot be performed
	// due to the properties of the request itself.
	ErrorCategoryRequestPropertiesPreventStreaming ManagedStreamingErrorCategory = iota

	// ErrorCategoryTableConfigurationPreventsStreaming indicates streaming cannot be performed
	// due to a conflicting table configuration.
	ErrorCategoryTableConfigurationPreventsStreaming

	// ErrorCategoryStreamingIngestionOff indicates streaming cannot be performed
	// due to service configuration.
	ErrorCategoryStreamingIngestionOff

	// ErrorCategoryThrottled indicates the streaming endpoint is throttled (HTTP 429).
	ErrorCategoryThrottled

	// ErrorCategoryOther covers all other streaming errors.
	ErrorCategoryOther

	// ErrorCategoryUnknown covers unexpected error types.
	ErrorCategoryUnknown
)

// String returns the string representation of the error category.
func (c ManagedStreamingErrorCategory) String() string {
	switch c {
	case ErrorCategoryRequestPropertiesPreventStreaming:
		return "REQUEST_PROPERTIES_PREVENT_STREAMING"
	case ErrorCategoryTableConfigurationPreventsStreaming:
		return "TABLE_CONFIGURATION_PREVENTS_STREAMING"
	case ErrorCategoryStreamingIngestionOff:
		return "STREAMING_INGESTION_OFF"
	case ErrorCategoryThrottled:
		return "THROTTLED"
	case ErrorCategoryOther:
		return "OTHER_ERRORS"
	default:
		return "UNKNOWN_ERRORS"
	}
}

// ManagedStreamingRequestFailureDetails contains details about a failed streaming request.
type ManagedStreamingRequestFailureDetails struct {
	Duration      time.Duration
	IsPermanent   bool
	ErrorCategory ManagedStreamingErrorCategory
	Err           error
}

// ManagedStreamingRequestSuccessDetails contains details about a successful streaming request.
type ManagedStreamingRequestSuccessDetails struct {
	Duration time.Duration
}

// ManagedStreamingPolicy controls managed streaming client behavior on errors.
type ManagedStreamingPolicy interface {
	// ContinueWhenStreamingIngestionUnavailable returns true if the client should
	// fall back to queued ingestion when streaming is unavailable.
	ContinueWhenStreamingIngestionUnavailable() bool

	// RetryPolicy returns the retry policy for streaming attempts.
	RetryPolicy() ingestoptions.IngestRetryPolicy

	// DataSizeFactor returns the size factor for determining the streaming size threshold.
	DataSizeFactor() float64

	// ShouldDefaultToQueuedIngestion returns true if this request should skip
	// streaming and go directly to queued ingestion.
	ShouldDefaultToQueuedIngestion(source ingestoptions.IngestionSource, database, table string) bool

	// StreamingErrorCallback is called when a streaming error occurs.
	StreamingErrorCallback(source ingestoptions.IngestionSource, database, table string, details ManagedStreamingRequestFailureDetails)

	// StreamingSuccessCallback is called when streaming succeeds.
	StreamingSuccessCallback(source ingestoptions.IngestionSource, database, table string, details ManagedStreamingRequestSuccessDetails)
}

// ManagedStreamingErrorState tracks the error state for a table.
type ManagedStreamingErrorState struct {
	ResetStateAt time.Time
	ErrorState   ManagedStreamingErrorCategory
}

// DefaultManagedStreamingPolicy is the default policy for managed streaming.
// When there is a permanent streaming error, it defaults to queued ingestion
// for a time period.
type DefaultManagedStreamingPolicy struct {
	continueWhenUnavailable   bool
	dataSizeFactor            float64
	retryPolicy               ingestoptions.IngestRetryPolicy
	throttleBackoffPeriod     time.Duration
	timeUntilResumingStreaming time.Duration

	mu                      sync.RWMutex
	defaultToQueuedByTable  map[string]*ManagedStreamingErrorState
}

// DefaultManagedStreamingPolicyOption configures a DefaultManagedStreamingPolicy.
type DefaultManagedStreamingPolicyOption func(*DefaultManagedStreamingPolicy)

// WithContinueWhenUnavailable sets whether to continue with queued when streaming is unavailable.
func WithContinueWhenUnavailable(v bool) DefaultManagedStreamingPolicyOption {
	return func(p *DefaultManagedStreamingPolicy) { p.continueWhenUnavailable = v }
}

// WithPolicyDataSizeFactor sets the data size factor.
func WithPolicyDataSizeFactor(v float64) DefaultManagedStreamingPolicyOption {
	return func(p *DefaultManagedStreamingPolicy) { p.dataSizeFactor = v }
}

// WithPolicyRetryPolicy sets the retry policy.
func WithPolicyRetryPolicy(v ingestoptions.IngestRetryPolicy) DefaultManagedStreamingPolicyOption {
	return func(p *DefaultManagedStreamingPolicy) { p.retryPolicy = v }
}

// WithThrottleBackoff sets the throttle backoff period.
func WithThrottleBackoff(v time.Duration) DefaultManagedStreamingPolicyOption {
	return func(p *DefaultManagedStreamingPolicy) { p.throttleBackoffPeriod = v }
}

// NewDefaultManagedStreamingPolicy creates a new DefaultManagedStreamingPolicy.
func NewDefaultManagedStreamingPolicy(opts ...DefaultManagedStreamingPolicyOption) *DefaultManagedStreamingPolicy {
	p := &DefaultManagedStreamingPolicy{
		continueWhenUnavailable:    ingestoptions.ManagedStreamingContinueWhenUnavailableDefault,
		dataSizeFactor:             ingestoptions.ManagedStreamingDataSizeFactorDefault,
		retryPolicy:               ingestoptions.CreateDefaultManagedStreamingRetryPolicy(),
		throttleBackoffPeriod:      ingestoptions.ManagedStreamingThrottleBackoff,
		timeUntilResumingStreaming: ingestoptions.ManagedStreamingResumeTime,
		defaultToQueuedByTable:     make(map[string]*ManagedStreamingErrorState),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *DefaultManagedStreamingPolicy) ContinueWhenStreamingIngestionUnavailable() bool {
	return p.continueWhenUnavailable
}

func (p *DefaultManagedStreamingPolicy) RetryPolicy() ingestoptions.IngestRetryPolicy {
	return p.retryPolicy
}

func (p *DefaultManagedStreamingPolicy) DataSizeFactor() float64 {
	return p.dataSizeFactor
}

func (p *DefaultManagedStreamingPolicy) ShouldDefaultToQueuedIngestion(source ingestoptions.IngestionSource, database, table string) bool {
	key := database + "-" + table

	p.mu.RLock()
	state, exists := p.defaultToQueuedByTable[key]
	p.mu.RUnlock()

	if !exists {
		return false
	}

	if state.ResetStateAt.After(time.Now()) {
		// If streaming is off and we're not configured to continue, return false to fail
		if state.ErrorState == ErrorCategoryStreamingIngestionOff && !p.continueWhenUnavailable {
			return false
		}
		return true
	}

	// Time expired, remove the entry
	p.mu.Lock()
	delete(p.defaultToQueuedByTable, key)
	p.mu.Unlock()

	return false
}

func (p *DefaultManagedStreamingPolicy) StreamingErrorCallback(source ingestoptions.IngestionSource, database, table string, details ManagedStreamingRequestFailureDetails) {
	key := database + "-" + table

	p.mu.Lock()
	defer p.mu.Unlock()

	switch details.ErrorCategory {
	case ErrorCategoryStreamingIngestionOff, ErrorCategoryTableConfigurationPreventsStreaming:
		p.defaultToQueuedByTable[key] = &ManagedStreamingErrorState{
			ResetStateAt: time.Now().Add(p.timeUntilResumingStreaming),
			ErrorState:   details.ErrorCategory,
		}
	case ErrorCategoryThrottled:
		p.defaultToQueuedByTable[key] = &ManagedStreamingErrorState{
			ResetStateAt: time.Now().Add(p.throttleBackoffPeriod),
			ErrorState:   details.ErrorCategory,
		}
	}
}

func (p *DefaultManagedStreamingPolicy) StreamingSuccessCallback(source ingestoptions.IngestionSource, database, table string, details ManagedStreamingRequestSuccessDetails) {
	// Default implementation does nothing
}
