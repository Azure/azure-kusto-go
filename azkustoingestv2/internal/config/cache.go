// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package config

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/resources"
)

// ConfigurationCache provides cached access to ingestion configuration.
type ConfigurationCache interface {
	// GetConfiguration returns the cached configuration, refreshing if needed.
	GetConfiguration(ctx context.Context) (*CachedConfigurationData, error)
	// RefreshInterval returns the refresh interval for this cache.
	RefreshInterval() time.Duration
	// Close stops background refresh and releases resources.
	Close()
}

// CachedConfigurationData wraps a ConfigurationResponse with pre-created
// RoundRobinContainerList instances for even distribution of uploads.
type CachedConfigurationData struct {
	Response *ConfigurationResponse

	storageOnce          sync.Once
	storageContainerList *resources.RoundRobinContainerList

	lakeOnce          sync.Once
	lakeContainerList *resources.RoundRobinContainerList
}

// NewCachedConfigurationData creates a new CachedConfigurationData.
func NewCachedConfigurationData(response *ConfigurationResponse) *CachedConfigurationData {
	return &CachedConfigurationData{Response: response}
}

// StorageContainerList returns the RoundRobinContainerList for storage containers.
// The list is lazily created and reused for all requests until the cache refreshes.
func (c *CachedConfigurationData) StorageContainerList() *resources.RoundRobinContainerList {
	c.storageOnce.Do(func() {
		if c.Response.ContainerSettings == nil || len(c.Response.ContainerSettings.Containers) == 0 {
			c.storageContainerList = resources.EmptyRoundRobinContainerList()
		} else {
			containers := make([]*resources.ExtendedContainerInfo, len(c.Response.ContainerSettings.Containers))
			for i, ci := range c.Response.ContainerSettings.Containers {
				containers[i] = &resources.ExtendedContainerInfo{
					ContainerName: ci.Path,
					SasURL:        ci.Path,
					UploadMethod:  ingestoptions.UploadMethodStorage,
				}
			}
			c.storageContainerList = resources.NewRoundRobinContainerList(containers)
		}
	})
	return c.storageContainerList
}

// LakeContainerList returns the RoundRobinContainerList for lake containers.
// The list is lazily created and reused for all requests until the cache refreshes.
func (c *CachedConfigurationData) LakeContainerList() *resources.RoundRobinContainerList {
	c.lakeOnce.Do(func() {
		if c.Response.ContainerSettings == nil || len(c.Response.ContainerSettings.LakeFolders) == 0 {
			c.lakeContainerList = resources.EmptyRoundRobinContainerList()
		} else {
			containers := make([]*resources.ExtendedContainerInfo, len(c.Response.ContainerSettings.LakeFolders))
			for i, ci := range c.Response.ContainerSettings.LakeFolders {
				containers[i] = &resources.ExtendedContainerInfo{
					ContainerName: ci.Path,
					SasURL:        ci.Path,
					UploadMethod:  ingestoptions.UploadMethodLake,
				}
			}
			c.lakeContainerList = resources.NewRoundRobinContainerList(containers)
		}
	})
	return c.lakeContainerList
}

// ContainerSettings returns the container settings from the response.
func (c *CachedConfigurationData) ContainerSettings() *ContainerSettings {
	return c.Response.ContainerSettings
}

// IngestionSettings returns the ingestion settings from the response.
func (c *CachedConfigurationData) IngestionSettings() *IngestionSettings {
	return c.Response.IngestionSettings
}

// cachedData is the internal cache entry with expiration tracking.
type cachedData struct {
	configuration *CachedConfigurationData
	fetchedAt     time.Time
	refreshAfter  time.Duration
}

// isExpired returns true if the cached data needs to be refreshed.
func (c *cachedData) isExpired() bool {
	return time.Since(c.fetchedAt) > c.refreshAfter
}

// DefaultConfigurationCache is the default implementation of ConfigurationCache.
type DefaultConfigurationCache struct {
	configClient *ConfigurationClient

	refreshInterval    time.Duration
	skipSecurityChecks bool

	mu    sync.RWMutex
	cache *cachedData
	done  chan struct{}
}

// DefaultConfigurationCacheOption configures a DefaultConfigurationCache.
type DefaultConfigurationCacheOption func(*DefaultConfigurationCache)

// WithCacheRefreshInterval sets the refresh interval for the cache.
func WithCacheRefreshInterval(interval time.Duration) DefaultConfigurationCacheOption {
	return func(c *DefaultConfigurationCache) { c.refreshInterval = interval }
}

// NewDefaultConfigurationCache creates a new DefaultConfigurationCache.
func NewDefaultConfigurationCache(
	configClient *ConfigurationClient,
	opts ...DefaultConfigurationCacheOption,
) *DefaultConfigurationCache {
	c := &DefaultConfigurationCache{
		configClient:    configClient,
		refreshInterval: ingestoptions.ConfigCacheDefaultRefreshInterval,
		done:            make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}

	go c.backgroundRefresh()

	return c
}

// GetConfiguration returns the cached configuration, refreshing if needed.
func (c *DefaultConfigurationCache) GetConfiguration(ctx context.Context) (*CachedConfigurationData, error) {
	c.mu.RLock()
	if c.cache != nil && !c.cache.isExpired() {
		cached := c.cache.configuration
		c.mu.RUnlock()
		return cached, nil
	}
	c.mu.RUnlock()

	return c.refreshConfiguration(ctx)
}

// RefreshInterval returns the configured refresh interval.
func (c *DefaultConfigurationCache) RefreshInterval() time.Duration {
	return c.refreshInterval
}

// Close stops the background refresh goroutine.
func (c *DefaultConfigurationCache) Close() {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}

func (c *DefaultConfigurationCache) refreshConfiguration(ctx context.Context) (*CachedConfigurationData, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if c.cache != nil && !c.cache.isExpired() {
		return c.cache.configuration, nil
	}

	resp, err := c.configClient.FetchConfiguration(ctx)
	if err != nil {
		// If we have stale data, return it rather than failing
		if c.cache != nil {
			return c.cache.configuration, nil
		}
		return nil, err
	}

	refreshAfter := c.refreshInterval
	if resp.ContainerSettings != nil && resp.ContainerSettings.RefreshInterval != "" {
		if parsed, parseErr := parseTimeSpan(resp.ContainerSettings.RefreshInterval); parseErr == nil {
			if parsed < refreshAfter {
				refreshAfter = parsed
			}
		}
	}

	c.cache = &cachedData{
		configuration: NewCachedConfigurationData(resp),
		fetchedAt:     time.Now(),
		refreshAfter:  refreshAfter,
	}

	return c.cache.configuration, nil
}

func (c *DefaultConfigurationCache) backgroundRefresh() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	elapsed := c.refreshInterval // Trigger immediate first fetch

	for {
		select {
		case <-ticker.C:
			elapsed += 30 * time.Second
			if elapsed >= c.refreshInterval {
				elapsed = 0
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				_, _ = c.refreshConfiguration(ctx)
				cancel()
			}
		case <-c.done:
			return
		}
	}
}

// parseTimeSpan parses a .NET-style time span string into a time.Duration.
// Supports: "HH:mm:ss", "d.HH:mm:ss", "HH:mm:ss.fffffff"
func parseTimeSpan(s string) (time.Duration, error) {
	var days, hours, minutes, seconds int
	var frac float64

	// Try d.HH:mm:ss format
	n, _ := fmt.Sscanf(s, "%d.%d:%d:%d", &days, &hours, &minutes, &seconds)
	if n == 4 {
		return time.Duration(days)*24*time.Hour +
			time.Duration(hours)*time.Hour +
			time.Duration(minutes)*time.Minute +
			time.Duration(seconds)*time.Second, nil
	}

	// Try HH:mm:ss.fffffff format
	n, _ = fmt.Sscanf(s, "%d:%d:%d.%f", &hours, &minutes, &seconds, &frac)
	if n >= 3 {
		d := time.Duration(hours)*time.Hour +
			time.Duration(minutes)*time.Minute +
			time.Duration(seconds)*time.Second
		if n == 4 {
			// Parse fractional seconds from string to avoid float precision issues
			for i := len(s) - 1; i >= 0; i-- {
				if s[i] == '.' {
					fracStr := s[i+1:]
					fracNanos := int64(0)
					for j, c := range fracStr {
						if j >= 7 {
							break
						}
						fracNanos = fracNanos*10 + int64(c-'0')
					}
					// Pad to 7 digits (100ns units)
					for j := len(fracStr); j < 7; j++ {
						fracNanos *= 10
					}
					d += time.Duration(fracNanos) * 100 * time.Nanosecond
					break
				}
			}
		}
		return d, nil
	}

	// Try plain HH:mm:ss
	n, err := fmt.Sscanf(s, "%d:%d:%d", &hours, &minutes, &seconds)
	if err != nil || n != 3 {
		return 0, fmt.Errorf("invalid time span format: %s", s)
	}
	return time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute + time.Duration(seconds)*time.Second, nil
}
