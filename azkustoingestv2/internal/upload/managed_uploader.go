// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package upload

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/config"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/resources"
)

// ManagedUploader implements Uploader with storage vs lake container selection.
type ManagedUploader struct {
	*ContainerUploaderBase

	logger *slog.Logger
}

// ManagedUploaderOption configures a ManagedUploader.
type ManagedUploaderOption func(*managedUploaderConfig)

type managedUploaderConfig struct {
	ignoreSizeLimit bool
	maxConcurrency  int
	maxDataSize     int64
	configCache     config.ConfigurationCache
	uploadMethod    ingestoptions.UploadMethod
	retryPolicy     ingestoptions.IngestRetryPolicy
}

// WithUploaderIgnoreSizeLimit sets whether to ignore size limits.
func WithUploaderIgnoreSizeLimit(ignore bool) ManagedUploaderOption {
	return func(c *managedUploaderConfig) { c.ignoreSizeLimit = ignore }
}

// WithUploaderMaxConcurrency sets the max upload concurrency.
func WithUploaderMaxConcurrency(n int) ManagedUploaderOption {
	return func(c *managedUploaderConfig) { c.maxConcurrency = n }
}

// WithUploaderMaxDataSize sets the max data size in bytes.
func WithUploaderMaxDataSize(n int64) ManagedUploaderOption {
	return func(c *managedUploaderConfig) { c.maxDataSize = n }
}

// WithUploaderConfigCache sets the configuration cache.
func WithUploaderConfigCache(cache config.ConfigurationCache) ManagedUploaderOption {
	return func(c *managedUploaderConfig) { c.configCache = cache }
}

// WithUploaderUploadMethod sets the upload method.
func WithUploaderUploadMethod(m ingestoptions.UploadMethod) ManagedUploaderOption {
	return func(c *managedUploaderConfig) { c.uploadMethod = m }
}

// WithUploaderRetryPolicy sets the retry policy.
func WithUploaderRetryPolicy(p ingestoptions.IngestRetryPolicy) ManagedUploaderOption {
	return func(c *managedUploaderConfig) { c.retryPolicy = p }
}

// NewManagedUploader creates a new ManagedUploader with the given options.
func NewManagedUploader(opts ...ManagedUploaderOption) (*ManagedUploader, error) {
	cfg := &managedUploaderConfig{
		maxConcurrency: ingestoptions.UploadContainerMaxConcurrency,
		maxDataSize:    ingestoptions.UploadContainerMaxDataSizeBytes,
		uploadMethod:   ingestoptions.UploadMethodDefault,
		retryPolicy:    ingestoptions.DefaultSimpleRetryPolicy(),
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.configCache == nil {
		return nil, fmt.Errorf("configuration cache is required")
	}

	base := NewContainerUploaderBase(
		cfg.retryPolicy,
		cfg.maxConcurrency,
		cfg.maxDataSize,
		cfg.configCache,
		cfg.uploadMethod,
	)
	base.ignoreSizeLimit = cfg.ignoreSizeLimit

	m := &ManagedUploader{
		ContainerUploaderBase: base,
		logger:                slog.Default(),
	}
	// Override the base's selectContainers with our implementation
	base.selectContainers = m.doSelectContainers
	return m, nil
}

// doSelectContainers selects containers based on upload method preference.
func (m *ManagedUploader) doSelectContainers(ctx context.Context, method ingestoptions.UploadMethod) (*resources.RoundRobinContainerList, error) {
	cachedConfig, err := m.configCache.GetConfiguration(ctx)
	if err != nil {
		return nil, ingestoptions.NewIngestClientError("failed to get configuration", err, false)
	}

	containerSettings := cachedConfig.ContainerSettings()
	if containerSettings == nil {
		return nil, ingestoptions.NewIngestClientError("no container settings available", nil, true)
	}

	hasStorage := len(containerSettings.Containers) > 0
	hasLake := len(containerSettings.LakeFolders) > 0

	m.logger.Debug("selecting containers",
		"method", method,
		"hasStorage", hasStorage,
		"hasLake", hasLake,
	)

	if !hasStorage && !hasLake {
		return nil, ingestoptions.NewIngestClientError("no containers available", nil, true)
	}

	if !hasStorage {
		return cachedConfig.LakeContainerList(), nil
	}
	if !hasLake {
		return cachedConfig.StorageContainerList(), nil
	}

	// Both types available — determine effective upload method
	effectiveMethod := method
	if method == ingestoptions.UploadMethodDefault {
		if containerSettings.PreferredUploadMethod == "Lake" {
			effectiveMethod = ingestoptions.UploadMethodLake
		} else {
			effectiveMethod = ingestoptions.UploadMethodStorage
		}
	}

	m.logger.Debug("selected upload method", "effective", effectiveMethod)

	if effectiveMethod == ingestoptions.UploadMethodLake {
		return cachedConfig.LakeContainerList(), nil
	}
	return cachedConfig.StorageContainerList(), nil
}

func (m *ManagedUploader) Close() error {
	return nil
}
