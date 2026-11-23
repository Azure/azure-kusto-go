// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"log/slog"

	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
)

func closeIngestionSource(source ingestoptions.IngestionSource, logger *slog.Logger) {
	// Internal wrappers borrow the source; the outer Ingest call owns cleanup.
	switch s := source.(type) {
	case *ingestoptions.FileSource:
		if s == nil {
			return
		}
	case *ingestoptions.StreamSource:
		if s == nil {
			return
		}
	default:
		return
	}
	if err := source.Close(); err != nil {
		logger.Warn("failed to close ingestion source", "source", source.Name(), "error", err)
	}
}
