// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package azkustoingestv2

import (
	"github.com/Azure/azure-kusto-go/azkustoingestv2/ingestoptions"
	"github.com/Azure/azure-kusto-go/azkustoingestv2/internal/validation"
)

func validateIngestionSource(source ingestoptions.IngestionSource) error {
	if validation.IsNil(source) {
		return ingestoptions.NewIngestClientError("ingestion source cannot be nil", nil, true)
	}
	return nil
}

func validateIngestionOperation(op *IngestionOperation) error {
	if op == nil {
		return ingestoptions.NewIngestClientError("ingestion operation cannot be nil", nil, true)
	}
	return nil
}
