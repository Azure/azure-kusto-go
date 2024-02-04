package query

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
)

// Dataset represents a result from kusto - a set of tables with metadata
// This basic interface is implemented by all dataset types - both v1 and v2
// for specific
type Dataset interface {
	Context() context.Context
	Op() errors.Op

	PrimaryResultKind() string
}

type FullDataset interface {
	Dataset
	Tables() []FullTable
}

// IterativeDataset represents an iterative result from kusto - where the tables are streamed as they are received from the service.
type IterativeDataset interface {
	Dataset
	Tables() <-chan TableResult
	ToFullDataset() (FullDataset, error)
	Close() error
}
