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
}
