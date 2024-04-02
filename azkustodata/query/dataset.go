package query

import (
	"context"
	"io"
	"net/http"

	"github.com/Azure/azure-kusto-go/azkustodata/errors"
)

type Response struct {
	Reader          io.ReadCloser
	ResponseHeaders http.Header
}

// BaseDataset represents a result from kusto - a set of tables with metadata
// This basic interface is implemented by all dataset types - both v1 and v2
// for specific
type BaseDataset interface {
	Context() context.Context
	Op() errors.Op

	PrimaryResultKind() string
	ResponseHeaders() http.Header
}

type Dataset interface {
	BaseDataset
	Tables() []Table
}

// IterativeDataset represents an iterative result from kusto - where the tables are streamed as they are received from the service.
type IterativeDataset interface {
	BaseDataset
	Tables() <-chan TableResult
	ToDataset() (Dataset, error)
	Close() error
}
