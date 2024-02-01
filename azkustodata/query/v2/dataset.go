package v2

import (
	"github.com/Azure/azure-kusto-go/azkustodata/query"
)

// Dataset represents a result from kusto - a set of tables with metadata
// In v2, along with the tables, the dataset also contains the completion status of the query, and the query properties.
type Dataset interface {
	query.Dataset
	Close() error
}

// IterativeDataset represents an iterative result from kusto - where the tables are streamed as they are received from the service.
type IterativeDataset interface {
	Dataset
	Results() <-chan TableResult
	ToFullDataset() (FullDataset, error)
}

// FullDataset represents a full result from kusto - where all the tables are received before the dataset is returned.
type FullDataset interface {
	Dataset
	query.FullDataset
}
