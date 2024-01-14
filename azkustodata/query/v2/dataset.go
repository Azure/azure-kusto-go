package v2

import (
	"github.com/Azure/azure-kusto-go/azkustodata/query"
)

// Dataset represents a result from kusto - a set of tables with metadata
// In v2, along with the tables, the dataset also contains the completion status of the query, and the query properties.
type Dataset interface {
	query.Dataset
	Header() *DataSetHeader
	Completion() *DataSetCompletion
	QueryProperties() []QueryProperties
	QueryCompletionInformation() []QueryCompletionInformation
	GetAllTables() ([]query.Table, []error)
	Close() error
}

// IterativeDataset represents an iterative result from kusto - where the tables are streamed as they are received from the service.
type IterativeDataset interface {
	Dataset
	Results() <-chan query.TableResult
}

// FullDataset represents a full result from kusto - where all the tables are received before the dataset is returned.
type FullDataset interface {
	Dataset
	query.FullDataset
}

// dataset is the internal interface for the dataset implementation, it is used by both the iterative and full datasets implementations.
type dataset interface {
	Dataset
	setHeader(dataSetHeader *DataSetHeader)
	setCompletion(completion *DataSetCompletion)
	setCurrentTable(currentTable table)
	getCurrentTable() table
	newTableFromHeader(th *TableHeader) (table, error)
	getNextFrame() Frame
	reportError(err error)
	onFinishHeader()
	onFinishTable()
	parseSecondaryTable(t query.Table) error
}
