package common

import "github.com/Azure/azure-kusto-go/azkustodata/query"

// TableResult is a structure that holds the result of a table operation.
// It contains a Table and an error, if any occurred during the operation.
type tableResult struct {
	// Table is the result of the operation.
	table query.StreamingTable
	// Err is the error that occurred during the operation, if any.
	err error
}

func (t *tableResult) Table() query.StreamingTable {
	return t.table
}
func (t *tableResult) Err() error {
	return t.err
}

func TableResultSuccess(table query.StreamingTable) query.TableResult {
	return &tableResult{
		table: table,
	}
}

func TableResultError(err error) query.TableResult {
	return &tableResult{
		err: err,
	}
}
