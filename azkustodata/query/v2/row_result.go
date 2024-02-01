package v2

import "github.com/Azure/azure-kusto-go/azkustodata/query"

type rowResult struct {
	row query.Row
	err error
}

func (r rowResult) Row() query.Row {
	return r.row
}

func (r rowResult) Err() error {
	return r.err
}

func RowResultSuccess(row query.Row) RowResult {
	return rowResult{
		row: row,
	}
}

func RowResultError(err error) RowResult {
	return rowResult{
		err: err,
	}
}

// RowResult is a single streamed row from a table.
// It can contain either a row or an error.
type RowResult interface {
	Row() query.Row
	Err() error
}
