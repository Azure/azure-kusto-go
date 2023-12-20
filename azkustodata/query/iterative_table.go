package query

type rowResult struct {
	row Row
	err error
}

func (r rowResult) Row() Row {
	return r.row
}

func (r rowResult) Err() error {
	return r.err
}

func RowResultSuccess(row Row) RowResult {
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
	Row() Row
	Err() error
}

// IterativeTable is a table that returns rows one at a time.
type IterativeTable interface {
	Table
	// Rows returns a channel that will be populated with rows as they are read.
	Rows() <-chan RowResult
	// SkipToEnd skips all remaining rows in the table.
	SkipToEnd() []error
}
