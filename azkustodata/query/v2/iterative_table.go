package v2

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"sync"
	"sync/atomic"
)

// iterativeTable represents a table that is streamed from the service.
// It is used by the iterative dataset.
// The rows are received from the service via the rawRows channel, and are parsed and sent to the rows channel.
type iterativeTable struct {
	query.BaseTable
	rows            chan query.RowResult
	rowCount        atomic.Uint32
	skip            atomic.Bool
	ctx             context.Context
	reportSkipError sync.Once
}

func (t *iterativeTable) addRawRows(rows []query.Row) {
	for _, row := range rows {
		if t.Skip() {
			t.reportSkipError.Do(func() {
				t.reportError(errors.ES(t.Op(), errors.KInternal, skipError))
			})
			return
		} else {
			if !t.reportRow(row) {
				return
			}
		}
		t.rowCount.Add(1)
	}
}

func (t *iterativeTable) RowCount() int {
	return int(t.rowCount.Load())
}

func (t *iterativeTable) setRowCount(rowCount int) {
	t.rowCount.Store(uint32(rowCount))
}

func (t *iterativeTable) Skip() bool {
	return t.skip.Load()
}

func (t *iterativeTable) setSkip(skip bool) {
	t.skip.Store(skip)
}

func NewIterativeTable(dataset *iterativeDataset, th TableHeader) (query.IterativeTable, error) {
	baseTable, err := newBaseTableFromHeader(dataset, th)
	if err != nil {
		return nil, err
	}

	t := &iterativeTable{
		BaseTable: baseTable,
		ctx:       dataset.Context(),
		rows:      make(chan query.RowResult, dataset.rowCapacity),
	}

	return t, nil
}

func (t *iterativeTable) finishTable(errs []OneApiError, cancelError error) {
	if cancelError != nil {
		t.reportError(cancelError)
	} else if errs != nil {
		t.reportError(combineOneApiErrors(errs))
	}
	close(t.rows)
}

func (t *iterativeTable) reportRow(row query.Row) bool {
	select {
	case t.rows <- query.RowResultSuccess(row):
		return true
	case <-t.ctx.Done():
		return false
	}
}

func (t *iterativeTable) reportError(err error) bool {
	select {
	case t.rows <- query.RowResultError(err):
		return true
	case <-t.ctx.Done():
		return false
	}
}

const skipError = "skipping row"

func (t *iterativeTable) Rows() <-chan query.RowResult {
	return t.rows
}

func (t *iterativeTable) SkipToEnd() []error {
	t.setSkip(true)

	var errs []error
	for r := range t.rows {
		if err, ok := r.Err().(*errors.Error); ok && err.Err.Error() != skipError {
			errs = append(errs, err)
		}
	}

	return errs
}

func (t *iterativeTable) ToTable() (query.Table, error) {
	if t.Skip() {
		return nil, errors.ES(t.Op(), errors.KInternal, "table is already skipped to the end")
	}

	var rows []query.Row
	for r := range t.rows {
		if r.Err() != nil {
			return nil, r.Err()
		} else {
			rows = append(rows, r.Row())
		}
	}

	return query.NewTable(t.BaseTable, rows), nil
}
