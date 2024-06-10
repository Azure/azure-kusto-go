package v2

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"sync"
)

// iterativeTable represents a table that is streamed from the service.
// It is used by the iterative dataset.
// The rows are received from the service via the rawRows channel, and are parsed and sent to the rows channel.
type iterativeTable struct {
	query.BaseTable
	lock     sync.RWMutex
	rawRows  chan []query.Row
	rows     chan query.RowResult
	rowCount int
	skip     bool
	ctx      context.Context
}

func (t *iterativeTable) addRawRows(rows []query.Row) {
	t.rawRows <- rows
}

func (t *iterativeTable) RowCount() int {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.rowCount
}

func (t *iterativeTable) setRowCount(rowCount int) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.rowCount = rowCount
}

func (t *iterativeTable) Skip() bool {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.skip
}

func (t *iterativeTable) setSkip(skip bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.skip = skip
}

func NewIterativeTable(dataset *iterativeDataset, th TableHeader) (query.IterativeTable, error) {
	baseTable, err := newBaseTableFromHeader(dataset, th)
	if err != nil {
		return nil, err
	}

	t := &iterativeTable{
		BaseTable: baseTable,
		ctx:       dataset.Context(),
		rawRows:   make(chan []query.Row, dataset.fragmentCapacity),
		rows:      make(chan query.RowResult, dataset.rowCapacity),
	}

	go t.readRows()

	return t, nil
}

func (t *iterativeTable) finishTable(errs []OneApiError) {
	if errs != nil {
		t.rows <- query.RowResultError(combineOneApiErrors(errs))
	}
	close(t.rawRows)
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

func (t *iterativeTable) readRows() {
	defer close(t.rows)

	for rows := range t.rawRows {
		for _, row := range rows {
			if t.Skip() {
				if !t.reportError(errors.ES(t.Op(), errors.KInternal, skipError)) {
					return
				}
			} else {
				if !t.reportRow(row) {
					return
				}
			}
			t.rowCount++
		}
	}
}

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
	t.lock.RLock()
	defer t.lock.RUnlock()
	if t.skip {
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
