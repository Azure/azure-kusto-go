package v2

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"sync"
)

// iterativeTable represents a table that is streamed from the service.
// It is used by the iterative dataset.
// The rows are received from the service via the rawRows channel, and are parsed and sent to the rows channel.
type iterativeTable struct {
	query.BaseTable
	lock     sync.RWMutex
	rawRows  chan RawRows
	rows     chan query.RowResult
	rowCount int
	skip     bool
	ctx      context.Context
}

func (t *iterativeTable) addRawRows(rows RawRows) {
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
	baseTable, err := newBaseTable(dataset, th)
	if err != nil {
		return nil, err
	}

	t := &iterativeTable{
		ctx:       dataset.Context(),
		BaseTable: baseTable,
		rawRows:   make(chan RawRows, dataset.fragmentCapacity),
		rows:      make(chan query.RowResult, dataset.rowCapacity),
	}

	go t.readRows()

	return t, nil
}

func parseColumns(th TableHeader, columns []query.Column, op errors.Op) *errors.Error {
	for i, c := range th.Columns() {
		normal := types.NormalizeColumn(c.ColumnType)
		if normal == "" {
			return errors.ES(op, errors.KClientArgs, "column[%d] is of type %q, which is not valid", i, c.ColumnType)
		}

		columns[i] = query.NewColumn(i, c.ColumnName, normal)
	}
	return nil
}

func parseRow(r []interface{}, t query.BaseTable, index int) (query.Row, *errors.Error) {
	values := make(value.Values, len(r))
	columns := t.Columns()
	for j, v := range r {
		parsed := value.Default(columns[j].Type())
		err := parsed.Unmarshal(v)

		if err != nil {
			return nil, errors.ES(t.Op(), errors.KInternal, "unable to unmarshal column %s into A %s value: %s", columns[j].Name(), columns[j].Type(), err)
		}
		values[j] = parsed
	}
	row := query.NewRow(t, index, values)
	return row, nil
}

func (t *iterativeTable) finishTable(errors []OneApiError) {
	if errors != nil {
		for _, e := range errors {
			t.tryInsertValue(nil, &e)
		}
	}
	close(t.rawRows)
}

func (t *iterativeTable) tryInsertValue(row query.Row, err error) {
	if err != nil {
		select {
		case t.rows <- query.RowResultError(err):
			break
		case <-t.ctx.Done():
			break
		}
	} else {
		select {
		case t.rows <- query.RowResultSuccess(row):
			break
		case <-t.ctx.Done():
			break
		}
	}
}

const skipError = "skipping row"

func (t *iterativeTable) readRows() {
	for rows := range t.rawRows {
		for _, r := range rows {
			if t.Skip() {
				t.tryInsertValue(nil, errors.ES(t.Op(), errors.KInternal, skipError))
			} else {
				row, err := parseRow(r, t, t.RowCount())
				if err != nil {
					t.tryInsertValue(nil, err)
					continue
				}
				t.tryInsertValue(row, nil)
			}
			t.rowCount++
		}
	}

	close(t.rows)
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
