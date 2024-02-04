package v2

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"strconv"
	"sync"
)

// iterativeTable represents a table that is streamed from the service.
// It is used by the iterative dataset.
// The rows are received from the service via the rawRows channel, and are parsed and sent to the rows channel.
type iterativeTable struct {
	query.Table
	lock     sync.RWMutex
	rawRows  chan RawRows
	rows     chan query.RowResult
	rowCount int
	skip     bool
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

func newTable(dataset query.Dataset, th TableHeader) (query.Table, error) {
	columns := make([]query.Column, len(th.Columns()))
	err := parseColumns(th, columns, dataset.Op())
	if err != nil {
		return nil, err
	}

	return query.NewTable(dataset, int64(th.TableId()), strconv.Itoa(th.TableId()), th.TableName(), th.TableKind(), columns), nil
}

func newIterativeTable(dataset query.Dataset, th TableHeader, rowsSize int) (query.IterativeTable, error) {
	baseTable, err := newTable(dataset, th)
	if err != nil {
		return nil, err
	}

	t := &iterativeTable{
		Table:   baseTable,
		rawRows: make(chan RawRows, rowsSize),
		rows:    make(chan query.RowResult, rowsSize),
	}

	go t.readRows()

	return t, nil
}

const defaultRowsSize = 100

func NewIterativeTable(dataset query.Dataset, th TableHeader) (query.IterativeTable, error) {
	return newIterativeTable(dataset, th, defaultRowsSize)
}

func parseColumns(th TableHeader, columns []query.Column, op errors.Op) *errors.Error {
	for i, c := range th.Columns() {
		columns[i] = query.NewColumn(i, c.ColumnName, types.Column(c.ColumnType))
		if !columns[i].Type().Valid() {
			return errors.ES(op, errors.KClientArgs, "column[%d] if of type %q, which is not valid", i, c.ColumnType)
		}
	}
	return nil
}

func parseRow(r []interface{}, t query.Table, index int) (query.Row, *errors.Error) {
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
			t.rows <- query.RowResultError(&e)
		}
	}
	close(t.rawRows)
}

const skipError = "skipping row"

func (t *iterativeTable) readRows() {
	for rows := range t.rawRows {
		for _, r := range rows {
			if r.Errors != nil {
				for _, e := range r.Errors {
					t.rows <- query.RowResultError(&e)
				}
				continue
			}

			if t.Skip() {
				t.rows <- query.RowResultError(errors.ES(t.Op(), errors.KInternal, skipError))
			} else {
				row, err := parseRow(r.Row, t, t.RowCount())
				if err != nil {
					t.rows <- query.RowResultError(err)
					continue
				}
				t.rows <- query.RowResultSuccess(row)
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

func (t *iterativeTable) ToFullTable() (query.FullTable, error) {
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

	return query.NewFullTable(t.Table, rows), nil
}
