package v2

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/query/common"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"strconv"
	"sync"
)

type streamingTable struct {
	common.BaseTable
	lock     sync.RWMutex
	rawRows  chan RawRows
	rows     chan query.RowResult
	rowCount int
	skip     bool
	end      chan bool
	closed   bool
}

func (t *streamingTable) RowCount() int {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.rowCount
}

func (t *streamingTable) setRowCount(rowCount int) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.rowCount = rowCount
}

func (t *streamingTable) Skip() bool {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.skip
}

func (t *streamingTable) setSkip(skip bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.skip = skip
}

func NewStreamingTable(dataset query.Dataset, th *TableHeader) (query.StreamingTable, *errors.Error) {
	t := &streamingTable{
		BaseTable: common.NewTable(dataset, int64(th.TableId), strconv.Itoa(th.TableId), th.TableName, th.TableKind, make([]query.Column, len(th.Columns))),
		rawRows:   make(chan RawRows),
		rows:      make(chan query.RowResult),
		end:       make(chan bool),
	}

	columns := t.Columns()
	for i, c := range th.Columns {
		columns[i] = common.NewColumn(i, c.ColumnName, types.Column(c.ColumnType))
		if !columns[i].Type().Valid() {
			return nil, errors.ES(t.Op(), errors.KClientArgs, "column[%d] if of type %q, which is not valid", i, c.ColumnType)
		}
	}

	go t.readRows()

	return t, nil
}

func (t *streamingTable) close(errors []OneApiError) {
	t.lock.Lock()

	if t.closed {
		t.lock.Unlock()
		return
	}

	t.closed = true

	close(t.rawRows)

	t.lock.Unlock()

	b := <-t.end

	if b {
		for _, e := range errors {
			t.rows <- query.RowResultError(&e)
		}
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	close(t.rows)
}

const skipError = "skipping row"

func (t *streamingTable) readRows() {
	for rows := range t.rawRows {
		for _, r := range rows {
			if t.Skip() {
				t.rows <- query.RowResultError(errors.ES(t.Op(), errors.KInternal, skipError))
			} else {
				values := make(value.Values, len(r))
				columns := t.Columns()
				for j, v := range r {
					parsed := value.Default(columns[j].Type())
					err := parsed.Unmarshal(v)
					if err != nil {
						t.rows <- query.RowResultError(errors.ES(t.Op(), errors.KInternal, "unable to unmarshal column %s into A %s value: %s", columns[j].Name, columns[j].Type, err))
						continue
					}
					values[j] = parsed
				}
				t.rows <- query.RowResultSuccess(common.NewRow(t, t.RowCount(), values))
			}
			t.rowCount++
		}
	}
	t.end <- true
}

func (t *streamingTable) Rows() <-chan query.RowResult {
	return t.rows
}

func (t *streamingTable) SkipToEnd() []error {
	t.setSkip(true)

	var errs []error
	for r := range t.rows {
		if err, ok := r.Err().(*errors.Error); ok && err.Err.Error() != skipError {
			errs = append(errs, err)
		}
	}

	return errs
}

func (t *streamingTable) Consume() ([]query.Row, []error) {
	var rows []query.Row
	var errs []error
	for r := range t.rows {
		if r.Err != nil {
			errs = append(errs, r.Err())
		} else {
			rows = append(rows, r.Row())
		}
	}

	return rows, errs
}
