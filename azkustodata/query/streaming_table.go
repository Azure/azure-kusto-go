package query

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"sync"
)

type RowResult struct {
	Row Row
	Err error
}

type streamingTable struct {
	baseTable
	lock     sync.RWMutex
	dataset  *DataSet
	rawRows  chan RawRows
	rows     chan RowResult
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

func NewStreamingTable(dataset *DataSet, th *TableHeader) (StreamingTable, *errors.Error) {
	t := &streamingTable{
		baseTable: baseTable{
			dataSet: dataset,
			id:      th.TableId,
			name:    th.TableName,
			kind:    th.TableKind,
			columns: make([]Column, len(th.Columns)),
		},
		dataset: dataset,
		rawRows: make(chan RawRows),
		rows:    make(chan RowResult),
		end:     make(chan bool),
	}

	for i, c := range th.Columns {

		t.columns[i] = Column{
			Ordinal: i,
			Name:    c.ColumnName,
			Type:    types.Column(c.ColumnType),
		}

		if !t.columns[i].Type.Valid() {
			return nil, errors.ES(dataset.op(), errors.KClientArgs, "column[%d] if of type %q, which is not valid", i, c.ColumnType)
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
			t.rows <- RowResult{Row: Row{}, Err: &e}
		}
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	close(t.rows)
}

func (t *streamingTable) readRows() {
	for rows := range t.rawRows {
		for _, r := range rows {
			if t.Skip() {
				t.rows <- RowResult{Row: Row{}, Err: errors.ES(t.op(), errors.KInternal, "skipping row")}
			} else {
				values := make(value.Values, len(r))
				for j, v := range r {
					parsed := value.Default(t.columns[j].Type)
					err := parsed.Unmarshal(v)
					if err != nil {
						t.rows <- RowResult{Row: Row{}, Err: errors.ES(t.op(), errors.KInternal, "unable to unmarshal column %s into A %s value: %s", t.columns[j].Name, t.columns[j].Type, err)}
						continue
					}
					values[j] = parsed
				}
				t.rows <- RowResult{Row: *NewRow(t, t.rowCount, values), Err: nil}
			}
			t.rowCount++
		}
	}
	t.end <- true
}

type StreamingTable interface {
	Table
	Rows() <-chan RowResult
	SkipToEnd() []error
}

func (t *streamingTable) Rows() <-chan RowResult {
	return t.rows
}

func (t *streamingTable) SkipToEnd() []error {
	t.setSkip(true)

	var errs []error
	for r := range t.rows {
		if r.Err != errors.ES(errors.OpUnknown, errors.KInternal, "skipping row") {
			errs = append(errs, r.Err)
		}
	}

	return errs
}

func (t *streamingTable) Consume() ([]Row, []error) {
	var rows []Row
	var errs []error
	for r := range t.rows {
		if r.Err != nil {
			errs = append(errs, r.Err)
		} else {
			rows = append(rows, r.Row)
		}
	}

	return rows, errs
}
