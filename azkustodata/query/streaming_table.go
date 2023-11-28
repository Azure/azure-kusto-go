package query

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
)

type RowResult struct {
	Row Row
	Err error
}

type streamingTable struct {
	baseTable
	dataset  *DataSet
	rawRows  chan RawRows
	rows     chan RowResult
	rowCount int
	skip     bool
	end      chan bool
}

func NewStreamingTable(dataset *DataSet, th *TableHeader) (StreamingTable, *errors.Error) {
	t := &streamingTable{
		baseTable: baseTable{
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
			return nil, errors.ES(errors.OpUnknown, errors.KClientArgs, "column[%d] if of type %q, which is not valid", i, c.ColumnType)
		}
	}

	go t.readRows()

	return t, nil
}

func (t *streamingTable) close(errors []OneApiError) {
	close(t.rawRows)

	b := <-t.end

	if b {
		for _, e := range errors {
			t.rows <- RowResult{Row: Row{}, Err: &e}
		}
	}

	close(t.rows)
}

var skipError = errors.ES(errors.OpUnknown, errors.KInternal, "skipping row")

func (t *streamingTable) readRows() {
	for rows := range t.rawRows {
		for _, r := range rows {
			if t.skip {
				t.rows <- RowResult{Row: Row{}, Err: skipError}
			} else {
				values := make(value.Values, len(r))
				for j, v := range r {
					parsed := value.Default(t.columns[j].Type)
					err := parsed.Unmarshal(v)
					if err != nil {
						t.rows <- RowResult{Row: Row{}, Err: errors.ES(errors.OpUnknown, errors.KInternal, "unable to unmarshal column %s into a %s value: %s", t.columns[j].Name, t.columns[j].Type, err)}
						continue
					}
					values[j] = parsed
				}
				t.rows <- RowResult{Row: *NewRow(t, values), Err: nil}
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
	t.skip = true

	var errs []error
	for r := range t.rows {
		if r.Err != skipError {
			errs = append(errs, r.Err)
		}
	}

	return errs
}
