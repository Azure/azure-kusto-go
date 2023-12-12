package v2

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"io"
	"strconv"
)

type fullDataset struct {
	baseDataset
	frames                []Frame
	currentFrame          int
	currentStreamingTable *streamingTable

	tables []query.Table
	errors []error
}

type fragmentedTable struct {
	query.Table
	rows   []query.Row
	errors []error
}

func (f fragmentedTable) RowCount() int {
	return len(f.rows)
}

func (f fragmentedTable) addRawRows(rows RawRows) {
	for _, r := range rows {
		row, err := parseRow(r, f)
		if err != nil {
			f.errors = append(f.errors, err)
		}
		f.rows = append(f.rows, row)
	}
}

func (f fragmentedTable) close(errors []OneApiError) {
	for _, e := range errors {
		f.errors = append(f.errors, &e)
	}
}

func (d *fullDataset) newTableFromHeader(th *TableHeader) (table, error) {
	columns := make([]query.Column, len(th.Columns))
	err := parseColumns(th, columns, d.Op())
	if err != nil {
		return nil, err
	}

	return fragmentedTable{Table: query.NewFullTable(d, int64(th.TableId), strconv.Itoa(th.TableId), th.TableName, th.TableKind, columns, nil, nil)}, nil
}

func (d *fullDataset) finishTable(t table) {
	f := t.(*fragmentedTable)
	d.tables = append(d.tables, query.NewFullTable(d, t.Ordinal(), t.Id(), t.Name(), t.Kind(), t.Columns(), f.rows, f.errors))
}

func (d *fullDataset) getNextFrame() Frame {
	if d.frames == nil {
		return nil
	}
	if d.currentFrame >= len(d.frames) {
		return nil
	}
	f := d.frames[d.currentFrame]
	d.currentFrame++
	return f
}

func (d *fullDataset) reportError(err error) {
	d.errors = append(d.errors, err)
}

func (d *fullDataset) close() {
	d.frames = nil
}

func (d *fullDataset) GetAllTables() ([]query.Table, []error) {
	return d.tables, d.errors
}

func NewFullDataSet(ctx context.Context, r io.ReadCloser) (Dataset, error) {
	defer r.Close()
	full, err := ReadFramesFull(r)
	if err != nil {
		return nil, err
	}

	d := &fullDataset{
		baseDataset: *newBaseDataset(query.NewDataset(ctx, errors.OpQuery)),
		frames:      full,
	}

	decodeTables(d)

	if len(d.errors) > 0 {
		return nil, errors.GetCombinedError(d.errors...)
	}

	return d, nil
}
