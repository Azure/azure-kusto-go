package v2

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"io"
)

// FullDataset represents a full result from kusto - where all the tables are received before the dataset is returned.
type fullDataset struct {
	baseDataset
	frames       []Frame
	errors       []error
	currentFrame int

	tables []query.Table
}

func (d *fullDataset) Tables() []query.Table {
	return d.tables
}

func (d *fullDataset) onFinishTable() {
	f := d.currentTable.(*fragmentedTable)
	d.tables = append(d.tables, query.NewDataTable(d, f.Ordinal(), f.Id(), f.Name(), f.Kind(), f.Columns(), f.rows, f.errors))
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

func NewFullDataSet(ctx context.Context, r io.ReadCloser) (FullDataset, error) {
	defer func(r io.ReadCloser) {
		_ = r.Close()
	}(r)
	full, err := readFramesFull(r)
	if err != nil {
		return nil, err
	}

	d := &fullDataset{
		// We don't need a real mutex here - everything happens synchronously
		baseDataset: *newBaseDataset(query.NewDataset(ctx, errors.OpQuery), true),
		frames:      full,
	}

	decodeTables(d)

	if len(d.errors) > 0 {
		ret := d
		if d.header == nil {
			ret = nil
		}
		return ret, errors.GetCombinedError(d.errors...)
	}

	return d, nil
}
