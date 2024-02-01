package v2

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
)

// FullDataset represents a full result from kusto - where all the tables are received before the dataset is returned.
type fullDataset struct {
	base   query.Dataset
	tables []query.FullTable
}

func (d *fullDataset) Context() context.Context {
	return d.base.Context()
}

func (d *fullDataset) Op() errors.Op {
	return d.base.Op()
}

func (d *fullDataset) Close() error {
	return nil
}

func (d *fullDataset) Tables() []query.FullTable {
	return d.tables
}

func NewFullDataset(base query.Dataset, tables []query.FullTable) FullDataset {
	return &fullDataset{
		base:   base,
		tables: tables,
	}
}
