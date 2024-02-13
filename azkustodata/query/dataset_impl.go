package query

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
)

// dataset is a basic implementation of Dataset, to be used by specific implementations.
type dataset struct {
	ctx                context.Context
	op                 errors.Op
	primaryResultsKind string
}

func (d *dataset) Context() context.Context {
	return d.ctx
}

func (d *dataset) Op() errors.Op {
	return d.op
}

func (d *dataset) PrimaryResultKind() string {
	return d.primaryResultsKind
}

func NewDataset(ctx context.Context, op errors.Op, primaryResultsKind string) Dataset {
	return &dataset{
		ctx:                ctx,
		op:                 op,
		primaryResultsKind: primaryResultsKind,
	}
}

type fullDataset struct {
	Dataset
	tables []FullTable
}

func NewFullDataset(base Dataset, tables []FullTable) FullDataset {
	return &fullDataset{
		Dataset: base,
		tables:  tables,
	}
}

func (d *fullDataset) Tables() []FullTable {
	return d.tables
}
