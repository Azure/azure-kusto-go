package common

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
)

type dataset struct {
	ctx context.Context
	op  errors.Op
}

func (d *dataset) Context() context.Context {
	return d.ctx
}

func (d *dataset) Op() errors.Op {
	return d.op
}

func NewDataset(ctx context.Context, op errors.Op) query.Dataset {
	return &dataset{
		ctx: ctx,
		op:  op,
	}
}
