package common

import (
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
)

type column struct {
	ordinal   int
	name      string
	kustoType types.Column
}

func (c column) Ordinal() int {
	return c.ordinal
}

func (c column) Name() string {
	return c.name
}

func (c column) Type() types.Column {
	return c.kustoType
}

func NewColumn(ordinal int, name string, kustoType types.Column) query.Column {
	return &column{
		ordinal:   ordinal,
		name:      name,
		kustoType: kustoType,
	}
}
