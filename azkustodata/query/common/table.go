package common

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
)

type baseTable struct {
	dataSet query.Dataset
	ordinal int64
	id      string
	name    string
	kind    string
	columns []query.Column
}

func NewTable(ds query.Dataset, ordinal int64, id string, name string, kind string, columns []query.Column) BaseTable {
	return &baseTable{
		dataSet: ds,
		ordinal: ordinal,
		id:      id,
		name:    name,
		kind:    kind,
		columns: columns,
	}
}

func (t *baseTable) Id() string {
	return t.id
}

func (t *baseTable) Ordinal() int64 {
	return t.ordinal
}

func (t *baseTable) Name() string {
	return t.name
}

func (t *baseTable) Columns() []query.Column {
	return t.columns
}

func (t *baseTable) Kind() string {
	return t.kind
}

func (t *baseTable) ColumnByName(name string) query.Column {
	for _, c := range t.columns {
		if c.Name() == name {
			return c
		}
	}
	return nil
}

func (t *baseTable) Op() errors.Op {
	set := t.dataSet
	if set == nil {
		return errors.OpUnknown
	}
	return set.Op()
}

type BaseTable interface {
	Id() string
	Ordinal() int64
	Name() string
	Columns() []query.Column
	Kind() string
	ColumnByName(name string) query.Column
	Op() errors.Op
}
