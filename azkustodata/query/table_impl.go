package query

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
)

type baseTable struct {
	dataSet       Dataset
	ordinal       int64
	id            string
	name          string
	kind          string
	columns       []Column
	columnsByName map[string]Column
}

func NewTable(ds Dataset, ordinal int64, id string, name string, kind string, columns []Column) BaseTable {
	b := &baseTable{
		dataSet: ds,
		ordinal: ordinal,
		id:      id,
		name:    name,
		kind:    kind,
		columns: columns,
	}
	b.columnsByName = make(map[string]Column)
	for _, c := range columns {
		b.columnsByName[c.Name()] = c
	}

	return b
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

func (t *baseTable) Columns() []Column {
	return t.columns
}

func (t *baseTable) Kind() string {
	return t.kind
}

func (t *baseTable) ColumnByName(name string) Column {
	if c, ok := t.columnsByName[name]; ok {
		return c
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
	Columns() []Column
	Kind() string
	ColumnByName(name string) Column
	Op() errors.Op
}
