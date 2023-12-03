package query

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"sync"
)

type Column struct {
	Ordinal int
	Name    string
	Type    types.Column
}

type Row struct {
	table  Table
	values value.Values
	Index  int
}

func NewRow(t Table, index int, values value.Values) *Row {
	return &Row{
		table:  t,
		Index:  index,
		values: values,
	}
}

type baseTable struct {
	dataSet *DataSet
	id      int
	name    string
	kind    string
	columns []Column
	lock    sync.RWMutex
}

type Table interface {
	Id() int
	Name() string
	Columns() []Column
	Kind() string
	ColumnByName(name string) *Column
	IsPrimaryResult() bool
	Consume() ([]Row, []error)

	op() errors.Op
}

func (t *baseTable) Id() int {
	return t.id
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

func (t *baseTable) ColumnByName(name string) *Column {
	for _, c := range t.columns {
		if c.Name == name {
			return &c
		}
	}
	return nil
}

func (t *baseTable) op() errors.Op {
	set := t.dataSet
	if set == nil {
		return errors.OpUnknown
	}
	return set.op()
}

const PrimaryResultTableKind = "PrimaryResult"

func (t *baseTable) IsPrimaryResult() bool {
	return t.kind == PrimaryResultTableKind
}
