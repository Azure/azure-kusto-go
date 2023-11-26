package query

import (
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
)

type Column struct {
	Ordinal int
	Name    string
	Type    types.Column
}

type Row struct {
	table  Table
	values value.Values
}

func NewRow(t Table, values value.Values) *Row {
	return &Row{
		table:  t,
		values: values,
	}
}

type baseTable struct {
	id      int
	name    string
	kind    string
	columns []Column
}

type Table interface {
	Id() int
	Name() string
	Columns() []Column
	Kind() string
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