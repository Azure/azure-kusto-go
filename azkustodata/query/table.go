package query

import "github.com/Azure/azure-kusto-go/azkustodata/errors"

type BaseTable interface {
	Id() string
	Index() int64
	Name() string
	Columns() []Column
	Kind() string
	ColumnByName(name string) Column
	Op() errors.Op
	IsPrimaryResult() bool
}

type Table interface {
	BaseTable
	ToFullTable() (FullTable, error)
}

type FullTable interface {
	BaseTable
	Rows() []Row
}
