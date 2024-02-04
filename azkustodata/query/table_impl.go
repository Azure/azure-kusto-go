package query

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
)

type baseTable struct {
	dataSet       Dataset
	index         int64
	id            string
	name          string
	kind          string
	columns       []Column
	columnsByName map[string]Column
}

func NewTable(ds Dataset, index int64, id string, name string, kind string, columns []Column) Table {
	b := &baseTable{
		dataSet: ds,
		index:   index,
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

func (t *baseTable) Index() int64 {
	return t.index
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

func (t *baseTable) IsPrimaryResult() bool {
	return t.Kind() == t.dataSet.PrimaryResultKind()
}

func (t *baseTable) Op() errors.Op {
	set := t.dataSet
	if set == nil {
		return errors.OpUnknown
	}
	return set.Op()
}

type fullTable struct {
	Table
	rows []Row
}

func NewFullTable(base Table, rows []Row) FullTable {
	return &fullTable{
		Table: base,
		rows:  rows,
	}
}

func ReplaceFullTableRows(t FullTable, rows []Row) FullTable {
	// Sadly we need this escape hatchet because of the way v1 works - we need to supply the rows after the table is created.
	t.(*fullTable).rows = rows
	return t
}

func (t *fullTable) Rows() []Row {
	return t.rows
}

func (t *fullTable) ToFullTable() (FullTable, error) {
	return t, nil
}
