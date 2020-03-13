// Package v1 holds framing information for the v1 REST API.
package v1

import (
	"fmt"
	"strings"

	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/data/value"
	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/internal/frames"
)

// Reference: This is what the top level data structure looks like for a V1 query. However, we are
// not using it because we want to stream the DataTable(s) back instead of reading all into memory.
/*
type DataSet struct {
	Tables []DataTable
}
*/

// DataTable represents a Kusto REST v1 DataTable that is returned in a DataSet.
type DataTable struct {
	TableName frames.TableKind
	Columns   table.Columns
	Rows      []value.Values

	Op errors.Op
}

// Unmarshal unmarshals a JSON decoded map value that represents a dataTable.
func (d *DataTable) Unmarshal(m map[string]interface{}) error {
	if err := d.unmarshalAttr(m); err != nil {
		return err
	}

	if err := d.unmarshalCols(m); err != nil {
		return err
	}

	if err := d.unmarshalRows(m); err != nil {
		return err
	}

	return nil
}

// dataTableFields are fields that must be included in a DataTable.
var dataTableFields = []string{frames.FieldTableName, frames.FieldColumns, frames.FieldRows}

func (d *DataTable) unmarshalAttr(m map[string]interface{}) error {
	for _, key := range dataTableFields {
		if _, exists := m[key]; !exists {
			return errors.E(d.Op, errors.KInternal, fmt.Errorf("DataTable.%s was not present", key))
		}
	}

	if v, ok := m[frames.FieldTableName].(string); ok {
		d.TableName = frames.TableKind(v)
	} else {
		return errors.E(d.Op, errors.KInternal, fmt.Errorf("DataTable.%s had non string entry, had type %T", frames.FieldTableName, m[frames.FieldTableName]))
	}

	if _, ok := m[frames.FieldColumns].([]interface{}); !ok {
		return errors.E(d.Op, errors.KInternal, fmt.Errorf("DataTable.Columns had type %T, expected []interface{}", m[frames.FieldColumns]))
	}
	return nil
}

func (d *DataTable) unmarshalCols(m map[string]interface{}) error {
	for _, inter := range m[frames.FieldColumns].([]interface{}) {
		m := inter.(map[string]interface{})
		for _, name := range []string{frames.FieldColumnName} {
			if _, exists := m[name]; !exists {
				return errors.E(d.Op, errors.KInternal, fmt.Errorf("DataTable.Columns had entry without .%s", name))
			}
		}
		cn, ok := m[frames.FieldColumnName].(string)
		if !ok {
			return errors.E(d.Op, errors.KInternal, fmt.Errorf("DataTable.Columns had entry with .ColumnName set to a %T type", m[frames.FieldColumnName]))
		}

		// Note: The v1 backend doesn't seem to send the ColumnType most of the time. So,
		// we need to convert the DataType, which is the C# name for the ColumnType. Kusto types seem to be C# types
		// just in lowercase instead of camel case. So we just convert it.
		cts, ok := m[frames.FieldColumnType].(string)
		ct := types.Column(cts)
		if !ok {
			dts, ok := m["DataType"].(string)
			if !ok {
				return errors.E(d.Op, errors.KInternal, fmt.Errorf("DataTable.Columns had entry with no .ColumnType set or .DataType "))
			}
			ct = types.Column(strings.ToLower(dts))
		}

		if !ct.Valid() {
			return errors.E(d.Op, errors.KInternal, fmt.Errorf("DataTable.Columns had entry with .ColumnType set to a %T type", m[frames.FieldColumnType]))
		}

		col := table.Column{
			Name: cn,
			Type: ct,
		}
		d.Columns = append(d.Columns, col)
	}
	return nil
}

func (d *DataTable) unmarshalRows(m map[string]interface{}) error {
	if _, ok := m[frames.FieldRows].([]interface{}); !ok {
		return errors.E(d.Op, errors.KInternal, fmt.Errorf("DataTable.Rows had type %T, expected []interface{}", m[frames.FieldRows]))
	}

	for x, inter := range m[frames.FieldRows].([]interface{}) {
		if _, ok := inter.([]interface{}); !ok {
			if err := errors.OneToErr(inter.(map[string]interface{}), d.Op); err != nil {
				return err
			}
			return errors.E(d.Op, errors.KInternal, fmt.Errorf("DataTable.Rows had entry(%d) of type %T, expected []interface{}", x, inter))
		}
		var newRow value.Values
		for i, inner := range inter.([]interface{}) {
			f := frames.Conversion[d.Columns[i].Type]
			if f == nil {
				return errors.E(d.Op, errors.KInternal, fmt.Errorf("in row %d, column %s: had unsupported type %s ", x, d.Columns[i].Name, d.Columns[i].Type))
			}
			inter, err := f(inner)
			if err != nil {
				return errors.E(d.Op, errors.KInternal, fmt.Errorf("in row %d, column %s, conversion error: %s", x, d.Columns[i].Name, err))
			}
			newRow = append(newRow, inter)
		}
		d.Rows = append(d.Rows, newRow)
	}
	return nil
}

func (DataTable) IsFrame() {}
