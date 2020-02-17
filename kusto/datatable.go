package kusto

import (
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-kusto-go/kusto/errors"
	"github.com/Azure/azure-kusto-go/kusto/types"
)

// dataTable is used report information as a Table with Columns as row headers and Rows as the contained
// data. It implements Frame.
type dataTable struct {
	baseFrame
	// TableID is a numeric representation of the this dataTable in relation to other dataTables returned
	// in numeric order starting at 09.
	TableID int `json:"TableId"`
	// TableKind is a Kusto dataTable sub-type.
	TableKind string
	// TableName is a name for the dataTable.
	TableName string
	// Columns is a list of column names and their Kusto storage types.
	Columns Columns
	// Rows contains the the table data that was fetched.
	Rows []types.KustoValues

	op errors.Op
}

// dataTableFields are fields that must be included in a DataTable.
var dataTableFields = []string{kFrameType, kTableID, kTableKind, kTableName, kColumns, kRows}

func (dt *dataTable) unmarshalAttributes(m map[string]interface{}) (err error) {
	for _, key := range dataTableFields {
		if _, exists := m[key]; !exists {
			return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable.%s was not present", key))
		}
	}

	if ft, ok := m[kFrameType].(string); !ok {
		return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable.Unmarshal received data with no FrameType key"))
	} else if ft != ftDataTable {
		return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable.Unmarshal received data with FrameType == %s", ft))
	} else {
		dt.baseFrame.FrameType = ft
	}

	jn, ok := m[kTableID].(json.Number)
	if !ok {
		return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable.TableId was not a json.Number"))
	}
	tblID, err := jn.Int64()
	if err != nil {
		return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable entry TableID was not an int64, was %s", m[kFrameType].(json.Number).String()))
	}
	dt.TableID = int(tblID)

	if v, ok := m[kTableKind].(string); ok {
		dt.TableKind = v
	} else {
		return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable.%s had non string entry, had type %T", kTableKind, m[kTableKind]))
	}

	if v, ok := m[kTableName].(string); ok {
		dt.TableName = v
	} else {
		return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable.%s had non string entry, had type %T", kTableName, m[kTableName]))
	}

	if _, ok := m[kColumns].([]interface{}); !ok {
		return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable.Columns had type %T, expected []interface{}", m[kColumns]))
	}
	return nil
}

func (dt *dataTable) unmarshalColumns(cols []interface{}) (err error) {
	for _, inter := range cols {
		m := inter.(map[string]interface{})
		for _, name := range []string{kColumnName, kColumnType} {
			if _, exists := m[name]; !exists {
				return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable.Columns had entry without .%s", name))
			}
		}
		cn, ok := m[kColumnName].(string)
		if !ok {
			return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable.Columns had entry with .ColumnName set to a %T type", m[kColumnName]))
		}
		ct, ok := m[kColumnType].(string)
		if !ok {
			return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable.Columns had entry with .DataType set to a %T type", m[kColumnType]))
		}
		col := Column{
			Name: cn,
			Type: ct,
		}
		dt.Columns = append(dt.Columns, col)
	}

	return nil
}

func (dt *dataTable) unmarshalRows(rows []interface{}) (e error) {
	for x, inter := range rows {
		if _, ok := inter.([]interface{}); !ok {
			if err := errors.OneToErr(inter.(map[string]interface{}), dt.op); err != nil {
				return err
			}
			return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable.Rows had entry(%d) of type %T, expected []interface{}", x, inter))
		}
		var newRow types.KustoValues

		// unmarshal cells
		for i, raw := range inter.([]interface{}) {
			convertFn := conversion[dt.Columns[i].Type]
			if convertFn == nil {
				return errors.E(dt.op, errors.KInternal, fmt.Errorf("in row %d, column %s: had unsupported type %s ", x, dt.Columns[i].Name, dt.Columns[i].Type))
			}
			cell, err := convertFn(raw)
			if err != nil {
				return errors.E(dt.op, errors.KInternal, fmt.Errorf("in row %d, column %s, conversion error: %s", x, dt.Columns[i].Name, err))
			}
			newRow = append(newRow, cell)
		}
		dt.Rows = append(dt.Rows, newRow)
	}

	return nil
}

// Unmarshal unmarshals a JSON decoded map value that represents a dataTable.
func (dt *dataTable) Unmarshal(m map[string]interface{}) (err error) {
	// Ensure all expected attributes are part of the datatable object
	for _, attribute := range [...]string{kTableID, kTableKind, kTableName, kFrameType, kColumns, kRows} {
		if _, exists := m[attribute]; !exists {
			return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable.%s was not present", attribute))
		}
	}

	if e := dt.unmarshalAttributes(m); e != nil {
		return e
	}

	if _, ok := m[kColumns].([]interface{}); !ok {
		return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable.Columns had type %T, expected []interface{}", m[kColumns]))
	}

	if e := dt.unmarshalColumns(m[kColumns].([]interface{})); e != nil {
		return e
	}

	if _, ok := m[kRows].([]interface{}); !ok {
		return errors.E(dt.op, errors.KInternal, fmt.Errorf("dataTable.Rows had type %T, expected []interface{}", m[kRows]))
	}

	if e := dt.unmarshalRows(m[kRows].([]interface{})); e != nil {
		return e
	}

	return nil
}

func (dataTable) isFrame() {}
