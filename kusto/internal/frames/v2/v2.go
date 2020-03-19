// Package v2 holds framing information for the v2 REST API.
package v2

import (
	"encoding/json"
	"fmt"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/data/value"
	"github.com/Azure/azure-kusto-go/kusto/internal/frames"
)

// Base is information that is encoded in all frames. The fields aren't actually
// in the spec, but are transmitted on the wire.
type Base struct {
	FrameType string
}

// DataSetHeader is the first frame in a response. It implements Frame.
type DataSetHeader struct {
	Base
	// Version is the version of the APi responding. The current version is "v2.0".
	Version string
	// IsProgressive indicates that TableHeader, TableFragment, TableProgress, and TableCompletion
	IsProgressive bool

	Op errors.Op
}

func (DataSetHeader) IsFrame() {}

// DataTable is used report information as a Table with Columns as row headers and Rows as the contained
// data. It implements Frame.
type DataTable struct {
	Base
	// TableID is a numeric representation of the this dataTable in relation to other dataTables returned
	// in numeric order starting at 09.
	TableID int `json:"TableId"`
	// TableKind is a Kusto dataTable sub-type.
	TableKind frames.TableKind
	// TableName is a name for the dataTable.
	TableName frames.TableKind
	// Columns is a list of column names and their Kusto storage types.
	Columns table.Columns
	// Rows contains the the table data that was fetched.
	Rows []value.Values

	Op errors.Op `json:"-"`
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
var dataTableFields = []string{frames.FieldFrameType, frames.FieldTableID, frames.FieldTableKind,
	frames.FieldTableName, frames.FieldColumns, frames.FieldRows}

func (d *DataTable) unmarshalAttr(m map[string]interface{}) error {
	for _, key := range dataTableFields {
		if _, exists := m[key]; !exists {
			return errors.ES(d.Op, errors.KInternal, "dataTable.%s was not present", key)
		}
	}

	if ft, ok := m[frames.FieldFrameType].(string); !ok {
		return errors.ES(d.Op, errors.KInternal, "dataTable.Unmarshal received data with no FrameType key")
	} else if ft != frames.TypeDataTable {
		return errors.ES(d.Op, errors.KInternal, "dataTable.Unmarshal received data with FrameType == %s", ft)
	} else {
		d.Base.FrameType = ft
	}

	jn, ok := m[frames.FieldTableID].(json.Number)
	if !ok {
		return errors.ES(d.Op, errors.KInternal, "dataTable.TableId was not a json.Number")
	}
	tblID, err := jn.Int64()
	if err != nil {
		return errors.ES(d.Op, errors.KInternal, "dataTable entry TableID was not an int64, was %s", m[frames.FieldFrameType].(json.Number).String())
	}
	d.TableID = int(tblID)

	if v, ok := m[frames.FieldTableKind].(string); ok {
		d.TableKind = frames.TableKind(v)
	} else {
		return errors.ES(d.Op, errors.KInternal, "dataTable.%s had non string entry, had type %T", frames.FieldTableKind, m[frames.FieldTableKind])
	}

	if v, ok := m[frames.FieldTableName].(string); ok {
		d.TableName = frames.TableKind(v)
	} else {
		return errors.ES(d.Op, errors.KInternal, "dataTable.%s had non string entry, had type %T", frames.FieldTableName, m[frames.FieldTableName])
	}

	if _, ok := m[frames.FieldColumns].([]interface{}); !ok {
		return errors.ES(d.Op, errors.KInternal, "dataTable.Columns had type %T, expected []interface{}", m[frames.FieldColumns])
	}
	return nil
}

func (d *DataTable) unmarshalCols(m map[string]interface{}) error {
	for _, inter := range m[frames.FieldColumns].([]interface{}) {
		m := inter.(map[string]interface{})
		for _, name := range []string{frames.FieldColumnName, frames.FieldColumnType} {
			if _, exists := m[name]; !exists {
				return errors.ES(d.Op, errors.KInternal, "dataTable.Columns had entry without .%s", name)
			}
		}
		cn, ok := m[frames.FieldColumnName].(string)
		if !ok {
			return errors.ES(d.Op, errors.KInternal, "dataTable.Columns had entry with .ColumnName set to a %T type", m[frames.FieldColumnName])
		}
		cts, ok := m[frames.FieldColumnType].(string)
		ct := types.Column(cts)
		if !ok || !ct.Valid() {
			return errors.ES(d.Op, errors.KInternal, "dataTable.Columns had entry with .ColumnType set to a %T type", m[frames.FieldColumnType])
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
		return errors.ES(d.Op, errors.KInternal, "dataTable.Rows had type %T, expected []interface{}", m[frames.FieldRows])
	}

	for x, inter := range m[frames.FieldRows].([]interface{}) {
		if _, ok := inter.([]interface{}); !ok {
			if err := errors.OneToErr(inter.(map[string]interface{}), d.Op); err != nil {
				return err
			}
			return errors.ES(d.Op, errors.KInternal, "dataTable.Rows had entry(%d) of type %T, expected []interface{}", x, inter)
		}
		var newRow value.Values
		for i, inner := range inter.([]interface{}) {
			f := frames.Conversion[d.Columns[i].Type]
			if f == nil {
				return errors.ES(d.Op, errors.KInternal, "in row %d, column %s: had unsupported type %s ", x, d.Columns[i].Name, d.Columns[i].Type)
			}
			inter, err := f(inner)
			if err != nil {
				return errors.ES(d.Op, errors.KInternal, "in row %d, column %s, conversion error: %s", x, d.Columns[i].Name, err.Error())
			}
			newRow = append(newRow, inter)
		}
		d.Rows = append(d.Rows, newRow)
	}
	return nil
}

func (DataTable) IsFrame() {}

// DataSetCompletion indicates the stream id done. It implements Frame.
type DataSetCompletion struct {
	Base
	// HasErrors indicates that their was an error in the stream.
	HasErrors bool
	// Cancelled indicates that the request was cancelled.
	Cancelled bool
	// OneAPIErrors is a list of errors encountered.
	OneAPIErrors []string `json:"OneApiErrors"`

	Op errors.Op `json:"-"`
}

func (DataSetCompletion) IsFrame() {}

// Unmarshal unmarshals a JSON decoded map value that represents a DataSetCompletion.
func (d *DataSetCompletion) Unmarshal(m map[string]interface{}) (err error) {
	const (
		oneAPIKey = "OneApiErrors"
		frameType = "FrameType" // This const duplicates FieldFrameType, but adds clarity in this code segment
		hasErrors = "HasErrors"
		cancelled = "Cancelled"
	)

	for _, name := range []string{frameType, hasErrors, cancelled} {
		if _, ok := m[name]; !ok {
			return errors.ES(d.Op, errors.KInternal, "DataSetCompletion.%s did not exist", name)
		}
	}
	var ok bool

	d.Base.FrameType, ok = m[frameType].(string)
	if !ok {
		return errors.ES(d.Op, errors.KInternal, "DataSetCompletion.%s was a %T, expected string", frameType, m[frameType])
	}
	if d.FrameType != frames.TypeDataSetCompletion {
		return errors.ES(d.Op, errors.KInternal, "DataSetCompletion.FrameType was set to %s", d.FrameType)
	}

	d.HasErrors, ok = m[hasErrors].(bool)
	if !ok {
		return errors.ES(d.Op, errors.KInternal, "DataSetCompletion.%s was a %T, expected string", hasErrors, m[hasErrors])
	}

	d.Cancelled, ok = m[cancelled].(bool)
	if !ok {
		return errors.ES(d.Op, errors.KTimeout, "DataSetCompletion.%s was a %T, expected string", cancelled, m[cancelled])
	}

	if _, ok := m[oneAPIKey]; ok {
		errList, ok := m[oneAPIKey].([]interface{})
		if !ok {
			return errors.ES(d.Op, errors.KInternal, "DataSetCompletion.OneApiErrors was expected to be []interface{}, was %T", m[oneAPIKey])
		}
		for _, entry := range errList {
			str, ok := entry.(string)
			if !ok {
				return errors.ES(d.Op, errors.KInternal, "DataSetCompletion.OneApiErrors had non-string type entry(%v)", entry)
			}
			d.OneAPIErrors = append(d.OneAPIErrors, str)
		}
	}
	return nil
}

// TableHeader indicates that instead of receiving a dataTable, we will receive a
// stream of table information. This structure holds the base information, but none
// of the row information.
type TableHeader struct {
	Base
	// TableID is a numeric representation of the this TableHeader in the stream.
	TableID int `json:"TableId"`
	// TableKind is a Kusto Table sub-type.
	TableKind frames.TableKind
	// TableName is a name for the Table.
	TableName frames.TableKind
	// Columns is a list of column names and their Kusto storage types.
	Columns table.Columns

	Op errors.Op `json:"-"`
}

func (TableHeader) IsFrame() {}

// Unmarshal unmarshals a JSON decoded map value that represents a TableHeader.
func (t *TableHeader) Unmarshal(m map[string]interface{}) (err error) {
	for _, key := range []string{frames.FieldTableID, frames.FieldTableKind, frames.FieldTableName,
		frames.FieldFrameType, frames.FieldColumns} {
		if _, exists := m[key]; !exists {
			return errors.ES(t.Op, errors.KInternal, "TableHeader.%s was not present", key)
		}
	}

	if ft, ok := m[frames.FieldFrameType].(string); !ok {
		return errors.ES(t.Op, errors.KInternal, "TableHeader.Unmarshal received data with no FrameType key")
	} else if ft != frames.TypeTableHeader {
		return errors.ES(t.Op, errors.KInternal, "TableHeader.Unmarshal received data with FrameType == %s", ft)
	} else {
		t.Base.FrameType = ft
	}

	jn, ok := m[frames.FieldTableID].(json.Number)
	if !ok {
		return errors.ES(t.Op, errors.KInternal, "TableHeader.TableId was not a json.Number")
	}
	tblID, err := jn.Int64()
	if err != nil {
		return errors.ES(t.Op, errors.KInternal, "TableHeader entry TableID was not an int64, was %s", m[frames.FieldFrameType].(json.Number).String())
	}
	t.TableID = int(tblID)

	if v, ok := m[frames.FieldTableKind].(string); ok {
		t.TableKind = frames.TableKind(v)
	} else {
		return errors.ES(t.Op, errors.KInternal, "TableHeader.%s had non string entry, had type %T", frames.FieldTableKind, m[frames.FieldTableKind])
	}

	if v, ok := m[frames.FieldTableName].(string); ok {
		t.TableName = frames.TableKind(v)
	} else {
		return errors.ES(t.Op, errors.KInternal, "TableHeader.%s had non string entry, had type %T", frames.FieldTableName, m[frames.FieldTableName])
	}

	if _, ok := m[frames.FieldColumns].([]interface{}); !ok {
		return errors.ES(t.Op, errors.KInternal, "TableHeader.Columns had type %T, expected []interface{}", m[frames.FieldColumns])
	}

	for _, inter := range m[frames.FieldColumns].([]interface{}) {
		m := inter.(map[string]interface{})
		for _, name := range []string{frames.FieldColumnName, frames.FieldColumnType} {
			if _, exists := m[name]; !exists {
				return errors.ES(t.Op, errors.KInternal, "TableHeader.Columns had entry without .%s", name)
			}
		}
		cn, ok := m[frames.FieldColumnName].(string)
		if !ok {
			return errors.ES(t.Op, errors.KInternal, "TableHeader.Columns had entry with .ColumnName set to a %T type", m[frames.FieldColumnName])
		}
		cts, ok := m[frames.FieldColumnType].(string)
		if !ok {
			return errors.ES(t.Op, errors.KInternal, "TableHeader.Columns had entry with .ColumnType set to a %T type", m[frames.FieldColumnType])
		}
		ct := types.Column(cts)
		if !ct.Valid() {
			return errors.ES(t.Op, errors.KInternal, "TableHeader.Columns had entry with .ColumnType set to a %T type", m[frames.FieldColumnType])
		}

		col := table.Column{
			Name: cn,
			Type: ct,
		}
		t.Columns = append(t.Columns, col)
	}
	return nil
}

const (
	TFDataAppend  = "DataAppend"
	TFDataReplace = "DataReplace"
)

// TableFragment details the streaming data passed by server that would normally be the Row data in
type TableFragment struct {
	Base
	// TableID is a numeric representation of the this table in relation to other table parts returned.
	TableID int `json:"TableId"`
	// FieldCount is the number of  fields being returned. This should align with the len(TableHeader.Columns).
	FieldCount int
	// TableFragment type is the type of TFDataAppend or TFDataReplace.
	TableFragmentType string
	// Rows contains the the table data that was fetched.
	Rows []value.Values

	Columns table.Columns `json:"-"` // Needed for decoding values.

	Op errors.Op `json:"-"`
}

func (TableFragment) IsFrame() {}

// Unmarshal unmarshals a JSON decoded map value that represents a TableFragment.
func (t *TableFragment) Unmarshal(m map[string]interface{}) (err error) {
	for _, key := range []string{frames.FieldFrameType, frames.FieldTableID, frames.FieldTableFragmentType, frames.FieldRows} {
		if _, exists := m[key]; !exists {
			return errors.ES(t.Op, errors.KInternal, "TableFragment.%s was not present", key)
		}
	}

	if ft, ok := m[frames.FieldFrameType].(string); !ok {
		return errors.ES(t.Op, errors.KInternal, "TableFragment.Unmarshal received data with no FrameType key")
	} else if ft != frames.TypeTableFragment {
		return errors.ES(t.Op, errors.KInternal, "TableFragment.Unmarshal received data with FrameType == %s", ft)
	} else {
		t.Base.FrameType = ft
	}

	jn, ok := m[frames.FieldTableID].(json.Number)
	if !ok {
		return errors.ES(t.Op, errors.KInternal, "TableFragment.TableId was not a json.Number")
	}
	tblID, err := jn.Int64()
	if err != nil {
		return errors.ES(t.Op, errors.KInternal, "TableFragment entry TableID was not an int64, was %s", m[frames.FieldFrameType].(json.Number).String())
	}
	t.TableID = int(tblID)

	// FieldCount is not always present.
	if fc, ok := m[frames.FieldCount]; ok {
		jn, ok = fc.(json.Number)
		if !ok {
			return errors.ES(t.Op, errors.KInternal, "TableFragment.FieldCount was not a json.Number")
		}
	}

	fieldCount, err := jn.Int64()
	if err != nil {
		return errors.ES(t.Op, errors.KInternal, "TableFragment entry FieldCount was not an int64, was %s", m[frames.FieldCount].(json.Number).String())
	}
	t.FieldCount = int(fieldCount)

	if v, ok := m[frames.FieldTableFragmentType].(string); ok {
		t.TableFragmentType = v
	} else {
		return errors.ES(t.Op, errors.KInternal, "TableFragment.%s had non string entry, had type %T", frames.FieldTableFragmentType, m[frames.FieldTableFragmentType])
	}

	if _, ok := m[frames.FieldRows].([]interface{}); !ok {
		return errors.ES(t.Op, errors.KInternal, "TableFragment.Rows had type %T, expected []interface{}", m[frames.FieldRows])
	}

	for x, inter := range m[frames.FieldRows].([]interface{}) {
		if _, ok := inter.([]interface{}); !ok {
			return errors.ES(t.Op, errors.KInternal, "TableFragment.Rows had entry(%d) of type %T, expected []interface{}", x, inter)
		}
		newRow, err := t.rowConversion(inter.([]interface{}))
		if err != nil {
			return errors.ES(t.Op, errors.KInternal, "in row %d: %s", x, err.Error())
		}
		t.Rows = append(t.Rows, newRow)
	}
	return nil
}

func (t *TableFragment) rowConversion(in []interface{}) (value.Values, error) {
	var newRow value.Values
	for i, inner := range in {
		f := frames.Conversion[t.Columns[i].Type]
		if f == nil {
			return nil, fmt.Errorf("column %s: had unsupported type %s ", t.Columns[i].Name, t.Columns[i].Type)
		}
		inter, err := f(inner)
		if err != nil {
			return nil, fmt.Errorf("column %s, conversion error: %s", t.Columns[i].Name, err)
		}
		newRow = append(newRow, inter)
	}
	return newRow, nil
}

// TableProgress interleaves with the TableFragment frame described above. It's sole purpose
// is to notify the client about the query progress.
type TableProgress struct {
	Base
	// TableID is a numeric representation of the this table in relation to other table parts returned.
	TableID int `json:"TableId"`
	// TableProgress is the progress in percent (0--100).
	TableProgress float64

	Op errors.Op `json:"-"`
}

func (TableProgress) IsFrame() {}

// Unmarshal unmarshals a JSON decoded map value that represents a TableProgress.
func (t *TableProgress) Unmarshal(m map[string]interface{}) (err error) {
	for _, key := range []string{frames.FieldFrameType, frames.FieldTableID, frames.FieldTableProgress} {
		if _, exists := m[key]; !exists {
			return errors.ES(t.Op, errors.KInternal, "TableProgress.%s was not present", key)
		}
	}

	if ft, ok := m[frames.FieldFrameType].(string); !ok {
		return errors.ES(t.Op, errors.KInternal, "TableProgress.Unmarshal received data with no FrameType key")
	} else if ft != frames.TypeTableProgress {
		return errors.ES(t.Op, errors.KInternal, "TableProgress.Unmarshal received data with FrameType == %s", ft)
	} else {
		t.Base.FrameType = ft
	}

	jn, ok := m[frames.FieldTableID].(json.Number)
	if !ok {
		return errors.ES(t.Op, errors.KInternal, "TableProgress.TableId was not a json.Number")
	}
	tblID, err := jn.Int64()
	if err != nil {
		return errors.ES(t.Op, errors.KInternal, "TableProgress entry TableID was not an int64, was %s", m[frames.FieldFrameType].(json.Number).String())
	}
	t.TableID = int(tblID)

	jn, ok = m[frames.FieldTableProgress].(json.Number)
	if !ok {

		return errors.ES(t.Op, errors.KInternal, "TableProgress.TableProgress was not a json.Number")
	}
	progress, err := jn.Float64()
	if err != nil {
		return errors.ES(t.Op, errors.KInternal, "TableProgress.TableProgress was not an int64, was %s", m[frames.FieldTableProgress].(json.Number).String())
	}
	t.TableProgress = progress

	return nil
}

// TableCompletion frames marks the end of the table transmission. No more frames related to that table will be sent.
type TableCompletion struct {
	Base
	// TableID is a numeric representation of the this table in relation to other table parts returned.
	TableID int `json:"TableId"`
	// RowCount is the final number of rows in the table.
	RowCount int

	Op errors.Op `json:"-"`
}

func (TableCompletion) IsFrame() {}

// Unmarshal unmarshals a JSON decoded map value that represents a TableCompletion.
func (t *TableCompletion) Unmarshal(m map[string]interface{}) (err error) {
	for _, key := range []string{frames.FieldFrameType, frames.FieldTableID, frames.FieldRowCount} {
		if _, exists := m[key]; !exists {
			return errors.ES(t.Op, errors.KInternal, "TableCompletion.%s was not present", key)
		}
	}

	if ft, ok := m[frames.FieldFrameType].(string); !ok {
		return errors.ES(t.Op, errors.KInternal, "TableCompletion.Unmarshal received data with no FrameType key")
	} else if ft != frames.TypeTableCompletion {
		return errors.ES(t.Op, errors.KInternal, "TableCompletion.Unmarshal received data with FrameType == %s", ft)
	} else {
		t.Base.FrameType = ft
	}

	jn, ok := m[frames.FieldTableID].(json.Number)
	if !ok {
		return errors.ES(t.Op, errors.KInternal, "TableCompletion.TableId was not a json.Number")
	}
	tblID, err := jn.Int64()
	if err != nil {
		return errors.ES(t.Op, errors.KInternal, "TableCompletion entry TableID was not an int64, was %s", m[frames.FieldFrameType].(json.Number).String())
	}
	t.TableID = int(tblID)

	jn, ok = m[frames.FieldRowCount].(json.Number)
	if !ok {
		return errors.ES(t.Op, errors.KInternal, "TableCompletion.RowCount was not a json.Number")
	}
	rc, err := jn.Int64()
	if err != nil {
		return errors.ES(t.Op, errors.KInternal, "TableCompletion entry RowCount was not an int64, was %s", m[frames.FieldRowCount].(json.Number).String())
	}
	t.RowCount = int(rc)

	return nil
}
