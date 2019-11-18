package azkustodata

import (
	"encoding/json"
	"fmt"

	"azure-kusto-go/azure-kusto-data/azkustodata/errors"
	"azure-kusto-go/azure-kusto-data/azkustodata/types"
)

const (
	// ftDataTable is the .FrameType that indicates a Kusto DataTable.
	ftDataTable = "DataTable"
	// ftDataSetCompletion is the .FrameType that indicates a Kusto DataSetCompletion.
	ftDataSetCompletion = "DataSetCompletion"
	// ftDataSetHeader is the .FrameType that indicates a Kusto DataSetHeader.
	ftDataSetHeader = "DataSetHeader"
	// ftTableHeader is the .FrameType that indicates a Kusto TableHeader.
	ftTableHeader = "TableHeader"
	// ftTableFragment is the .FrameType that indicates a Kusto TableFragment.
	ftTableFragment = "TableFragment"
	// ftTableProgress is the .FrameType that indicates a Kusto TableProgress.
	ftTableProgress = "TableProgress"
	// ftTableCompletion is the .FrameType that indicates a Kusto TableCompletion.
	ftTableCompletion = "TableCompletion"
)

// These constants represent keys for fields when unmarshalling various JSON dicts representing Kusto frames.
const (
	kFrameType         = "FrameType"
	kTableID           = "TableId"
	kTableKind         = "TableKind"
	kTableName         = "TableName"
	kColumns           = "Columns"
	kColumnName        = "ColumnName"
	kColumnType        = "ColumnType"
	kRows              = "Rows"
	kFieldCount        = "FieldCount"
	kTableFragmentType = "TableFragmentType"
	kTableProgress     = "TableProgress"
	kRowCount          = "RowCount"
)

// frame is a type of Kusto frame as defined in the reference document.
type frame interface {
	isFrame()
}

// errorFrame is not actually a Kusto frame, but is used to signal the end of a stream
// where we encountered an error. errorFrame implements error.
type errorFrame struct {
	Msg string
}

// Error implements error.Error().
func (e errorFrame) Error() string {
	return e.Msg
}

func (errorFrame) isFrame() {}

// baseFrame is information that is encoded in all frames. The fields aren't actually
// in the spec, but are transmitted on the wire.
type baseFrame struct {
	FrameType string
}

// dataSetHeader is the first frame in a response. It implements Frame.
type dataSetHeader struct {
	baseFrame
	// Version is the version of the APi responding. The current version is "v2.0".
	Version string
	// IsProgressive indicates that tableHeader, TableFragment, TableProgress, and TableCompletion
	IsProgressive bool

	op errors.Op
}

func (dataSetHeader) isFrame() {}

const (
	// tkQueryProperties is a dataTable.TableKind that contains properties about the query itself.
	// The dataTable.TableName is usually tnExtendedProperties.
	tkQueryProperties = "QueryProperties"
	// tkPrimaryResult is a dataTable.TableKind that contains the query information the user wants.
	// The dataTable.TableName is tnPrimaryResult.
	tkPrimaryResult = "PrimaryResult"
	// tkQueryCompletionInformation contains information on how long the query took.
	// The dataTable.TableName is tnQueryCompletionInformation.
	tkQueryCompletionInformation = "QueryCompletionInformation"
	tkQueryTraceLog              = "QueryTraceLog"
	tkQueryPerfLog               = "QueryPerfLog"
	tkTableOfContents            = "TableOfContents"
	tkQueryPlan                  = "QueryPlan"
	tkUnknown                    = "Unknown"
)

var tkDetection = map[string]bool{
	tkQueryProperties:            true,
	tkPrimaryResult:              true,
	tkQueryCompletionInformation: true,
	tkQueryTraceLog:              true,
	tkQueryPerfLog:               true,
	tkTableOfContents:            true,
	tkQueryPlan:                  true,
	tkUnknown:                    true,
}

const (
	// tnExtendedProperties is a dataTable.TableName associated with a TableKind of tkQueryProperties.
	tnExtendedProperties = "@ExtendedProperties"
	// tnPrimaryResult is a dataTable.TableName associated with a TableKind of tkPrimaryResult.
	tnPrimaryResult = "PrimaryResult" // tkPrimaryResult
	// tnQueryCompletionInformation is a dataTable.TableName associated with a TableKind of tkQueryCompletionInformation.
	tnQueryCompletionInformation = "QueryCompletionInformation" // tkQueryCompletionInformation
)



// dataSetCompletion indicates the stream id done. It implements Frame.
type dataSetCompletion struct {
	baseFrame
	// HasErrors indicates that their was an error in the stream.
	HasErrors bool
	// Cancelled indicates that the request was cancelledt.
	Cancelled bool
	// OneAPIErrors is a list of errors encounteredt.
	OneAPIErrors []string `json:"OneApiErrors"`

	op errors.Op
}

func (dataSetCompletion) isFrame() {}

// Unmarshal unmarshals a JSON decoded map value that represents a dataSetCompletion.
func (dsc *dataSetCompletion) Unmarshal(m map[string]interface{}) (err error) {
	const (
		oneAPIKey = "OneApiErrors"
		frameType = "FrameType"
		hasErrors = "HasErrors"
		cancelled = "Cancelled"
	)

	for _, name := range []string{frameType, hasErrors, cancelled} {
		if _, ok := m[name]; !ok {
			return errors.E(dsc.op, errors.KInternal, fmt.Errorf("dataSetCompletion.%s did not exist", name))
		}
	}
	var ok bool

	dsc.baseFrame.FrameType, ok = m[frameType].(string)
	if !ok {
		return errors.E(dsc.op, errors.KInternal, fmt.Errorf("dataSetCompletion.%s was a %T, expected string", frameType, m[frameType]))
	}
	if dsc.FrameType != ftDataSetCompletion {
		return errors.E(dsc.op, errors.KInternal, fmt.Errorf("dataSetCompletion.FrameType was set to %s", dsc.FrameType))
	}

	dsc.HasErrors, ok = m[hasErrors].(bool)
	if !ok {
		return errors.E(dsc.op, errors.KInternal, fmt.Errorf("dataSetCompletion.%s was a %T, expected string", hasErrors, m[hasErrors]))
	}

	dsc.Cancelled, ok = m[cancelled].(bool)
	if !ok {
		return errors.E(dsc.op, errors.KTimeout, fmt.Errorf("dataSetCompletion.%s was a %T, expected string", cancelled, m[cancelled]))
	}

	if _, ok := m[oneAPIKey]; ok {
		errList, ok := m[oneAPIKey].([]interface{})
		if !ok {
			return errors.E(dsc.op, errors.KInternal, fmt.Errorf("dataSetCompletion.OneApiErrors was expected to be []interface{}, was %T", m[oneAPIKey]))
		}
		for _, entry := range errList {
			str, ok := entry.(string)
			if !ok {
				return errors.E(dsc.op, errors.KInternal, fmt.Errorf("dataSetCompletion.OneApiErrors had non-string type entry(%v)", entry))
			}
			dsc.OneAPIErrors = append(dsc.OneAPIErrors, str)
		}
	}
	return nil
}

// tableHeader indicates that instead of receiving a dataTable, we will receive a
// stream of table information. This structure holds the base information, but none
// of the row information.
type tableHeader struct {
	baseFrame
	// TableID is a numeric representation of the this tableHeader in the stream.
	TableID int `json:"TableId"`
	// TableKind is a Kusto Table sub-type.
	TableKind string
	// TableName is a name for the Table.
	TableName string
	// Columns is a list of column names and their Kusto storage types.
	Columns Columns

	op errors.Op
}

func (tableHeader) isFrame() {}

// Unmarshal unmarshals a JSON decoded map value that represents a tableHeader.
func (t *tableHeader) Unmarshal(m map[string]interface{}) (err error) {
	for _, key := range []string{kTableID, kTableKind, kTableName, kFrameType, kColumns} {
		if _, exists := m[key]; !exists {
			return errors.E(t.op, errors.KInternal, fmt.Errorf("tableHeader.%s was not present", key))
		}
	}

	if ft, ok := m[kFrameType].(string); !ok {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("tableHeader.Unmarshal received data with no FrameType key"))
	} else if ft != ftTableHeader {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("tableHeader.Unmarshal received data with FrameType == %s", ft))
	} else {
		t.baseFrame.FrameType = ft
	}

	jn, ok := m[kTableID].(json.Number)
	if !ok {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("tableHeader.TableId was not a json.Number"))
	}
	tblID, err := jn.Int64()
	if err != nil {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("tableHeader entry TableID was not an int64, was %s", m[kFrameType].(json.Number).String()))
	}
	t.TableID = int(tblID)

	if v, ok := m[kTableKind].(string); ok {
		t.TableKind = v
	} else {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("tableHeader.%s had non string entry, had type %T", kTableKind, m[kTableKind]))
	}

	if v, ok := m[kTableName].(string); ok {
		t.TableName = v
	} else {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("tableHeader.%s had non string entry, had type %T", kTableName, m[kTableName]))
	}

	if _, ok := m[kColumns].([]interface{}); !ok {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("tableHeader.Columns had type %T, expected []interface{}", m[kColumns]))
	}

	for _, inter := range m[kColumns].([]interface{}) {
		m := inter.(map[string]interface{})
		for _, name := range []string{kColumnName, kColumnType} {
			if _, exists := m[name]; !exists {
				return errors.E(t.op, errors.KInternal, fmt.Errorf("tableHeader.Columns had entry without .%s", name))
			}
		}
		cn, ok := m[kColumnName].(string)
		if !ok {
			return errors.E(t.op, errors.KInternal, fmt.Errorf("tableHeader.Columns had entry with .ColumnName set to a %T type", m[kColumnName]))
		}
		ct, ok := m[kColumnType].(string)
		if !ok {
			return errors.E(t.op, errors.KInternal, fmt.Errorf("TaableHeader.Columns had entry with .ColumnType set to a %T type", m[kColumnType]))
		}
		col := Column{
			ColumnName: cn,
			ColumnType: ct,
		}
		t.Columns = append(t.Columns, col)
	}
	return nil
}

const (
	TFDataAppend  = "DataAppend"
	TFDataReplace = "DataReplace"
)

// tableFragment details the streaming data passed by server that would normally be the Row data in
type tableFragment struct {
	baseFrame
	// TableID is a numeric representation of the this table in relation to other table parts returnedt.
	TableID int `json:"TableId"`
	// FieldCount is the number of  fields being returnedt. This should align with the len(tableHeader.Columns).
	FieldCount int
	// TableFragment type is the type of TFDataAppend or TFDataReplace.
	TableFragmentType string
	// Rows contains the the table data that was fetchedt.
	Rows []types.KustoValues

	columns Columns // Needed for decoding values.
	op      errors.Op
}

func (tableFragment) isFrame() {}

// Unmarshal unmarshals a JSON decoded map value that represents a TableFragment.
func (t *tableFragment) Unmarshal(m map[string]interface{}) (err error) {
	for _, key := range []string{kFrameType, kTableID, kTableFragmentType, kRows} {
		if _, exists := m[key]; !exists {
			return errors.E(t.op, errors.KInternal, fmt.Errorf("TableFragment.%s was not present", key))
		}
	}

	if ft, ok := m[kFrameType].(string); !ok {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableFragment.Unmarshal received data with no FrameType key"))
	} else if ft != ftTableFragment {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableFragment.Unmarshal received data with FrameType == %s", ft))
	} else {
		t.baseFrame.FrameType = ft
	}

	jn, ok := m[kTableID].(json.Number)
	if !ok {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableFragment.TableId was not a json.Number"))
	}
	tblID, err := jn.Int64()
	if err != nil {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableFragment entry TableID was not an int64, was %s", m[kFrameType].(json.Number).String()))
	}
	t.TableID = int(tblID)

	// FieldCount is not always present.
	if fc, ok := m[kFieldCount]; ok {
		jn, ok = fc.(json.Number)
		if !ok {
			return errors.E(t.op, errors.KInternal, fmt.Errorf("TableFragment.FieldCount was not a json.Number"))
		}
	}

	fieldCount, err := jn.Int64()
	if err != nil {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableFragment entry FieldCount was not an int64, was %s", m[kFieldCount].(json.Number).String()))
	}
	t.FieldCount = int(fieldCount)

	if v, ok := m[kTableFragmentType].(string); ok {
		t.TableFragmentType = v
	} else {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableFragment.%s had non string entry, had type %T", kTableFragmentType, m[kTableFragmentType]))
	}

	if _, ok := m[kRows].([]interface{}); !ok {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableFragment.Rows had type %T, expected []interface{}", m[kRows]))
	}

	for x, inter := range m[kRows].([]interface{}) {
		if _, ok := inter.([]interface{}); !ok {
			return errors.E(t.op, errors.KInternal, fmt.Errorf("TableFragment.Rows had entry(%d) of type %T, expected []interface{}", x, inter))
		}
		newRow, err := t.rowConversion(inter.([]interface{}))
		if err != nil {
			return errors.ES(t.op, errors.KInternal, "in row %d: %s", x, err)
		}
		t.Rows = append(t.Rows, newRow)
	}
	return nil
}

func (t *tableFragment) rowConversion(in []interface{}) (types.KustoValues, error) {
	var newRow types.KustoValues
	for i, inner := range in {
		f := conversion[t.columns[i].ColumnType]
		if f == nil {
			return nil, fmt.Errorf("column %s: had unsupported type %s ", t.columns[i].ColumnName, t.columns[i].ColumnType)
		}
		inter, err := f(inner)
		if err != nil {
			return nil, fmt.Errorf("column %s, conversion error: %s", t.columns[i].ColumnName, err)
		}
		newRow = append(newRow, inter)
	}
	return newRow, nil
}

// tableProgress interleaves with the TableFragment frame described above. It's sole purpose
// is to notify the client about the query progress.
type tableProgress struct {
	baseFrame
	// TableID is a numeric representation of the this table in relation to other table parts returnedt.
	TableID int `json:"TableId"`
	// TableProgress is the progress in percent (0--100).
	TableProgress float64

	op errors.Op
}

func (tableProgress) isFrame() {}

// Unmarshal unmarshals a JSON decoded map value that represents a TableProgress.
func (t *tableProgress) Unmarshal(m map[string]interface{}) (err error) {
	for _, key := range []string{kFrameType, kTableID, kTableProgress} {
		if _, exists := m[key]; !exists {
			return errors.E(t.op, errors.KInternal, fmt.Errorf("TableProgress.%s was not present", key))
		}
	}

	if ft, ok := m[kFrameType].(string); !ok {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableProgress.Unmarshal received data with no FrameType key"))
	} else if ft != ftTableProgress {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableProgress.Unmarshal received data with FrameType == %s", ft))
	} else {
		t.baseFrame.FrameType = ft
	}

	jn, ok := m[kTableID].(json.Number)
	if !ok {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableProgress.TableId was not a json.Number"))
	}
	tblID, err := jn.Int64()
	if err != nil {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableProgress entry TableID was not an int64, was %s", m[kFrameType].(json.Number).String()))
	}
	t.TableID = int(tblID)

	jn, ok = m[kTableProgress].(json.Number)
	if !ok {

		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableProgress.TableProgress was not a json.Number"))
	}
	progress, err := jn.Float64()
	if err != nil {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableProgress.TableProgress was not an int64, was %s", m[kTableProgress].(json.Number).String()))
	}
	t.TableProgress = progress

	return nil
}

// tableCompletion frames marks the end of the table transmission. No more frames related to that table will be sent.
type tableCompletion struct {
	baseFrame
	// TableID is a numeric representation of the this table in relation to other table parts returnedt.
	TableID int `json:"TableId"`
	// RowCount is the final number of rows in the table.
	RowCount int

	op errors.Op
}

func (tableCompletion) isFrame() {}

// Unmarshal unmarshals a JSON decoded map value that represents a TableCompletion.
func (t *tableCompletion) Unmarshal(m map[string]interface{}) (err error) {
	for _, key := range []string{kFrameType, kTableID, kRowCount} {
		if _, exists := m[key]; !exists {
			return errors.E(t.op, errors.KInternal, fmt.Errorf("TableCompletion.%s was not present", key))
		}
	}

	if ft, ok := m[kFrameType].(string); !ok {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableCompletion.Unmarshal received data with no FrameType key"))
	} else if ft != ftTableCompletion {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableCompletion.Unmarshal received data with FrameType == %s", ft))
	} else {
		t.baseFrame.FrameType = ft
	}

	jn, ok := m[kTableID].(json.Number)
	if !ok {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableCompletion.TableId was not a json.Number"))
	}
	tblID, err := jn.Int64()
	if err != nil {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableCompletion entry TableID was not an int64, was %s", m[kFrameType].(json.Number).String()))
	}
	t.TableID = int(tblID)

	jn, ok = m[kRowCount].(json.Number)
	if !ok {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableCompletion.RowCount was not a json.Number"))
	}
	rc, err := jn.Int64()
	if err != nil {
		return errors.E(t.op, errors.KInternal, fmt.Errorf("TableCompletion entry RowCount was not an int64, was %s", m[kRowCount].(json.Number).String()))
	}
	t.RowCount = int(rc)

	return nil
}
