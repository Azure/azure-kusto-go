package v2

import (
	"bytes"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/goccy/go-json"
)

type FrameColumn struct {
	ColumnIndex int    `json:"-"`
	ColumnName  string `json:"ColumnName"`
	ColumnType  string `json:"ColumnType"`
}

func (f FrameColumn) Index() int {
	return f.ColumnIndex
}

func (f FrameColumn) Name() string {
	return f.ColumnName
}

func (f FrameColumn) Type() types.Column {
	return types.Column(f.ColumnType)
}

type FrameType string

// UnmarshalJSON implements the json.Unmarshaler interface for QueryProperties.
func (q *DataTable) UnmarshalJSON(b []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()

	err := assertToken(decoder, json.Delim('{'))
	if err != nil {
		return err
	}

	err = assertStringProperty(decoder, "FrameType", string(DataTableFrameType))
	if err != nil {
		return err
	}

	q.TableId, err = getIntProperty(decoder, "TableId")
	if err != nil {
		return err
	}

	q.TableKind, err = getStringProperty(decoder, "TableKind")
	if err != nil {
		return err
	}

	q.TableName, err = getStringProperty(decoder, "TableName")
	if err != nil {
		return err
	}

	err = assertToken(decoder, json.Token("Columns"))
	if err != nil {
		return err
	}

	cols, err := decodeColumns(decoder)

	err = assertToken(decoder, json.Token("Rows"))
	if err != nil {
		return err
	}

	rows, err := readRows(b, decoder, cols)
	if err != nil {
		return err
	}

	q.Columns = cols
	q.Rows = rows
	return nil
}

func (t *TableHeader) UnmarshalJSON(b []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()

	err := assertToken(decoder, json.Delim('{'))
	if err != nil {
		return err
	}

	err = assertStringProperty(decoder, "FrameType", string(TableHeaderFrameType))
	if err != nil {
		return err
	}

	t.TableId, err = getIntProperty(decoder, "TableId")
	if err != nil {
		return err
	}

	t.TableKind, err = getStringProperty(decoder, "TableKind")
	if err != nil {
		return err
	}

	t.TableName, err = getStringProperty(decoder, "TableName")
	if err != nil {
		return err
	}

	err = assertToken(decoder, json.Token("Columns"))
	if err != nil {
		return err
	}

	t.Columns, err = decodeColumns(decoder)
	if err != nil {
		return err
	}

	return nil
}

func decodeColumns(decoder *json.Decoder) ([]query.Column, error) {
	cols := make([]query.Column, 0)

	err := assertToken(decoder, json.Delim('['))
	if err != nil {
		return nil, err
	}

	for i := 0; decoder.More(); i++ {
		col := FrameColumn{}
		err := decoder.Decode(&col)
		if err != nil {
			return nil, err
		}
		col.ColumnIndex = i
		col.ColumnType = string(types.NormalizeColumn(col.ColumnType))
		if col.ColumnType == "" {
			return nil, errors.ES(errors.OpTableAccess, errors.KClientArgs, "column[%d] is of type %s, which is not valid", i, col.ColumnType)
		}
		cols = append(cols, col)
	}

	if err := assertToken(decoder, json.Delim(']')); err != nil {
		return nil, err
	}

	return cols, nil
}

func (t *TableFragment) UnmarshalJSON(b []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()

	for {
		tok, err := decoder.Token()
		if err != nil {
			return err
		}
		if tok == json.Token("Rows") {
			break
		}
	}

	rows, err := readRows(b, decoder, t.Columns)
	if err != nil {
		return err
	}

	t.Rows = rows
	return nil
}

func readRows(b []byte, decoder *json.Decoder, cols []query.Column) ([]query.Row, error) {
	var rows = make([]query.Row, 0, 1000)

	columnsByName := make(map[string]query.Column, len(cols))
	for _, c := range cols {
		columnsByName[c.Name()] = c
	}

	err := assertToken(decoder, json.Delim('['))
	if err != nil {
		return nil, err
	}

	for i := 0; decoder.More(); i++ {
		values := make([]value.Kusto, 0, len(cols))
		err := unmarhsalRow(b, decoder, func(field int, t json.Token) error {
			kusto := value.Default(cols[field].Type())
			err := kusto.Unmarshal(t)
			if err != nil {
				return err
			}
			values = append(values, kusto)
			return nil
		})
		if err != nil {
			return nil, err
		}

		row := query.NewRowFromParts(cols, func(name string) query.Column { return columnsByName[name] }, i, values)
		rows = append(rows, row)
	}

	if err := assertToken(decoder, json.Delim(']')); err != nil {
		return nil, err
	}
	return rows, nil
}

const (
	DataSetHeaderFrameType     FrameType = "DataSetHeader"
	DataTableFrameType         FrameType = "DataTable"
	TableHeaderFrameType       FrameType = "TableHeader"
	TableFragmentFrameType     FrameType = "TableFragment"
	TableCompletionFrameType   FrameType = "TableCompletion"
	DataSetCompletionFrameType FrameType = "DataSetCompletion"
	TableProgressFrameType     FrameType = "TableProgress"
)

type DataSetHeader struct {
	IsProgressive           bool
	Version                 string
	IsFragmented            bool
	ErrorReportingPlacement string
}

type DataTable struct {
	TableId   int
	TableKind string
	TableName string
	Columns   []query.Column
	Rows      []query.Row
}

type TableHeader struct {
	TableId   int
	TableKind string
	TableName string
	Columns   []query.Column
}

type TableFragment struct {
	Columns []query.Column
	Rows    []query.Row
}

type TableCompletion struct {
	TableId      int
	RowCount     int
	OneApiErrors []OneApiError
}

type DataSetCompletion struct {
	HasErrors    bool
	Cancelled    bool
	OneApiErrors []OneApiError
}

type TableProgress struct {
	TableId       int
	TableProgress float64
}
