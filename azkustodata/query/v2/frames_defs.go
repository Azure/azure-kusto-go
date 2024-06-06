package v2

import (
	"bytes"
	"encoding/json"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
)

type FrameColumn struct {
	ColumnName string `json:"ColumnName"`
	ColumnType string `json:"ColumnType"`
}

type FrameType string

// UnmarshalJSON implements the json.Unmarshaler interface for QueryProperties.
func (q *ColumnsAndRows) UnmarshalJSON(b []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()

	cols := make([]FrameColumn, 0)

	err := decoder.Decode(&cols)
	if err != nil {
		return err
	}

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

func readRows(b []byte, decoder *json.Decoder, cols []FrameColumn) ([]value.Values, error) {
	var rows []value.Values

	err := assertToken(decoder, json.Delim('['))
	if err != nil {
		return nil, err
	}

	for decoder.More() {
		row := make(value.Values, 0, len(cols))
		err := unmarhsalRow(b, decoder, func(field int, t json.Token) error {
			col := types.NormalizeColumn(cols[field].ColumnType)
			kusto := value.Default(col)
			err := kusto.Unmarshal(t)
			if err != nil {
				return err
			}
			row = append(row, kusto)
			return nil
		})
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}

	if err := assertToken(decoder, json.Delim(']')); err != nil {
		return nil, err
	}
	return rows, nil
}

type ColumnsAndRows struct {
	Columns []FrameColumn
	Rows    []value.Values
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
	Columns   ColumnsAndRows
}

type QueryPropertiesDataTable struct {
	TableId   int
	TableKind string
	TableName string
	Rows      []QueryProperties
}

type QueryCompletionInformationDataTable struct {
	TableId   int
	TableKind string
	TableName string
	Rows      []QueryCompletionInformation
}

type TableHeader struct {
	TableId   int
	TableKind string
	TableName string
	Columns   []FrameColumn
}

type TableFragment struct {
	Columns []FrameColumn
	Rows    []value.Values
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
