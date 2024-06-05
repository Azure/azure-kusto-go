package v2

import (
	"encoding/json"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"strings"
)

type FrameColumn struct {
	ColumnName string `json:"ColumnName"`
	ColumnType string `json:"ColumnType"`
}

type FrameType string

type KustoTable struct {
	Columns []FrameColumn
	Rows    value.Values
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

func (a *Animal) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	switch strings.ToLower(s) {
	default:
		*a = Unknown
	case "gopher":
		*a = Gopher
	case "zebra":
		*a = Zebra
	}

	return nil
}

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
	Columns   []FrameColumn
	Rows      KustoTable
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
	TableFragmentType string
	TableId           int
	Rows              value.Values
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
