package v2

import (
	"fmt"
)

type RawRow struct {
	Row    []interface{}
	Errors []OneApiError
}

func NewRawRow(items ...interface{}) RawRow {
	return RawRow{Row: items}
}

type Frame interface {
	fmt.Stringer
	GetFrameType() string
}

type FrameColumn struct {
	ColumnName string `json:"ColumnName"`
	ColumnType string `json:"ColumnType"`
}

const DataSetHeaderFrameType = "DataSetHeader"

type DataSetHeader struct {
	IsProgressive           bool   `json:"IsProgressive"`
	Version                 string `json:"Version"`
	IsFragmented            bool   `json:"IsFragmented"`
	ErrorReportingPlacement string `json:"ErrorReportingPlacement"`
}

const DataTableFrameType = "DataTable"

type RawRows []RawRow

type DataTable struct {
	TableId   int           `json:"TableId"`
	TableKind string        `json:"TableKind"`
	TableName string        `json:"TableName"`
	Columns   []FrameColumn `json:"Columns"`
	Rows      RawRows       `json:"Rows"`
}

const TableHeaderFrameType = "TableHeader"

type TableHeader struct {
	TableId   int           `json:"TableId"`
	TableKind string        `json:"TableKind"`
	TableName string        `json:"TableName"`
	Columns   []FrameColumn `json:"Columns"`
}

const TableFragmentFrameType = "TableFragment"

type TableFragment struct {
	TableFragmentType string  `json:"TableFragmentType"`
	TableId           int     `json:"TableId"`
	Rows              RawRows `json:"Rows"`
}

const TableCompletionFrameType = "TableCompletion"

type TableCompletion struct {
	TableId      int           `json:"TableId"`
	RowCount     int           `json:"RowCount"`
	OneApiErrors []OneApiError `json:"OneApiErrors"`
}

const DataSetCompletionFrameType = "DataSetCompletion"

type DataSetCompletion struct {
	HasErrors    bool          `json:"HasErrors"`
	Cancelled    bool          `json:"Cancelled"`
	OneApiErrors []OneApiError `json:"OneApiErrors"`
}

type EveryFrame struct {
	FrameType               string        `json:"FrameType"`
	IsProgressive           bool          `json:"IsProgressive"`
	Version                 string        `json:"Version"`
	IsFragmented            bool          `json:"IsFragmented"`
	ErrorReportingPlacement string        `json:"ErrorReportingPlacement"`
	TableId                 int           `json:"TableId"`
	TableKind               string        `json:"TableKind"`
	TableName               string        `json:"TableName"`
	Columns                 []FrameColumn `json:"Columns"`
	Rows                    RawRows       `json:"Rows"`
	TableFragmentType       string        `json:"TableFragmentType"`
	RowCount                int           `json:"RowCount"`
	OneApiErrors            []OneApiError `json:"OneApiErrors"`
	HasErrors               bool          `json:"HasErrors"`
	Cancelled               bool          `json:"Cancelled"`
}
