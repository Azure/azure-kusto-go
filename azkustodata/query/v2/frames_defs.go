package v2

// This file contains the raw JSON frames that are received from the Kusto service.

type RawRow struct {
	Row    []interface{}
	Errors []OneApiError
}

func NewRawRow(items ...interface{}) RawRow {
	return RawRow{Row: items}
}

type FrameColumn struct {
	ColumnName string `json:"ColumnName"`
	ColumnType string `json:"ColumnType"`
}

type FrameType string

const (
	DataSetHeaderFrameType     FrameType = "DataSetHeader"
	DataTableFrameType         FrameType = "DataTable"
	TableHeaderFrameType       FrameType = "TableHeader"
	TableFragmentFrameType     FrameType = "TableFragment"
	TableCompletionFrameType   FrameType = "TableCompletion"
	DataSetCompletionFrameType FrameType = "DataSetCompletion"
	TableProgressFrameType     FrameType = "TableProgress"
)

type DataSetHeader interface {
	IsProgressive() bool
	Version() string
	IsFragmented() bool
	ErrorReportingPlacement() string
}

type DataTable interface {
	TableId() int
	TableKind() string
	TableName() string
	Columns() []FrameColumn
	Rows() RawRows
}

type TableHeader interface {
	TableId() int
	TableKind() string
	TableName() string
	Columns() []FrameColumn
}

type TableFragment interface {
	TableFragmentType() string
	TableId() int
	Rows() RawRows
}

type TableCompletion interface {
	TableId() int
	RowCount() int
	OneApiErrors() []OneApiError
}

type DataSetCompletion interface {
	HasErrors() bool
	Cancelled() bool
	OneApiErrors() []OneApiError
}

type TableProgress interface {
	TableId() int
	TableProgress() float64
}

type RawRows []RawRow

type EveryFrame struct {
	frameType               FrameType     `json:"FrameType"`
	isProgressive           bool          `json:"IsProgressive"`
	version                 string        `json:"Version"`
	isFragmented            bool          `json:"IsFragmented"`
	errorReportingPlacement string        `json:"ErrorReportingPlacement"`
	tableId                 int           `json:"TableId"`
	tableKind               string        `json:"TableKind"`
	tableName               string        `json:"TableName"`
	columns                 []FrameColumn `json:"Columns"`
	rows                    RawRows       `json:"Rows"`
	tableFragmentType       string        `json:"TableFragmentType"`
	rowCount                int           `json:"RowCount"`
	oneApiErrors            []OneApiError `json:"OneApiErrors"`
	hasErrors               bool          `json:"HasErrors"`
	cancelled               bool          `json:"Cancelled"`
	tableProgress           float64       `json:"TableProgress"`
}

func (f *EveryFrame) FrameType() FrameType            { return f.frameType }
func (f *EveryFrame) IsProgressive() bool             { return f.isProgressive }
func (f *EveryFrame) Version() string                 { return f.version }
func (f *EveryFrame) IsFragmented() bool              { return f.isFragmented }
func (f *EveryFrame) ErrorReportingPlacement() string { return f.errorReportingPlacement }
func (f *EveryFrame) TableId() int                    { return f.tableId }
func (f *EveryFrame) TableKind() string               { return f.tableKind }
func (f *EveryFrame) TableName() string               { return f.tableName }
func (f *EveryFrame) Columns() []FrameColumn          { return f.columns }
func (f *EveryFrame) Rows() RawRows                   { return f.rows }
func (f *EveryFrame) TableFragmentType() string       { return f.tableFragmentType }
func (f *EveryFrame) RowCount() int                   { return f.rowCount }
func (f *EveryFrame) OneApiErrors() []OneApiError     { return f.oneApiErrors }
func (f *EveryFrame) HasErrors() bool                 { return f.hasErrors }
func (f *EveryFrame) Cancelled() bool                 { return f.cancelled }
func (f *EveryFrame) TableProgress() float64          { return f.tableProgress }

func (f *EveryFrame) AsDataSetHeader() DataSetHeader {
	if f.frameType == DataSetHeaderFrameType {
		return f
	} else {
		return nil
	}
}
func (f *EveryFrame) AsDataTable() DataTable {
	if f.frameType == DataTableFrameType {
		return f
	} else {
		return nil
	}
}
func (f *EveryFrame) AsTableHeader() TableHeader {
	if f.frameType == TableHeaderFrameType {
		return f
	} else {
		return nil
	}
}
func (f *EveryFrame) AsTableFragment() TableFragment {
	if f.frameType == TableFragmentFrameType {
		return f
	} else {
		return nil
	}
}
func (f *EveryFrame) AsTableCompletion() TableCompletion {
	if f.frameType == TableCompletionFrameType {
		return f
	} else {
		return nil
	}
}
func (f *EveryFrame) AsDataSetCompletion() DataSetCompletion {
	if f.frameType == DataSetCompletionFrameType {
		return f
	} else {
		return nil
	}
}
func (f *EveryFrame) AsTableProgress() TableProgress {
	if f.frameType == TableProgressFrameType {
		return f
	} else {
		return nil
	}
}
