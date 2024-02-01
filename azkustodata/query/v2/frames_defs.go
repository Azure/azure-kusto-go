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
	FrameTypeJson               FrameType     `json:"FrameType"`
	IsProgressiveJson           bool          `json:"IsProgressive"`
	VersionJson                 string        `json:"Version"`
	IsFragmentedJson            bool          `json:"IsFragmented"`
	ErrorReportingPlacementJson string        `json:"ErrorReportingPlacement"`
	TableIdJson                 int           `json:"TableId"`
	TableKindJson               string        `json:"TableKind"`
	TableNameJson               string        `json:"TableName"`
	ColumnsJson                 []FrameColumn `json:"Columns"`
	RowsJson                    RawRows       `json:"Rows"`
	TableFragmentTypeJson       string        `json:"TableFragmentType"`
	RowCountJson                int           `json:"RowCount"`
	OneApiErrorsJson            []OneApiError `json:"OneApiErrors"`
	HasErrorsJson               bool          `json:"HasErrors"`
	CancelledJson               bool          `json:"Cancelled"`
	TableProgressJson           float64       `json:"TableProgress"`
}

func (f *EveryFrame) FrameType() FrameType            { return f.FrameTypeJson }
func (f *EveryFrame) IsProgressive() bool             { return f.IsProgressiveJson }
func (f *EveryFrame) Version() string                 { return f.VersionJson }
func (f *EveryFrame) IsFragmented() bool              { return f.IsFragmentedJson }
func (f *EveryFrame) ErrorReportingPlacement() string { return f.ErrorReportingPlacementJson }
func (f *EveryFrame) TableId() int                    { return f.TableIdJson }
func (f *EveryFrame) TableKind() string               { return f.TableKindJson }
func (f *EveryFrame) TableName() string               { return f.TableNameJson }
func (f *EveryFrame) Columns() []FrameColumn          { return f.ColumnsJson }
func (f *EveryFrame) Rows() RawRows                   { return f.RowsJson }
func (f *EveryFrame) TableFragmentType() string       { return f.TableFragmentTypeJson }
func (f *EveryFrame) RowCount() int                   { return f.RowCountJson }
func (f *EveryFrame) OneApiErrors() []OneApiError     { return f.OneApiErrorsJson }
func (f *EveryFrame) HasErrors() bool                 { return f.HasErrorsJson }
func (f *EveryFrame) Cancelled() bool                 { return f.CancelledJson }
func (f *EveryFrame) TableProgress() float64          { return f.TableProgressJson }

func (f *EveryFrame) AsDataSetHeader() DataSetHeader {
	if f.FrameTypeJson == DataSetHeaderFrameType {
		return f
	} else {
		return nil
	}
}
func (f *EveryFrame) AsDataTable() DataTable {
	if f.FrameTypeJson == DataTableFrameType {
		return f
	} else {
		return nil
	}
}
func (f *EveryFrame) AsTableHeader() TableHeader {
	if f.FrameTypeJson == TableHeaderFrameType {
		return f
	} else {
		return nil
	}
}
func (f *EveryFrame) AsTableFragment() TableFragment {
	if f.FrameTypeJson == TableFragmentFrameType {
		return f
	} else {
		return nil
	}
}
func (f *EveryFrame) AsTableCompletion() TableCompletion {
	if f.FrameTypeJson == TableCompletionFrameType {
		return f
	} else {
		return nil
	}
}
func (f *EveryFrame) AsDataSetCompletion() DataSetCompletion {
	if f.FrameTypeJson == DataSetCompletionFrameType {
		return f
	} else {
		return nil
	}
}
func (f *EveryFrame) AsTableProgress() TableProgress {
	if f.FrameTypeJson == TableProgressFrameType {
		return f
	} else {
		return nil
	}
}
