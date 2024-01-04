package v2

import (
	"fmt"
)

// This file contains the boilerplate code for String() and GetFrameType() methods for all frames.

func (f *DataSetHeader) String() string {
	return fmt.Sprintf("DataSetHeader(IsProgressive=%t, Version=%s, IsFragmented=%t, ErrorReportingPlacement=%s)", f.IsProgressive, f.Version, f.IsFragmented, f.ErrorReportingPlacement)
}
func (f *DataTable) String() string {
	return fmt.Sprintf("DataTable(TableId=%d, TableKind=%s, TableName=%s, Columns=%v, Rows=%v)", f.TableId, f.TableKind, f.TableName, f.Columns, f.Rows)
}
func (c *FrameColumn) String() string {
	return fmt.Sprintf("FrameColumn(ColumnName=%s, ColumnType=%s)", c.ColumnName, c.ColumnType)
}

func (r *RawRow) String() string {
	return fmt.Sprintf("RawRow(Row=%v, Errors=%v)", r.Row, r.Errors)
}

func (f *TableHeader) String() string {
	return fmt.Sprintf("TableHeader(TableId=%d, TableKind=%s, TableName=%s, Columns=%v)", f.TableId, f.TableKind, f.TableName, f.Columns)
}

func (f *TableFragment) String() string {
	return fmt.Sprintf("TableFragment(TableFragmentType=%s, TableId=%d, Rows=%v)", f.TableFragmentType, f.TableId, f.Rows)
}

func (f *TableCompletion) String() string {
	return fmt.Sprintf("TableCompletion(TableId=%d, RowCount=%d, OneApiErrors=%#v)", f.TableId, f.RowCount, f.OneApiErrors)
}

func (f *DataSetCompletion) String() string {
	return fmt.Sprintf("DataSetCompletion(HasErrors=%t, Cancelled=%t, OneApiErrors=%#v)", f.HasErrors, f.Cancelled, f.OneApiErrors)
}

func (f *TableProgress) String() string {
	return fmt.Sprintf("TableProgress(TableId=%d, Progress=%d)", f.TableId, f.Progress)
}

func (f *DataSetHeader) GetFrameType() string {
	return DataSetHeaderFrameType
}

func (f *DataTable) GetFrameType() string {
	return DataTableFrameType
}

func (f *TableHeader) GetFrameType() string {
	return TableHeaderFrameType
}

func (f *TableFragment) GetFrameType() string {
	return TableFragmentFrameType
}

func (f *TableCompletion) GetFrameType() string {
	return TableCompletionFrameType
}

func (f *DataSetCompletion) GetFrameType() string {
	return DataSetCompletionFrameType
}

func (f *TableProgress) GetFrameType() string { return TableProgressFrameType }
