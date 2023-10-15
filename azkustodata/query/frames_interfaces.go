package query

import "fmt"

func (f *DataSetHeader) String() string {
	return fmt.Sprintf("DataSetHeader(IsProgressive=%t, Version=%s, IsFragmented=%t, ErrorReportingPlacement=%s)", f.IsProgressive, f.Version, f.IsFragmented, f.ErrorReportingPlacement)
}
func (f *DataTable) String() string {
	return fmt.Sprintf("DataTable(TableId=%d, TableKind=%s, TableName=%s, Columns=%v, Rows=%v)", f.TableId, f.TableKind, f.TableName, f.Columns, f.Rows)
}
func (c *FrameColumn) String() string {
	return fmt.Sprintf("FrameColumn(ColumnName=%s, ColumnType=%s)", c.ColumnName, c.ColumnType)
}

func (f *TableHeader) String() string {
	return fmt.Sprintf("TableHeader(TableId=%d, TableKind=%s, TableName=%s, Columns=%v)", f.TableId, f.TableKind, f.TableName, f.Columns)
}

func (f *TableFragment) String() string {
	return fmt.Sprintf("TableFragment(TableFragmentType=%s, TableId=%d, Rows=%v)", f.TableFragmentType, f.TableId, f.Rows)
}

func (f *TableCompletion) String() string {
	return fmt.Sprintf("TableCompletion(TableId=%d, RowCount=%d, OneApiErrors=%v)", f.TableId, f.RowCount, f.OneApiErrors)
}

func (e *OneApiError) String() string {
	return fmt.Sprintf("OneApiError(Error=%v)", e.Error)
}

func (e *ErrorMessage) String() string {
	return fmt.Sprintf("ErrorMessage(Code=%s, Message=%s, Type=%s, ErrorContext=%v, IsPermanent=%t)", e.Code, e.Message, e.Type, e.Context, e.IsPermanent)
}

func (e *ErrorContext) String() string {
	return fmt.Sprintf("ErrorContext(Timestamp=%s, ServiceAlias=%s, MachineName=%s, ProcessName=%s, ProcessId=%d, ThreadId=%d, ClientRequestId=%s, ActivityId=%s, SubActivityId=%s, ActivityType=%s, ParentActivityId=%s, ActivityStack=%s)", e.Timestamp, e.ServiceAlias, e.MachineName, e.ProcessName, e.ProcessId, e.ThreadId, e.ClientRequestId, e.ActivityId, e.SubActivityId, e.ActivityType, e.ParentActivityId, e.ActivityStack)
}

func (f *DataSetCompletion) String() string {
	return fmt.Sprintf("DataSetCompletion(HasErrors=%t, Cancelled=%t, OneApiErrors=%v)", f.HasErrors, f.Cancelled, f.OneApiErrors)
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
