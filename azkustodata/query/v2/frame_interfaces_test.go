package v2

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDataSetHeader_String(t *testing.T) {
	t.Parallel()
	header := &DataSetHeader{
		IsProgressive:           true,
		Version:                 "V2",
		IsFragmented:            false,
		ErrorReportingPlacement: "Start",
	}
	expected := "DataSetHeader(IsProgressive=true, Version=V2, IsFragmented=false, ErrorReportingPlacement=Start)"
	assert.Equal(t, expected, header.String())
}

func TestDataTable_String(t *testing.T) {
	t.Parallel()
	table := &DataTable{
		TableId:   1,
		TableKind: "TestKind",
		TableName: "TestName",
		Columns:   []FrameColumn{{ColumnName: "TestColumn", ColumnType: "TestType"}},
		Rows:      [][]interface{}{{"TestValue"}},
	}
	expected := "DataTable(TableId=1, TableKind=TestKind, TableName=TestName, Columns=[{TestColumn TestType}], Rows=[[TestValue]])"
	assert.Equal(t, expected, table.String())
}

func TestFrameColumn_String(t *testing.T) {
	t.Parallel()
	column := &FrameColumn{
		ColumnName: "TestColumn",
		ColumnType: "TestType",
	}
	expected := "FrameColumn(ColumnName=TestColumn, ColumnType=TestType)"
	assert.Equal(t, expected, column.String())
}

func TestTableHeader_String(t *testing.T) {
	t.Parallel()
	header := &TableHeader{
		TableId:   1,
		TableKind: "TestKind",
		TableName: "TestName",
		Columns:   []FrameColumn{{ColumnName: "TestColumn", ColumnType: "TestType"}},
	}
	expected := "TableHeader(TableId=1, TableKind=TestKind, TableName=TestName, Columns=[{TestColumn TestType}])"
	assert.Equal(t, expected, header.String())
}

func TestTableFragment_String(t *testing.T) {
	t.Parallel()
	fragment := &TableFragment{
		TableFragmentType: "TestType",
		TableId:           1,
		Rows:              [][]interface{}{{"TestValue"}},
	}
	expected := "TableFragment(TableFragmentType=TestType, TableId=1, Rows=[[TestValue]])"
	assert.Equal(t, expected, fragment.String())
}

func TestTableCompletion_String(t *testing.T) {
	t.Parallel()
	completion := &TableCompletion{
		TableId:  1,
		RowCount: 1,
		OneApiErrors: []OneApiError{
			{
				ErrorMessage: ErrorMessage{
					Code:    "TestCode",
					Message: "TestMessage",
					Type:    "TestType",
					Context: ErrorContext{
						Timestamp:        "TestTimestamp",
						ServiceAlias:     "TestServiceAlias",
						MachineName:      "TestMachineName",
						ProcessName:      "TestProcessName",
						ProcessId:        1,
						ThreadId:         1,
						ClientRequestId:  "TestClientRequestId",
						ActivityId:       "TestActivityId",
						SubActivityId:    "TestSubActivityId",
						ActivityType:     "TestActivityType",
						ParentActivityId: "TestParentActivityId",
						ActivityStack:    "TestActivityStack",
					},
					IsPermanent: false,
				},
			},
		},
	}
	expected := "TableCompletion(TableId=1, RowCount=1, OneApiErrors=[]v2.OneApiError{v2.OneApiError{ErrorMessage:v2.ErrorMessage{Code:\"TestCode\", Message:\"TestMessage\", Type:\"TestType\", Context:v2.ErrorContext{Timestamp:\"TestTimestamp\", ServiceAlias:\"TestServiceAlias\", MachineName:\"TestMachineName\", ProcessName:\"TestProcessName\", ProcessId:1, ThreadId:1, ClientRequestId:\"TestClientRequestId\", ActivityId:\"TestActivityId\", SubActivityId:\"TestSubActivityId\", ActivityType:\"TestActivityType\", ParentActivityId:\"TestParentActivityId\", ActivityStack:\"TestActivityStack\"}, IsPermanent:false}}})"
	assert.Equal(t, expected, completion.String())
}

func TestDataSetCompletion_String(t *testing.T) {
	t.Parallel()
	completion := &DataSetCompletion{
		HasErrors: true,
		Cancelled: false,
		OneApiErrors: []OneApiError{
			{
				ErrorMessage: ErrorMessage{
					Code:    "TestCode",
					Message: "TestMessage",
					Type:    "TestType",
					Context: ErrorContext{
						Timestamp:        "TestTimestamp",
						ServiceAlias:     "TestServiceAlias",
						MachineName:      "TestMachineName",
						ProcessName:      "TestProcessName",
						ProcessId:        1,
						ThreadId:         1,
						ClientRequestId:  "TestClientRequestId",
						ActivityId:       "TestActivityId",
						SubActivityId:    "TestSubActivityId",
						ActivityType:     "TestActivityType",
						ParentActivityId: "TestParentActivityId",
						ActivityStack:    "TestActivityStack",
					},
					IsPermanent: false,
				},
			},
		},
	}
	expected := "DataSetCompletion(HasErrors=true, Cancelled=false, OneApiErrors=[]v2.OneApiError{v2.OneApiError{ErrorMessage:v2.ErrorMessage{Code:\"TestCode\", Message:\"TestMessage\", Type:\"TestType\", Context:v2.ErrorContext{Timestamp:\"TestTimestamp\", ServiceAlias:\"TestServiceAlias\", MachineName:\"TestMachineName\", ProcessName:\"TestProcessName\", ProcessId:1, ThreadId:1, ClientRequestId:\"TestClientRequestId\", ActivityId:\"TestActivityId\", SubActivityId:\"TestSubActivityId\", ActivityType:\"TestActivityType\", ParentActivityId:\"TestParentActivityId\", ActivityStack:\"TestActivityStack\"}, IsPermanent:false}}})"
	assert.Equal(t, expected, completion.String())
}

func TestDataSetHeader_GetFrameType(t *testing.T) {
	t.Parallel()
	header := &DataSetHeader{}
	assert.Equal(t, DataSetHeaderFrameType, header.GetFrameType())
}

func TestDataTable_GetFrameType(t *testing.T) {
	t.Parallel()
	table := &DataTable{}
	assert.Equal(t, DataTableFrameType, table.GetFrameType())
}

func TestTableHeader_GetFrameType(t *testing.T) {
	t.Parallel()
	header := &TableHeader{}
	assert.Equal(t, TableHeaderFrameType, header.GetFrameType())
}

func TestTableFragment_GetFrameType(t *testing.T) {
	t.Parallel()
	fragment := &TableFragment{}
	assert.Equal(t, TableFragmentFrameType, fragment.GetFrameType())
}

func TestTableCompletion_GetFrameType(t *testing.T) {
	t.Parallel()
	completion := &TableCompletion{}
	assert.Equal(t, TableCompletionFrameType, completion.GetFrameType())
}

func TestDataSetCompletion_GetFrameType(t *testing.T) {
	t.Parallel()
	completion := &DataSetCompletion{}
	assert.Equal(t, DataSetCompletionFrameType, completion.GetFrameType())
}