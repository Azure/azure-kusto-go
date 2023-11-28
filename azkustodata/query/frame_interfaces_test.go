package query

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDataSetHeader_String(t *testing.T) {
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
	column := &FrameColumn{
		ColumnName: "TestColumn",
		ColumnType: "TestType",
	}
	expected := "FrameColumn(ColumnName=TestColumn, ColumnType=TestType)"
	assert.Equal(t, expected, column.String())
}

func TestTableHeader_String(t *testing.T) {
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
	fragment := &TableFragment{
		TableFragmentType: "TestType",
		TableId:           1,
		Rows:              [][]interface{}{{"TestValue"}},
	}
	expected := "TableFragment(TableFragmentType=TestType, TableId=1, Rows=[[TestValue]])"
	assert.Equal(t, expected, fragment.String())
}

func TestTableCompletion_String(t *testing.T) {
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
	expected := "TableCompletion(TableId=1, RowCount=1, OneApiErrors=[]query.OneApiError{query.OneApiError{ErrorMessage:query.ErrorMessage{Code:\"TestCode\", Message:\"TestMessage\", Type:\"TestType\", Context:query.ErrorContext{Timestamp:\"TestTimestamp\", ServiceAlias:\"TestServiceAlias\", MachineName:\"TestMachineName\", ProcessName:\"TestProcessName\", ProcessId:1, ThreadId:1, ClientRequestId:\"TestClientRequestId\", ActivityId:\"TestActivityId\", SubActivityId:\"TestSubActivityId\", ActivityType:\"TestActivityType\", ParentActivityId:\"TestParentActivityId\", ActivityStack:\"TestActivityStack\"}, IsPermanent:false}}})"
	assert.Equal(t, expected, completion.String())
}

func TestDataSetCompletion_String(t *testing.T) {
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
	expected := "DataSetCompletion(HasErrors=true, Cancelled=false, OneApiErrors=[]query.OneApiError{query.OneApiError{ErrorMessage:query.ErrorMessage{Code:\"TestCode\", Message:\"TestMessage\", Type:\"TestType\", Context:query.ErrorContext{Timestamp:\"TestTimestamp\", ServiceAlias:\"TestServiceAlias\", MachineName:\"TestMachineName\", ProcessName:\"TestProcessName\", ProcessId:1, ThreadId:1, ClientRequestId:\"TestClientRequestId\", ActivityId:\"TestActivityId\", SubActivityId:\"TestSubActivityId\", ActivityType:\"TestActivityType\", ParentActivityId:\"TestParentActivityId\", ActivityStack:\"TestActivityStack\"}, IsPermanent:false}}})"
	assert.Equal(t, expected, completion.String())
}

func TestDataSetHeader_GetFrameType(t *testing.T) {
	header := &DataSetHeader{}
	assert.Equal(t, DataSetHeaderFrameType, header.GetFrameType())
}

func TestDataTable_GetFrameType(t *testing.T) {
	table := &DataTable{}
	assert.Equal(t, DataTableFrameType, table.GetFrameType())
}

func TestTableHeader_GetFrameType(t *testing.T) {
	header := &TableHeader{}
	assert.Equal(t, TableHeaderFrameType, header.GetFrameType())
}

func TestTableFragment_GetFrameType(t *testing.T) {
	fragment := &TableFragment{}
	assert.Equal(t, TableFragmentFrameType, fragment.GetFrameType())
}

func TestTableCompletion_GetFrameType(t *testing.T) {
	completion := &TableCompletion{}
	assert.Equal(t, TableCompletionFrameType, completion.GetFrameType())
}

func TestDataSetCompletion_GetFrameType(t *testing.T) {
	completion := &DataSetCompletion{}
	assert.Equal(t, DataSetCompletionFrameType, completion.GetFrameType())
}
