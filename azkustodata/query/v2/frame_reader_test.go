package v2

import (
	"bytes"
	"context"
	_ "embed"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

//go:embed testData/validFrames.json
var validFrames string

//go:embed testData/aliases.json
var aliases string

//go:embed testData/partialError.json
var partialErrors string

//go:embed testData/twoTables.json
var twoTables string

//go:embed testData/error.txt
var errorText string

func TestDecodeValidFrames(t *testing.T) {
	reader := bytes.NewReader([]byte(validFrames))
	f, err := newFrameReader(io.NopCloser(reader), context.Background())
	require.NoError(t, err)
	require.NotNil(t, f)

	expected := []string{`{"FrameType":"DataSetHeader","IsProgressive":false,"Version":"v2.0","IsFragmented":true,"ErrorReportingPlacement":"EndOfTable"}`,
		`{"FrameType":"DataTable","TableId":0,"TableKind":"QueryProperties","TableName":"@ExtendedProperties","Columns":[{"ColumnName":"TableId","ColumnType":"int"},{"ColumnName":"Key","ColumnType":"string"},{"ColumnName":"Value","ColumnType":"dynamic"}],"Rows":[[1,"Visualization","{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\",\"Xmin\":null,\"Xmax\":null}"]]}`,
		`{"FrameType":"TableHeader","TableId":1,"TableKind":"PrimaryResult","TableName":"AllDataTypes","Columns":[{"ColumnName":"vnum","ColumnType":"int"},{"ColumnName":"vdec","ColumnType":"decimal"},{"ColumnName":"vdate","ColumnType":"datetime"},{"ColumnName":"vspan","ColumnType":"timespan"},{"ColumnName":"vobj","ColumnType":"dynamic"},{"ColumnName":"vb","ColumnType":"bool"},{"ColumnName":"vreal","ColumnType":"real"},{"ColumnName":"vstr","ColumnType":"string"},{"ColumnName":"vlong","ColumnType":"long"},{"ColumnName":"vguid","ColumnType":"guid"}]}`,
		`{"FrameType":"TableFragment","TableFragmentType":"DataAppend","TableId":1,"Rows":[[1,"2.00000000000001","2020-03-04T14:05:01.3109965Z","01:23:45.6789000",{"moshe":"value"},true,0.01,"asdf",9223372036854775807,"123e27de-1e4e-49d9-b579-fe0b331d3642"],[null,null,null,null,null,null,null,"",null,null]]}`,
		`{"FrameType":"TableCompletion","TableId":1,"RowCount":2}`,
		`{"FrameType":"DataTable","TableId":2,"TableKind":"QueryCompletionInformation","TableName":"QueryCompletionInformation","Columns":[{"ColumnName":"Timestamp","ColumnType":"datetime"},{"ColumnName":"ClientRequestId","ColumnType":"string"},{"ColumnName":"ActivityId","ColumnType":"guid"},{"ColumnName":"SubActivityId","ColumnType":"guid"},{"ColumnName":"ParentActivityId","ColumnType":"guid"},{"ColumnName":"Level","ColumnType":"int"},{"ColumnName":"LevelName","ColumnType":"string"},{"ColumnName":"StatusCode","ColumnType":"int"},{"ColumnName":"StatusCodeName","ColumnType":"string"},{"ColumnName":"EventType","ColumnType":"int"},{"ColumnName":"EventTypeName","ColumnType":"string"},{"ColumnName":"Payload","ColumnType":"string"}],"Rows":[["2023-11-26T13:34:17.0731478Z","blab6","123e27de-1e4e-49d9-b579-fe0b331d3642","123e27de-1e4e-49d9-b579-fe0b331d3642","123e27de-1e4e-49d9-b579-fe0b331d3642",4,"Info",0,"S_OK (0)",4,"QueryInfo","{\"Count\":1,\"Text\":\"Query completed successfully\"}"],["2023-11-26T13:34:17.0731478Z","blab6","123e27de-1e4e-49d9-b579-fe0b331d3642","123e27de-1e4e-49d9-b579-fe0b331d3642","123e27de-1e4e-49d9-b579-fe0b331d3642",4,"Info",0,"S_OK (0)",5,"WorkloadGroup","{\"Count\":1,\"Text\":\"default\"}"]]}`,
		`{"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":false}`}

	for _, e := range expected {
		line, err := f.advance()
		require.NoError(t, err)
		require.Equal(t, e, string(line))
	}
}

func TestInvalidJsonEmptyLine(t *testing.T) {
	reader := bytes.NewReader([]byte("[{}\n\n"))
	f, err := newFrameReader(io.NopCloser(reader), context.Background())
	require.NoError(t, err)
	require.NotNil(t, f)

	line, err := f.advance()
	require.Equal(t, "{}", string(line))
	require.NoError(t, err)

	line, err = f.advance()
	require.ErrorContains(t, err, "EOF")
	require.Nil(t, line)
}

func TestInvalidJsonInvalidDelimiter(t *testing.T) {
	reader := bytes.NewReader([]byte("[{}\n;{}\n]"))
	f, err := newFrameReader(io.NopCloser(reader), context.Background())
	require.NoError(t, err)
	require.NotNil(t, f)

	line, err := f.advance()
	require.Equal(t, "{}", string(line))
	require.NoError(t, err)

	line, err = f.advance()
	require.ErrorContains(t, err, "got ';'")
	require.Nil(t, line)
}

func TestInvalidJson(t *testing.T) {
	reader := bytes.NewReader([]byte(errorText))
	f, err := newFrameReader(io.NopCloser(reader), context.Background())
	require.ErrorContains(t, err, "Bad request")
	require.Nil(t, f)
}
