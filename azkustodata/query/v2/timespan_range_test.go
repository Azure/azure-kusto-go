package v2

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A timespan that is valid in Kusto but does not fit into a time.Duration must be reported as an error,
// not returned as a wrapped-around value.
func TestStreamingDataSet_TimespanOutsideDurationRange(t *testing.T) {
	const response = `[{"FrameType":"DataSetHeader","IsProgressive":false,"Version":"v2.0","IsFragmented":true,"ErrorReportingPlacement":"EndOfTable"}
,{"FrameType":"DataTable","TableId":0,"TableKind":"QueryProperties","TableName":"@ExtendedProperties","Columns":[],"Rows":[]}
,{"FrameType":"TableHeader","TableId":1,"TableKind":"PrimaryResult","TableName":"PrimaryResult","Columns":[{"ColumnName":"Span","ColumnType":"timespan"}]}
,{"FrameType":"TableFragment","TableId":1,"Rows":[["1.02:03:04"],["3652058.00:00:00"]]}
,{"FrameType":"TableCompletion","TableId":1,"RowCount":2}
,{"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":false}
]
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dataset, err := NewIterativeDataset(ctx, io.NopCloser(strings.NewReader(response)), 1, 1, 1)
	require.NoError(t, err)
	defer dataset.Close()

	var spans []time.Duration
	var reportedErrors []error
	for result := range dataset.Tables() {
		if result.Err() != nil {
			reportedErrors = append(reportedErrors, result.Err())
			continue
		}
		table := result.Table()
		for row := range table.Rows() {
			if row.Err() != nil {
				reportedErrors = append(reportedErrors, row.Err())
				continue
			}
			if !table.IsPrimaryResult() || row.Row() == nil {
				continue
			}
			span, err := row.Row().TimespanByIndex(0)
			require.NoError(t, err)
			spans = append(spans, *span)
		}
	}

	for _, span := range spans {
		require.Equal(t, 26*time.Hour+3*time.Minute+4*time.Second, span, "only the in-range row may produce a value")
	}
	require.NotEmpty(t, reportedErrors)
	require.Contains(t, reportedErrors[0].Error(), "outside the range of time.Duration")
}
