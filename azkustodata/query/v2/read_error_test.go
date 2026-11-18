package v2

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStreamingDataSet_ReadErrorAfterTableCompletion(t *testing.T) {
	const prefix = `[{"FrameType":"DataSetHeader","IsProgressive":false,"Version":"v2.0","IsFragmented":true,"ErrorReportingPlacement":"EndOfTable"}
,{"FrameType":"DataTable","TableId":0,"TableKind":"QueryProperties","TableName":"@ExtendedProperties","Columns":[],"Rows":[]}
,{"FrameType":"TableHeader","TableId":1,"TableKind":"PrimaryResult","TableName":"PrimaryResult","Columns":[{"ColumnName":"Value","ColumnType":"string"}]}
,{"FrameType":"TableFragment","TableId":1,"Rows":[["example"]]}
`
	const tableEnd = `,{"FrameType":"TableCompletion","TableId":1,"RowCount":1}
`
	const datasetEnd = `,{"FrameType":"DataSetCompletion","HasErrors":false,"Cancelled":false}
]
`
	readErr := errors.New("connection lost")
	for _, tt := range []struct {
		name     string
		response string
		fail     bool
	}{
		{"complete", prefix + tableEnd + datasetEnd, false},
		{"failure during table", prefix, true},
		{"failure after table", prefix + tableEnd, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var reader io.Reader = strings.NewReader(tt.response)
			if tt.fail {
				reader = io.MultiReader(reader, iotest.ErrReader(readErr))
			}
			dataset, err := NewIterativeDataset(ctx, io.NopCloser(reader), 1, 1, 1)
			require.NoError(t, err)
			defer dataset.Close()
			rows := 0
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
					} else if table.IsPrimaryResult() && row.Row() != nil {
						rows++
					}
				}
			}
			require.Equal(t, 1, rows)
			if tt.fail {
				require.NotEmpty(t, reportedErrors)
				for _, err := range reportedErrors {
					require.ErrorIs(t, err, readErr)
				}
			} else {
				require.Empty(t, reportedErrors)
			}
		})
	}
}
