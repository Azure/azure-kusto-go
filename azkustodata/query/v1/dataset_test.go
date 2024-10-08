package v1

import (
	"context"
	_ "embed"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"io"
	"strings"
	"testing"
	"time"
)

type firstTable struct {
	A int32 `kusto:"a"`
}

func TestDatasetSuccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		file string
	}{
		{name: "success", file: successFile},
		{name: "data type only", file: dataTypeOnlyFile},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reader := io.NopCloser(strings.NewReader(tt.file))
			ctx := context.Background()
			op := errors.OpQuery
			ds, err := NewDatasetFromReader(ctx, op, reader)
			assert.NoError(t, err)

			assert.NotNil(t, ds)

			assert.Equal(t, ctx, ds.Context())
			assert.Equal(t, op, ds.Op())

			expectedIndex := []TableIndexRow{
				{Ordinal: 0, Kind: "QueryResult", Name: "PrimaryResult", Id: "e43f725a-26fd-4219-8869-30c21e1b139c", PrettyName: ""},
				{Ordinal: 1, Kind: "QueryResult", Name: "PrimaryResult", Id: "0f66e92a-8d0e-43da-8a66-ddb6bf84c49d", PrettyName: ""},
				{Ordinal: 2, Kind: "QueryProperties", Name: "@ExtendedProperties", Id: "d52bc55b-fc74-4a63-adb9-b72ff939e4c2", PrettyName: ""},
				{Ordinal: 3, Kind: "QueryStatus", Name: "QueryStatus", Id: "00000000-0000-0000-0000-000000000000", PrettyName: ""},
			}
			assert.EqualValues(t, expectedIndex, ds.Index())

			expectedStatus := []QueryStatus{
				{
					Timestamp:         time.Date(2023, 12, 3, 13, 17, 49, 483295600, time.UTC),
					Severity:          4,
					SeverityName:      "Info",
					StatusCode:        0,
					StatusDescription: "Query completed successfully",
					Count:             1,
					RequestId:         uuid.MustParse("6b4c0ab2-180e-46d8-b97e-593e6aea1e7a"),
					ActivityId:        uuid.MustParse("6b4c0ab2-180e-46d8-b97e-593e6aea1e7a"),
					SubActivityId:     uuid.MustParse("2a41ff99-6429-418e-8bae-5cf703c5138a"),
					ClientActivityId:  "blab6",
				},
				{
					Timestamp:         time.Date(2023, 12, 3, 13, 17, 49, 483295600, time.UTC),
					Severity:          6,
					SeverityName:      "Stats",
					StatusCode:        0,
					StatusDescription: "{\"ExecutionTime\":0.0,\"resource_usage\":{\"cache\":{\"memory\":{\"hits\":0,\"misses\":0,\"total\":0},\"disk\":{\"hits\":0,\"misses\":0,\"total\":0},\"shards\":{\"hot\":{\"hitbytes\":0,\"missbytes\":0,\"retrievebytes\":0},\"cold\":{\"hitbytes\":0,\"missbytes\":0,\"retrievebytes\":0},\"bypassbytes\":0}},\"cpu\":{\"user\":\"00:00:00\",\"kernel\":\"00:00:00\",\"total cpu\":\"00:00:00\"},\"memory\":{\"peak_per_node\":524384},\"network\":{\"inter_cluster_total_bytes\":962,\"cross_cluster_total_bytes\":0}},\"input_dataset_statistics\":{\"extents\":{\"total\":0,\"scanned\":0,\"scanned_min_datetime\":\"0001-01-01T00:00:00.0000000Z\",\"scanned_max_datetime\":\"0001-01-01T00:00:00.0000000Z\"},\"rows\":{\"total\":0,\"scanned\":0},\"rowstores\":{\"scanned_rows\":0,\"scanned_values_size\":0},\"shards\":{\"queries_generic\":0,\"queries_specialized\":0}},\"dataset_statistics\":[{\"table_row_count\":3,\"table_size\":15},{\"table_row_count\":3,\"table_size\":43}],\"cross_cluster_resource_usage\":{}}",
					Count:             1,
					RequestId:         uuid.MustParse("6b4c0ab2-180e-46d8-b97e-593e6aea1e7a"),
					ActivityId:        uuid.MustParse("6b4c0ab2-180e-46d8-b97e-593e6aea1e7a"),
					SubActivityId:     uuid.MustParse("2a41ff99-6429-418e-8bae-5cf703c5138a"),
					ClientActivityId:  "blab6",
				},
			}
			assert.EqualValues(t, expectedStatus, ds.Status())

			expectedInfo := []QueryProperties{
				{Value: "{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\",\"Xmin\":null,\"Xmax\":null}"},
				{Value: "{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\",\"Xmin\":null,\"Xmax\":null}"},
			}
			assert.EqualValues(t, expectedInfo, ds.Info())

			table1Rows := ds.Tables()[0].Rows()
			expectedTable1 := []firstTable{
				{A: 1},
				{A: 2},
				{A: 3},
			}

			table1, errs := query.ToStructs[firstTable](table1Rows)

			assert.Nil(t, errs)

			assert.EqualValues(t, expectedTable1, table1)

			type secondTable struct {
				A string `kusto:"a"`
				B int32  `kusto:"b"`
			}

			table2Rows := ds.Tables()[1].Rows()
			expectedTable2Rows := []secondTable{
				{A: "a", B: 1},
				{A: "b", B: 2},
				{A: "c", B: 3},
			}

			table2, errs := query.ToStructs[secondTable](table2Rows)
			assert.Nil(t, errs)

			assert.EqualValues(t, expectedTable2Rows, table2)
		})
	}
}

func TestDatasetPartialErrors(t *testing.T) {
	t.Parallel()

	reader := io.NopCloser(strings.NewReader(partialErrorFile))
	ctx := context.Background()
	op := errors.OpQuery
	ds, err := NewDatasetFromReader(ctx, op, reader)
	assert.Nil(t, ds)
	assert.ErrorContains(t, err, "Query execution has exceeded the allowed limits")
}

func TestBoolAsInt(t *testing.T) {
	t.Parallel()

	reader := io.NopCloser(strings.NewReader(booleanIntFile))
	ctx := context.Background()
	op := errors.OpQuery
	ds, err := NewDatasetFromReader(ctx, op, reader)
	assert.NoError(t, err)

	rows := ds.Tables()[0].Rows()

	falseBool, err := rows[0].BoolByIndex(0)
	assert.NoError(t, err)
	assert.Equal(t, *falseBool, false)

	falseInt, err := rows[1].BoolByIndex(0)
	assert.NoError(t, err)
	assert.Equal(t, *falseInt, false)

	trueBool, err := rows[2].BoolByIndex(0)
	assert.NoError(t, err)
	assert.Equal(t, *trueBool, true)

	trueInt, err := rows[3].BoolByIndex(0)
	assert.NoError(t, err)
	assert.Equal(t, *trueInt, true)

	nullBool, err := rows[4].BoolByIndex(0)
	assert.NoError(t, err)
	assert.Nil(t, nullBool)
}
