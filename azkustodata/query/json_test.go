package query

import (
	_ "embed"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed testData/validFrames.json
var validFrames string

func TestReadFramesWithValidInput(t *testing.T) {
	ch := make(chan Frame)

	var err error

	go func() {
		err = ReadFrames(strings.NewReader(strings.TrimSpace(validFrames)), ch)
		require.NoError(t, err)
	}()

	dataSetHeader := <-ch
	assert.Equal(t, &DataSetHeader{
		IsProgressive:           false,
		Version:                 "v2.0",
		IsFragmented:            true,
		ErrorReportingPlacement: "EndOfTable",
	}, dataSetHeader)

	dataTable := (<-ch).(*DataTable)
	assert.Equal(t, dataTable.TableId, 0)
	assert.Equal(t, dataTable.TableKind, "QueryProperties")
	assert.Equal(t, dataTable.TableName, "@ExtendedProperties")
	assert.Equal(t, dataTable.Columns, []FrameColumn{
		{"TableId", "int"},
		{"Key", "string"},
		{"Value", "dynamic"},
	})
	assert.Equal(t, len(dataTable.Rows), 1)

	tableHeader := (<-ch).(*TableHeader)
	assert.Equal(t, tableHeader.TableId, 1)
	assert.Equal(t, tableHeader.TableKind, "PrimaryResult")
	assert.Equal(t, tableHeader.TableName, "BigChunkus")
	assert.Equal(t, tableHeader.Columns, []FrameColumn{
		{"AvgTicketPrice", "real"},
		{"Cancelled", "bool"},
		{"Carrier", "string"},
		{"Dest", "string"},
		{"DestAirportID", "string"},
		{"DestCityName", "string"},
		{"DestCountry", "string"},
		{"DestLocation", "dynamic"},
		{"DestRegion", "string"},
		{"DestWeather", "string"},
		{"DistanceKilometers", "real"},
		{"DistanceMiles", "real"},
		{"FlightDelay", "bool"},
		{"FlightDelayMin", "long"},
		{"FlightDelayType", "string"},
		{"FlightNum", "string"},
		{"FlightTimeHour", "real"},
		{"FlightTimeMin", "real"},
		{"Origin", "string"},
		{"OriginAirportID", "string"},
		{"OriginCityName", "string"},
		{"OriginCountry", "string"},
		{"OriginLocation", "dynamic"},
		{"OriginRegion", "string"},
		{"OriginWeather", "string"},
		{"dayOfWeek", "int"},
		{"timestamp", "datetime"},
	})

	tableFragment := (<-ch).(*TableFragment)
	assert.Equal(t, tableFragment.TableFragmentType, "DataAppend")
	assert.Equal(t, tableFragment.TableId, 1)
	assert.Equal(t, len(tableFragment.Rows), 5)

	tableCompletion := (<-ch).(*TableCompletion)
	assert.Equal(t, tableCompletion.TableId, 1)
	assert.Equal(t, tableCompletion.RowCount, 5)
	assert.Equal(t, len(tableCompletion.OneApiErrors), 1)

	dataSetCompletion := (<-ch).(*DataSetCompletion)
	assert.Equal(t, dataSetCompletion.HasErrors, true)
	assert.Equal(t, dataSetCompletion.Cancelled, false)
	assert.Equal(t, len(dataSetCompletion.OneApiErrors), 1)

	assert.Nil(t, <-ch)

	require.NoError(t, err)
}

func TestReadFramesWithEmptyInput(t *testing.T) {
	src := ``

	ch := make(chan Frame)
	var err error

	go func() {
		err = ReadFrames(strings.NewReader(src), ch)
	}()

	for range ch {
		assert.Fail(t, "should not receive any frames")
	}

	require.NoError(t, err)
}

func TestReadFramesWithInvalidInput(t *testing.T) {
	src := `[{]`

	ch := make(chan Frame)
	var err error

	go func() {
		err = ReadFrames(strings.NewReader(src), ch)
	}()

	for range ch {
		assert.Fail(t, "should not receive any frames")
	}

	require.ErrorContains(t, err, "invalid character ']'")
}

func TestReadFramesWithInvalidFrameType(t *testing.T) {
	src := `[{"FrameType": "InvalidFrameType"}
]`

	ch := make(chan Frame)
	var err error

	go func() {
		err = ReadFrames(strings.NewReader(src), ch)
	}()

	invalid := <-ch
	assert.Nil(t, invalid)

	require.ErrorContains(t, err, "unknown frame type: InvalidFrameType")
}

func TestReadFramesWithInvalidFrame(t *testing.T) {
	src := `[{"FrameType": "DataSetHeader", "IsProgressive": "invalid"}
]`

	ch := make(chan Frame)
	var err error

	go func() {
		err = ReadFrames(strings.NewReader(src), ch)
	}()

	invalid := <-ch
	assert.Nil(t, invalid)

	require.ErrorContains(t, err, "json: cannot unmarshal string")
}
