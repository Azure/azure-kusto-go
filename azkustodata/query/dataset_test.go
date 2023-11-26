package query

import (
	"context"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestDataSet_ReadFrames_WithError(t *testing.T) {
	reader := strings.NewReader("invalid")
	d := &DataSet{
		reader:       reader,
		frames:       make(chan Frame, DefaultFrameCapacity),
		errorChannel: make(chan error, 1),
		tables:       make(chan TableResult, 1),
		ctx:          context.Background(),
	}
	go d.ReadFrames()

	err := <-d.errorChannel
	assert.Error(t, err)
}

func TestDataSet_DecodeTables_WithInvalidFrame(t *testing.T) {
	reader := strings.NewReader(`[{"FrameType": "InvalidFrameType"}]`)
	d := NewDataSet(context.Background(), reader, DefaultFrameCapacity)

	tableResult := <-d.tables
	assert.Nil(t, tableResult.Table)
}

func TestDataSet_DecodeTables_WithValidFrames(t *testing.T) {
	reader := strings.NewReader(strings.TrimSpace(validFrames))
	d := NewDataSet(context.Background(), reader, DefaultFrameCapacity)

	for tableResult := range d.tables {
		assert.NoError(t, tableResult.Err)
		if tableResult.Table != nil {
			if t, ok := tableResult.Table.(StreamingTable); ok {
				t.SkipToEnd()
			}
		}
	}
}

func TestDataSet_DecodeTables_WithInvalidDataSetHeader(t *testing.T) {
	reader := strings.NewReader(`[{"FrameType": "DataSetHeader", "Version": "V1"}]`)
	d := NewDataSet(context.Background(), reader, DefaultFrameCapacity)

	tableResult := <-d.tables
	assert.Error(t, tableResult.Err)
	assert.Contains(t, tableResult.Err.Error(), "received a DataSetHeader frame that is not version 2")
}

func TestDataSet_DecodeTables_WithInvalidTableFragment(t *testing.T) {
	reader := strings.NewReader(`[{"FrameType": "TableFragment", "TableId": 1}]`)
	d := NewDataSet(context.Background(), reader, DefaultFrameCapacity)

	tableResult := <-d.tables
	assert.Error(t, tableResult.Err)
	assert.Contains(t, tableResult.Err.Error(), "received a TableFragment frame while no streaming table was open")
}

func TestDataSet_DecodeTables_WithInvalidTableCompletion(t *testing.T) {
	reader := strings.NewReader(`[{"FrameType": "TableCompletion", "TableId": 1}]`)
	d := NewDataSet(context.Background(), reader, DefaultFrameCapacity)

	tableResult := <-d.tables
	assert.Error(t, tableResult.Err)
	assert.Contains(t, tableResult.Err.Error(), "received a TableCompletion frame while no streaming table was open")
}
