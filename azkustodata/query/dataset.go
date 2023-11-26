package query

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"io"
)

const DefaultFrameCapacity = 5

const version = "v2.0"
const errorReportingPlacement = "EndOfTable"

type TableResult struct {
	Table Table
	Err   error
}

type DataSet struct {
	reader        io.Reader
	DataSetHeader *DataSetHeader
	Completion    *DataSetCompletion
	frames        chan Frame
	errorChannel  chan error

	tables chan TableResult

	currentStreamingTable *streamingTable
	ctx                   context.Context
}

func (d *DataSet) ReadFrames() {
	err := ReadFrames(d.reader, d.frames)
	if err != nil {
		d.errorChannel <- err
	}
}

func (d *DataSet) DecodeTables() {
	defer close(d.tables)
	for {
		var f Frame = nil

		select {
		case err := <-d.errorChannel:
			d.tables <- TableResult{Table: nil, Err: err}
			break
		case <-d.ctx.Done():
			d.tables <- TableResult{Table: nil, Err: errors.ES(errors.OpUnknown, errors.KInternal, "context cancelled")}
			break
		case fc, ok := <-d.frames:
			if ok {
				f = fc
			}
		}

		if f == nil {
			break
		}

		if d.Completion != nil {
			d.tables <- TableResult{Table: nil, Err: errors.ES(errors.OpUnknown, errors.KInternal, "received a frame after DataSetCompletion")}
			break
		}

		// Dataset Frames
		if header, ok := f.(*DataSetHeader); ok {
			d.DataSetHeader = header
			if d.DataSetHeader.Version != version {
				d.tables <- TableResult{Table: nil, Err: errors.ES(errors.OpUnknown, errors.KInternal, "received a DataSetHeader frame that is not version 2")}
				break
			}
			if !d.DataSetHeader.IsFragmented {
				d.tables <- TableResult{Table: nil, Err: errors.ES(errors.OpUnknown, errors.KInternal, "received a DataSetHeader frame that is not fragmented")}
				break
			}
			if d.DataSetHeader.IsProgressive {
				d.tables <- TableResult{Table: nil, Err: errors.ES(errors.OpUnknown, errors.KInternal, "received a DataSetHeader frame that is progressive")}
				break
			}
			const EndOfTableErrorPlacement = errorReportingPlacement
			if d.DataSetHeader.ErrorReportingPlacement != EndOfTableErrorPlacement {
				d.tables <- TableResult{Table: nil, Err: errors.ES(errors.OpUnknown, errors.KInternal, "received a DataSetHeader frame that does not report errors at the end of the table")}
				break
			}
		} else if completion, ok := f.(*DataSetCompletion); ok {
			d.Completion = completion
			// DataTable
		} else if dt, ok := f.(*DataTable); ok {
			t, err := NewFullTable(dt)
			d.tables <- TableResult{Table: t, Err: err}
			if err != nil {
				break
			}

			// Streaming Frames
		} else if th, ok := f.(*TableHeader); ok {
			if d.currentStreamingTable != nil {
				err := errors.ES(errors.OpUnknown, errors.KInternal, "received a TableHeader frame while a streaming table was still open")
				d.tables <- TableResult{Table: nil, Err: err}
				break
			}
			t, err := NewStreamingTable(d, th)
			if err != nil {
				d.tables <- TableResult{Table: nil, Err: err}
				break
			}
			d.currentStreamingTable = t.(*streamingTable)
			d.tables <- TableResult{Table: t, Err: nil}
		} else if tf, ok := f.(*TableFragment); ok {
			if d.currentStreamingTable == nil {
				err := errors.ES(errors.OpUnknown, errors.KInternal, "received a TableFragment frame while no streaming table was open")
				d.tables <- TableResult{Table: nil, Err: err}
				break
			}
			if d.currentStreamingTable.Id() != tf.TableId {
				err := errors.ES(errors.OpUnknown, errors.KInternal, "received a TableFragment frame for table %d while table %d was open", tf.TableId, d.currentStreamingTable.Id())
				d.tables <- TableResult{Table: nil, Err: err}
			}

			d.currentStreamingTable.rawRows <- tf.Rows
		} else if tc, ok := f.(*TableCompletion); ok {
			if d.currentStreamingTable == nil {
				err := errors.ES(errors.OpUnknown, errors.KInternal, "received a TableCompletion frame while no streaming table was open")
				d.tables <- TableResult{Table: nil, Err: err}
				break
			}
			if d.currentStreamingTable.Id() != tc.TableId {
				err := errors.ES(errors.OpUnknown, errors.KInternal, "received a TableCompletion frame for table %d while table %d was open", tc.TableId, d.currentStreamingTable.Id())
				d.tables <- TableResult{Table: nil, Err: err}
			}

			d.currentStreamingTable.close(tc.OneApiErrors)

			if d.currentStreamingTable.rowCount != tc.RowCount {
				err := errors.ES(errors.OpUnknown, errors.KInternal, "received a TableCompletion frame for table %d with row count %d while %d rows were received", tc.TableId, tc.RowCount, d.currentStreamingTable.rowCount)
				d.tables <- TableResult{Table: nil, Err: err}
			}

			d.currentStreamingTable = nil
		} else {
			err := errors.ES(errors.OpUnknown, errors.KInternal, "unknown frame type")
			d.tables <- TableResult{Table: nil, Err: err}
			break
		}
	}
}

func NewDataSet(ctx context.Context, r io.Reader, capacity int) *DataSet {
	d := &DataSet{
		reader:       r,
		frames:       make(chan Frame, capacity),
		errorChannel: make(chan error, 1),
		tables:       make(chan TableResult, 1),
		ctx:          ctx,
	}
	go d.ReadFrames()

	go d.DecodeTables()

	return d
}
