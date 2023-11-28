package query

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"io"
)

// DefaultFrameCapacity is the default capacity of the channel that receives frames from the Kusto service. Lower capacity means less memory usage, but might cause the channel to block if the frames are not consumed fast enough.
const DefaultFrameCapacity = 5

const version = "v2.0"
const errorReportingPlacement = "EndOfTable"

// TableResult is a structure that holds the result of a table operation.
// It contains a Table and an error, if any occurred during the operation.
type TableResult struct {
	// Table is the result of the operation.
	Table Table
	// Err is the error that occurred during the operation, if any.
	Err error
}

// DataSet is a structure that represents a set of data as returned by the Kusto service.
type DataSet struct {
	// reader is an io.ReadCloser used to read the data from the Kusto service.
	reader io.ReadCloser
	// ctx is the context of the data set, as received from the request.
	ctx context.Context
	// DataSetHeader is the header of the data set. It's the first frame received.
	DataSetHeader *DataSetHeader
	// Completion is the completion status of the data set. It's the last frame received.
	Completion *DataSetCompletion

	// queryProperties contains the information from the "QueryProperties" table.
	queryProperties []QueryProperties
	// queryCompletionInformation contains the information from the "QueryCompletionInformation" table.
	queryCompletionInformation []QueryCompletionInformation

	// SecondaryResults is all the non-primary tables, which are always saved.
	SecondaryResults []Table

	// frames is a channel that receives all the frames from the data set as they are parsed.
	frames chan Frame
	// errorChannel is a channel to report errors during the parsing of frames
	errorChannel chan error

	// tables is a channel that sends the parsed tables as they are decoded.
	tables chan TableResult

	// currentStreamingTable is a reference to the current streamed table, which is still receiving rows.
	currentStreamingTable *streamingTable
}

// op returns the operation of the data set.
func (d *DataSet) op() errors.Op {
	op := d.ctx.Value("op")
	if op == nil {
		return errors.OpUnknown
	}
	return op.(errors.Op)
}

// readFrames consumes the frames from the reader and sends them to the frames channel.
func (d *DataSet) readFrames() {
	err := ReadFrames(d.reader, d.frames)
	if err != nil {
		d.errorChannel <- err
	}

	err = d.reader.Close()
	if err != nil {
		d.errorChannel <- err
	}
}

// decodeTables decodes the frames from the frames channel and sends the tables to the tables channel.
func (d *DataSet) decodeTables() {
	defer func() {
		close(d.tables)
		if d.currentStreamingTable != nil {
			d.currentStreamingTable.close([]OneApiError{})
		}
	}()
	for {
		var f Frame = nil

		op := d.op()
		select {
		case err := <-d.errorChannel:
			d.tables <- TableResult{Table: nil, Err: err}
			break
		case <-d.ctx.Done():
			d.tables <- TableResult{Table: nil, Err: errors.ES(op, errors.KInternal, "context cancelled")}
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
			d.tables <- TableResult{Table: nil, Err: errors.ES(op, errors.KInternal, "received a frame after DataSetCompletion")}
			break
		}

		if header, ok := f.(*DataSetHeader); ok {
			if !d.parseDatasetHeader(header, op) {
				break
			}
		} else if completion, ok := f.(*DataSetCompletion); ok {
			d.Completion = completion
		} else if dt, ok := f.(*DataTable); ok {
			t, err := NewFullTable(d, dt)
			d.tables <- TableResult{Table: t, Err: err}
			if err != nil {
				break
			}
			if !t.IsPrimaryResult() {
				d.SecondaryResults = append(d.SecondaryResults, t)
			}
		} else if !d.parseStreamingTable(f, op) {
			break
		} else {
			err := errors.ES(op, errors.KInternal, "unknown frame type")
			d.tables <- TableResult{Table: nil, Err: err}
			break
		}
	}
}

func (d *DataSet) parseStreamingTable(f Frame, op errors.Op) bool {
	if th, ok := f.(*TableHeader); ok {
		if d.currentStreamingTable != nil {
			err := errors.ES(op, errors.KInternal, "received a TableHeader frame while a streaming table was still open")
			d.tables <- TableResult{Table: nil, Err: err}
			return false
		}
		t, err := NewStreamingTable(d, th)
		if err != nil {
			d.tables <- TableResult{Table: nil, Err: err}
			return false
		}
		d.currentStreamingTable = t.(*streamingTable)
		d.tables <- TableResult{Table: t, Err: nil}
	} else if tf, ok := f.(*TableFragment); ok {
		if d.currentStreamingTable == nil {
			err := errors.ES(op, errors.KInternal, "received a TableFragment frame while no streaming table was open")
			d.tables <- TableResult{Table: nil, Err: err}
			return false
		}
		if d.currentStreamingTable.Id() != tf.TableId {
			err := errors.ES(op, errors.KInternal, "received a TableFragment frame for table %d while table %d was open", tf.TableId, d.currentStreamingTable.Id())
			d.tables <- TableResult{Table: nil, Err: err}
		}

		d.currentStreamingTable.rawRows <- tf.Rows
	} else if tc, ok := f.(*TableCompletion); ok {
		if d.currentStreamingTable == nil {
			err := errors.ES(op, errors.KInternal, "received a TableCompletion frame while no streaming table was open")
			d.tables <- TableResult{Table: nil, Err: err}
			return false
		}
		if d.currentStreamingTable.Id() != tc.TableId {
			err := errors.ES(op, errors.KInternal, "received a TableCompletion frame for table %d while table %d was open", tc.TableId, d.currentStreamingTable.Id())
			d.tables <- TableResult{Table: nil, Err: err}
		}

		d.currentStreamingTable.close(tc.OneApiErrors)

		if d.currentStreamingTable.rowCount != tc.RowCount {
			err := errors.ES(op, errors.KInternal, "received a TableCompletion frame for table %d with row count %d while %d rows were received", tc.TableId, tc.RowCount, d.currentStreamingTable.rowCount)
			d.tables <- TableResult{Table: nil, Err: err}
		}

		d.currentStreamingTable = nil
	}

	return true
}

func (d *DataSet) parseDatasetHeader(header *DataSetHeader, op errors.Op) bool {
	d.DataSetHeader = header
	if d.DataSetHeader.Version != version {
		d.tables <- TableResult{Table: nil, Err: errors.ES(op, errors.KInternal, "received a DataSetHeader frame that is not version 2")}
		return false
	}
	if !d.DataSetHeader.IsFragmented {
		d.tables <- TableResult{Table: nil, Err: errors.ES(op, errors.KInternal, "received a DataSetHeader frame that is not fragmented")}
		return false
	}
	if d.DataSetHeader.IsProgressive {
		d.tables <- TableResult{Table: nil, Err: errors.ES(op, errors.KInternal, "received a DataSetHeader frame that is progressive")}
		return false
	}
	const EndOfTableErrorPlacement = errorReportingPlacement
	if d.DataSetHeader.ErrorReportingPlacement != EndOfTableErrorPlacement {
		d.tables <- TableResult{Table: nil, Err: errors.ES(op, errors.KInternal, "received a DataSetHeader frame that does not report errors at the end of the table")}
		return false
	}

	return true
}

// NewDataSet creates a new DataSet from a reader.
// The capacity parameter is the capacity of the channel that receives the frames from the Kusto service.
func NewDataSet(ctx context.Context, r io.ReadCloser, capacity int) *DataSet {
	d := &DataSet{
		reader:       r,
		frames:       make(chan Frame, capacity),
		errorChannel: make(chan error, 1),
		tables:       make(chan TableResult, 1),
		ctx:          ctx,
	}
	go d.readFrames()

	go d.decodeTables()

	return d
}
