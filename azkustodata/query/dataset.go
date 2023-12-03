package query

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"io"
	"sync"
)

// DefaultFrameCapacity is the default capacity of the channel that receives frames from the Kusto service. Lower capacity means less memory usage, but might cause the channel to block if the frames are not consumed fast enough.
const DefaultFrameCapacity = 5

const version = "v2.0"
const errorReportingPlacement = "EndOfTable"
const PrimaryResultTableKind = "PrimaryResult"

// TableResult is a structure that holds the result of a table operation.
// It contains a Table and an error, if any occurred during the operation.
type TableResult struct {
	// Table is the result of the operation.
	Table StreamingTable
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
	header *DataSetHeader
	// Completion is the completion status of the data set. It's the last frame received.
	completion *DataSetCompletion

	// queryProperties contains the information from the "QueryProperties" table.
	queryProperties []QueryProperties
	// queryCompletionInformation contains the information from the "QueryCompletionInformation" table.
	queryCompletionInformation []QueryCompletionInformation

	// frames is a channel that receives all the frames from the data set as they are parsed.
	frames chan Frame
	// errorChannel is a channel to report errors during the parsing of frames
	errorChannel chan error

	// results is a channel that sends the parsed results as they are decoded.
	results chan TableResult

	// currentStreamingTable is a reference to the current streamed table, which is still receiving rows.
	currentStreamingTable *streamingTable

	lock sync.RWMutex
}

func (d *DataSet) Header() *DataSetHeader {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.header
}

func (d *DataSet) setHeader(dataSetHeader *DataSetHeader) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.header = dataSetHeader
}

func (d *DataSet) Completion() *DataSetCompletion {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.completion
}

func (d *DataSet) setCompletion(completion *DataSetCompletion) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.completion = completion
}

func (d *DataSet) getCurrentStreamingTable() *streamingTable {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.currentStreamingTable
}

func (d *DataSet) setCurrentStreamingTable(currentStreamingTable *streamingTable) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.currentStreamingTable = currentStreamingTable
}

func (d *DataSet) Results() chan TableResult {
	return d.results
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

// decodeTables decodes the frames from the frames channel and sends the results to the results channel.
func (d *DataSet) decodeTables() {
	defer func() {
		close(d.results)
		table := d.getCurrentStreamingTable()
		if table != nil {
			table.close([]OneApiError{})
		}
	}()
	op := d.op()

	for {
		var f Frame = nil

		select {
		case err := <-d.errorChannel:
			d.results <- TableResult{Table: nil, Err: err}
			break
		case <-d.ctx.Done():
			d.results <- TableResult{Table: nil, Err: errors.ES(op, errors.KInternal, "context cancelled")}
			break
		case fc, ok := <-d.frames:
			if ok {
				f = fc
			}
		}

		if f == nil {
			break
		}

		if d.Completion() != nil {
			d.results <- TableResult{Table: nil, Err: errors.ES(op, errors.KInternal, "received a frame after DataSetCompletion")}
			break
		}

		if header, ok := f.(*DataSetHeader); ok {
			if !d.parseDatasetHeader(header, op) {
				break
			}
		} else if completion, ok := f.(*DataSetCompletion); ok {
			d.setCompletion(completion)
		} else if dt, ok := f.(*DataTable); ok {
			t, err := NewFullTable(d, dt)
			if err != nil {
				d.results <- TableResult{Table: nil, Err: err}
			}
			err = d.parseSecondaryTable(t)
			if err != nil {
				d.results <- TableResult{Table: nil, Err: err}
			}
		} else if d.parseStreamingTable(f, op) {
			continue
		} else {
			err := errors.ES(op, errors.KInternal, "unknown frame type")
			d.results <- TableResult{Table: nil, Err: err}
			break
		}
	}
}

func (d *DataSet) parseStreamingTable(f Frame, op errors.Op) bool {

	table := d.getCurrentStreamingTable()

	if th, ok := f.(*TableHeader); ok {
		if table != nil {
			err := errors.ES(op, errors.KInternal, "received a TableHeader frame while a streaming table was still open")
			d.results <- TableResult{Table: nil, Err: err}
			return false
		}
		if table.Kind() != PrimaryResultTableKind {
			err := errors.ES(op, errors.KInternal, "Received a TableHeader frame for a table that is not a primary result table")
			d.results <- TableResult{Table: nil, Err: err}
			return false
		}

		t, err := NewStreamingTable(d, th)
		if err != nil {
			d.results <- TableResult{Table: nil, Err: err}
			return false
		}
		d.setCurrentStreamingTable(t.(*streamingTable))
		d.results <- TableResult{Table: t, Err: nil}
	} else if tf, ok := f.(*TableFragment); ok {
		if table == nil {
			err := errors.ES(op, errors.KInternal, "received a TableFragment frame while no streaming table was open")
			d.results <- TableResult{Table: nil, Err: err}
			return false
		}
		if table.Id() != tf.TableId {
			err := errors.ES(op, errors.KInternal, "received a TableFragment frame for table %d while table %d was open", tf.TableId, table.Id())
			d.results <- TableResult{Table: nil, Err: err}
		}

		table.rawRows <- tf.Rows
	} else if tc, ok := f.(*TableCompletion); ok {
		if table == nil {
			err := errors.ES(op, errors.KInternal, "received a TableCompletion frame while no streaming table was open")
			d.results <- TableResult{Table: nil, Err: err}
			return false
		}
		if table.Id() != tc.TableId {
			err := errors.ES(op, errors.KInternal, "received a TableCompletion frame for table %d while table %d was open", tc.TableId, table.Id())
			d.results <- TableResult{Table: nil, Err: err}
		}

		table.close(tc.OneApiErrors)

		if table.rowCount != tc.RowCount {
			err := errors.ES(op, errors.KInternal, "received a TableCompletion frame for table %d with row count %d while %d rows were received", tc.TableId, tc.RowCount, table.rowCount)
			d.results <- TableResult{Table: nil, Err: err}
		}

		d.setCurrentStreamingTable(nil)
	}

	return true
}

func (d *DataSet) parseDatasetHeader(header *DataSetHeader, op errors.Op) bool {
	if header.Version != version {
		d.results <- TableResult{Table: nil, Err: errors.ES(op, errors.KInternal, "received a DataSetHeader frame that is not version 2")}
		return false
	}
	if !header.IsFragmented {
		d.results <- TableResult{Table: nil, Err: errors.ES(op, errors.KInternal, "received a DataSetHeader frame that is not fragmented")}
		return false
	}
	if header.IsProgressive {
		d.results <- TableResult{Table: nil, Err: errors.ES(op, errors.KInternal, "received a DataSetHeader frame that is progressive")}
		return false
	}
	const EndOfTableErrorPlacement = errorReportingPlacement
	if header.ErrorReportingPlacement != EndOfTableErrorPlacement {
		d.results <- TableResult{Table: nil, Err: errors.ES(op, errors.KInternal, "received a DataSetHeader frame that does not report errors at the end of the table")}
		return false
	}
	d.setHeader(header)

	return true
}

// NewDataSet creates a new DataSet from a reader.
// The capacity parameter is the capacity of the channel that receives the frames from the Kusto service.
func NewDataSet(ctx context.Context, r io.ReadCloser, capacity int) *DataSet {
	d := &DataSet{
		reader:       r,
		frames:       make(chan Frame, capacity),
		errorChannel: make(chan error, 1),
		results:      make(chan TableResult, 1),
		ctx:          ctx,
	}
	go d.readFrames()

	go d.decodeTables()

	return d
}
