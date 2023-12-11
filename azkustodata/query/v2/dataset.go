package v2

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"io"
	"sync"
)

// DefaultFrameCapacity is the default capacity of the channel that receives frames from the Kusto service. Lower capacity means less memory usage, but might cause the channel to block if the frames are not consumed fast enough.
const DefaultFrameCapacity = 5

const version = "v2.0"
const errorReportingPlacement = "EndOfTable"
const PrimaryResultTableKind = "PrimaryResult"

// dataSet is a structure that represents a set of data as returned by the Kusto service.
type dataSet struct {
	query.Dataset
	// reader is an io.ReadCloser used to read the data from the Kusto service.
	reader io.ReadCloser
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
	results chan query.TableResult

	// currentStreamingTable is a reference to the current streamed table, which is still receiving rows.
	currentStreamingTable *streamingTable

	lock sync.RWMutex
}

func (d *dataSet) Header() *DataSetHeader {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.header
}

func (d *dataSet) setHeader(dataSetHeader *DataSetHeader) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.header = dataSetHeader
}

func (d *dataSet) Completion() *DataSetCompletion {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.completion
}

func (d *dataSet) setCompletion(completion *DataSetCompletion) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.completion = completion
}

func (d *dataSet) getCurrentStreamingTable() *streamingTable {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.currentStreamingTable
}

func (d *dataSet) setCurrentStreamingTable(currentStreamingTable *streamingTable) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.currentStreamingTable = currentStreamingTable
}

func (d *dataSet) Results() chan query.TableResult {
	return d.results
}

// readFrames consumes the frames from the reader and sends them to the frames channel.
func (d *dataSet) readFrames() {
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
func (d *dataSet) decodeTables() {
	defer func() {
		close(d.results)
		table := d.getCurrentStreamingTable()
		if table != nil {
			table.close([]OneApiError{})
		}
	}()
	op := d.Op()

	for {
		var f Frame = nil

		select {
		case err := <-d.errorChannel:
			d.results <- query.TableResultError(err)
			break
		case <-d.Context().Done():
			d.results <- query.TableResultError(errors.ES(op, errors.KInternal, "context cancelled"))
			break
		case fc := <-d.frames:
			f = fc
		}

		if f == nil {
			break
		}

		if d.Completion() != nil {
			d.results <- query.TableResultError(errors.ES(op, errors.KInternal, "received a frame after DataSetCompletion"))
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
				d.results <- query.TableResultError(err)
				break
			}
			err = d.parseSecondaryTable(t)
			if err != nil {
				d.results <- query.TableResultError(err)
				break
			}
		} else if d.parseStreamingTable(f, op) {
			continue
		} else {
			err := errors.ES(op, errors.KInternal, "unknown frame type")
			d.results <- query.TableResultError(err)
			break
		}
	}
}

func (d *dataSet) parseStreamingTable(f Frame, op errors.Op) bool {

	table := d.getCurrentStreamingTable()

	if th, ok := f.(*TableHeader); ok {
		if table != nil {
			err := errors.ES(op, errors.KInternal, "received a TableHeader frame while a streaming table was still open")
			d.results <- query.TableResultError(err)
			return false
		}
		if th.TableKind != PrimaryResultTableKind {
			err := errors.ES(op, errors.KInternal, "Received a TableHeader frame for a table that is not a primary result table")
			d.results <- query.TableResultError(err)
			return false
		}

		t, err := NewStreamingTable(d, th)
		if err != nil {
			d.results <- query.TableResultError(err)
			return false
		}
		d.setCurrentStreamingTable(t.(*streamingTable))
		d.results <- query.TableResultSuccess(t)
	} else if tf, ok := f.(*TableFragment); ok {
		if table == nil {
			err := errors.ES(op, errors.KInternal, "received a TableFragment frame while no streaming table was open")
			d.results <- query.TableResultError(err)
			return false
		}
		if int(table.Ordinal()) != tf.TableId {
			err := errors.ES(op, errors.KInternal, "received a TableFragment frame for table %d while table %d was open", tf.TableId, int(table.Ordinal()))
			d.results <- query.TableResultError(err)
		}

		table.rawRows <- tf.Rows
	} else if tc, ok := f.(*TableCompletion); ok {
		if table == nil {
			err := errors.ES(op, errors.KInternal, "received a TableCompletion frame while no streaming table was open")
			d.results <- query.TableResultError(err)
			return false
		}
		if int(table.Ordinal()) != tc.TableId {
			err := errors.ES(op, errors.KInternal, "received a TableCompletion frame for table %d while table %d was open", tc.TableId, int(table.Ordinal()))
			d.results <- query.TableResultError(err)
		}

		table.close(tc.OneApiErrors)

		if table.rowCount != tc.RowCount {
			err := errors.ES(op, errors.KInternal, "received a TableCompletion frame for table %d with row count %d while %d rows were received", tc.TableId, tc.RowCount, table.rowCount)
			d.results <- query.TableResultError(err)
		}

		d.setCurrentStreamingTable(nil)
	}

	return true
}

func (d *dataSet) parseDatasetHeader(header *DataSetHeader, op errors.Op) bool {
	if header.Version != version {
		d.results <- query.TableResultError(errors.ES(op, errors.KInternal, "received a DataSetHeader frame that is not version 2"))
		return false
	}
	if !header.IsFragmented {
		d.results <- query.TableResultError(errors.ES(op, errors.KInternal, "received a DataSetHeader frame that is not fragmented"))
		return false
	}
	if header.IsProgressive {
		d.results <- query.TableResultError(errors.ES(op, errors.KInternal, "received a DataSetHeader frame that is progressive"))
		return false
	}
	const EndOfTableErrorPlacement = errorReportingPlacement
	if header.ErrorReportingPlacement != EndOfTableErrorPlacement {
		d.results <- query.TableResultError(errors.ES(op, errors.KInternal, "received a DataSetHeader frame that does not report errors at the end of the table"))
		return false
	}
	d.setHeader(header)

	return true
}

func (d *dataSet) Consume() ([]query.Table, []error) {
	tables := make([]query.Table, 0, len(d.results))
	errs := make([]error, 0, len(d.results))

	for tb := range d.Results() {
		if tb.Err() != nil {
			errs = append(errs, tb.Err())
			continue
		}

		table := tb.Table()
		if table == nil {
			errs = append(errs, errors.ES(d.Op(), errors.KInternal, "received a nil table"))
			continue
		}

		rows, errs2 := table.Consume()
		tables = append(tables, query.NewFullTable(d, table.Ordinal(), table.Id(), table.Name(), table.Kind(), table.Columns(), rows, errs2))
	}

	return tables, errs
}

// NewDataSet creates a new dataSet from a reader.
// The capacity parameter is the capacity of the channel that receives the frames from the Kusto service.
func NewDataSet(ctx context.Context, r io.ReadCloser, capacity int) Dataset {
	d := &dataSet{
		Dataset:      query.NewDataset(ctx, errors.OpQuery),
		reader:       r,
		frames:       make(chan Frame, capacity),
		errorChannel: make(chan error, 1),
		results:      make(chan query.TableResult, 1),
	}
	go d.readFrames()

	go d.decodeTables()

	return d
}

type Dataset interface {
	query.Dataset
	Header() *DataSetHeader
	Completion() *DataSetCompletion
	QueryProperties() []QueryProperties
	QueryCompletionInformation() []QueryCompletionInformation
	Results() chan query.TableResult
}
