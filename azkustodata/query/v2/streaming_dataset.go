package v2

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"io"
)

// streamingDataset is a structure that represents a set of data as returned by the Kusto service.
type streamingDataset struct {
	baseDataset
	// reader is an io.ReadCloser used to read the data from the Kusto service.
	reader io.ReadCloser
	// frames is a channel that receives all the frames from the data set as they are parsed.
	frames chan Frame
	// errorChannel is a channel to report errors during the parsing of frames
	errorChannel chan error
	// results is a channel that sends the parsed results as they are decoded.
	results chan query.TableResult
}

func (d *streamingDataset) Results() <-chan query.TableResult {
	return d.results
}

// readFrames consumes the frames from the reader and sends them to the frames channel.
func (d *streamingDataset) readFrames() {
	err := ReadFramesIterative(d.reader, d.frames)
	if err != nil {
		d.errorChannel <- err
	}

	err = d.reader.Close()
	if err != nil {
		d.errorChannel <- err
	}
}

func (d *streamingDataset) getNextFrame() Frame {
	var f Frame = nil

	select {
	case err := <-d.errorChannel:
		d.reportError(err)
		break
	case <-d.Context().Done():
		d.reportError(errors.ES(d.Op(), errors.KInternal, "context cancelled"))
		break
	case fc := <-d.frames:
		f = fc
	}
	return f
}

func (d *streamingDataset) reportError(err error) {
	d.results <- query.TableResultError(err)
}

func (d *streamingDataset) onFinishHeader() {
	d.results <- query.TableResultSuccess(d.currentTable.(*streamingTable))
}

func (d *streamingDataset) newTableFromHeader(th *TableHeader) (table, error) {
	newStreamingTable, e := NewStreamingTable(d, th)
	if e != nil {
		return nil, e
	}
	return newStreamingTable.(*streamingTable), nil
}

func (d *streamingDataset) close() {
	close(d.results)
}

func (d *streamingDataset) GetAllTables() ([]query.Table, []error) {
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

		rows, errs2 := table.GetAllRows()
		tables = append(tables, query.NewFullTable(d, table.Ordinal(), table.Id(), table.Name(), table.Kind(), table.Columns(), rows, errs2))
	}

	return tables, errs
}

// NewStreamingDataSet creates a new streamingDataset from a reader.
// The capacity parameter is the capacity of the channel that receives the frames from the Kusto service.
func NewStreamingDataSet(ctx context.Context, r io.ReadCloser, capacity int) StreamingDataset {
	d := &streamingDataset{
		baseDataset:  *newBaseDataset(query.NewDataset(ctx, errors.OpQuery), false),
		reader:       r,
		frames:       make(chan Frame, capacity),
		errorChannel: make(chan error, 1),
		results:      make(chan query.TableResult, 1),
	}
	go d.readFrames()

	go decodeTables(d)

	return d
}
