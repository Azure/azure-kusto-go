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
	reader        io.ReadCloser
	DataSetHeader *DataSetHeader
	Completion    *DataSetCompletion

	queryProperties            []QueryProperties
	queryCompletionInformation []QueryCompletionInformation

	SecondaryResults []Table

	frames       chan Frame
	errorChannel chan error

	tables chan TableResult

	currentStreamingTable *streamingTable
	ctx                   context.Context
}

var errorTableUninitialized = errors.ES(errors.OpUnknown, errors.KInternal, "Table uninitialized")

const QueryPropertiesKind = "QueryProperties"
const QueryCompletionInformationKind = "QueryCompletionInformation"

func (d *DataSet) QueryProperties() ([]QueryProperties, error) {
	if d.SecondaryResults == nil {
		return nil, errorTableUninitialized
	}

	if d.queryProperties != nil {
		return d.queryProperties, nil
	}

	for _, t := range d.SecondaryResults {
		if t.Kind() == QueryPropertiesKind {
			rows := t.(FullTable).Rows()
			d.queryProperties = make([]QueryProperties, 0, len(rows))
			for i, r := range rows {
				err := r.ToStruct(&d.queryProperties[i])
				if err != nil {
					return nil, err
				}
			}

			return d.queryProperties, nil
		}
	}

	return nil, errorTableUninitialized
}

func (d *DataSet) QueryCompletionInformation() ([]QueryCompletionInformation, error) {
	if d.SecondaryResults == nil {
		return nil, errorTableUninitialized
	}

	if d.queryCompletionInformation != nil {
		return d.queryCompletionInformation, nil
	}

	for _, t := range d.SecondaryResults {
		if t.Kind() == QueryCompletionInformationKind {
			rows := t.(FullTable).Rows()
			d.queryCompletionInformation = make([]QueryCompletionInformation, 0, len(rows))
			for i, r := range rows {
				err := r.ToStruct(&d.queryCompletionInformation[i])
				if err != nil {
					return nil, err
				}
			}

			return d.queryCompletionInformation, nil
		}
	}

	return nil, errorTableUninitialized
}

func (d *DataSet) ReadFrames() {
	err := ReadFrames(d.reader, d.frames)
	if err != nil {
		d.errorChannel <- err
	}

	d.reader.Close()
}

func (d *DataSet) DecodeTables() {
	defer func() {
		close(d.tables)
		if d.currentStreamingTable != nil {
			d.currentStreamingTable.close([]OneApiError{})
		}
	}()
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
			if !t.IsPrimaryResult() {
				d.SecondaryResults = append(d.SecondaryResults, t)
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

func NewDataSet(ctx context.Context, r io.ReadCloser, capacity int) *DataSet {
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
