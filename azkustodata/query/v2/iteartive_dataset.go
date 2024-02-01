package v2

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"io"
)

// DefaultFrameCapacity is the default capacity of the channel that receives frames from the Kusto service. Lower capacity means less memory usage, but might cause the channel to block if the frames are not consumed fast enough.
const DefaultFrameCapacity = 5

const version = "v2.0"
const PrimaryResultTableKind = "PrimaryResult"

// iterativeDataset contains the main logic of parsing a v2 dataset.
// v2 is made from a series of frames, which are decoded by turn.
// This supports both full and streaming datasets, via fullDataset and iterativeDataset  respectively.
type iterativeDataset struct {
	query.Dataset

	// reader is an io.ReadCloser used to read the data from the Kusto service.
	reader io.ReadCloser
	// frames is a channel that receives all the frames from the data set as they are parsed.
	frames chan *EveryFrame
	// errorChannel is a channel to report errors during the parsing of frames
	errorChannel chan error
	// results is a channel that sends the parsed results as they are decoded.
	results chan TableResult
}

func NewIterativeDataSet(ctx context.Context, r io.ReadCloser, capacity int) (IterativeDataset, error) {
	d := &iterativeDataset{
		Dataset:      query.NewDataset(ctx, errors.OpQuery),
		reader:       r,
		frames:       make(chan *EveryFrame, capacity),
		errorChannel: make(chan error, 1),
		results:      make(chan TableResult, 1),
	}

	br, err := prepareReadBuffer(d.reader)
	if err != nil {
		return nil, err
	}

	go func() {
		err := readFramesIterative(br, d.frames)
		if err != nil {
			d.errorChannel <- err
		}
	}()

	go decodeTables(d)

	return d, nil
}

func (d *iterativeDataset) getNextFrame() *EveryFrame {
	var f *EveryFrame = nil

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

func (d *iterativeDataset) reportError(err error) {
	d.results <- TableResultError(err)
}

func (d *iterativeDataset) sendTable(tb *iterativeTable) {
	d.results <- TableResultSuccess(tb)
}

func (d *iterativeDataset) Results() <-chan TableResult {
	return d.results
}

func (d *iterativeDataset) Close() error {
	close(d.results)
	// try to close the error channel, but don't block if it's full
	select {
	case <-d.errorChannel:
	default:
	}
	close(d.errorChannel)

	return d.reader.Close()
}

func (d *iterativeDataset) ToFullDataset() (FullDataset, error) {
	tables := make([]query.FullTable, 0, len(d.results))

	for tb := range d.Results() {
		if tb.Err() != nil {
			return nil, tb.Err()
		}

		table, err := tb.Table().ToFullTable()
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return NewFullDataset(d, tables), nil
}

// decodeTables decodes the frames from the frames channel and sends the results to the results channel.
func decodeTables(d *iterativeDataset) {
	op := d.Op()

	gotDataSetCompletion := false
	var currentTable *iterativeTable
	var queryProperties *iterativeTable

	defer func() {
		_ = d.Close()
		if currentTable != nil {
			currentTable.close([]OneApiError{})
		}
	}()

	for {
		f := d.getNextFrame()

		if f == nil {
			break
		}

		if gotDataSetCompletion {
			d.reportError(errors.ES(op, errors.KInternal, "received a frame after DataSetCompletion"))
			break
		} else if h := f.AsDataSetHeader(); h != nil {
			if !handleDatasetHeader(d, h) {
				break
			}
		} else if c := f.AsDataSetCompletion(); c != nil {
			handleDatasetCompletion(d, c)
			gotDataSetCompletion = true
		} else if dt := f.AsDataTable(); dt != nil {
			if !handleDataTable(d, &queryProperties, dt) {
				break
			}
		} else if th := f.AsTableHeader(); th != nil {
			if !handleTableHeader(d, currentTable, th) {
				break
			}
		} else if tf := f.AsTableFragment(); tf != nil {
			if !handleTableFragment(d, currentTable, tf) {
				break
			}
		} else if tc := f.AsTableCompletion(); tc != nil {
			if !handleTableCompletion(d, currentTable, tc) {
				break
			}
		} else if prog := f.AsTableProgress(); prog != nil {
			d.reportError(errors.ES(op, errors.KInternal, "Unexpected TableProgress frame - progressive results are not supported"))
			break
		}

		// Not a frame we know how to handle
		d.reportError(errors.ES(op, errors.KInternal, "unknown frame type"))
	}
}

func handleDatasetCompletion(d *iterativeDataset, c DataSetCompletion) {
	if c.HasErrors() && c.OneApiErrors() != nil {
		for _, e := range c.OneApiErrors() {
			d.reportError(&e)
		}
	}
}

func handleDataTable(d *iterativeDataset, queryProperties **iterativeTable, dt DataTable) bool {
	if dt.TableKind() == PrimaryResultTableKind {
		d.reportError(errors.ES(d.Op(), errors.KInternal, "received a DataTable frame for a primary result table"))
		return false
	}
	switch dt.TableKind() {
	case QueryPropertiesKind:
		// When we get this, we want to store it and not send it to the user immediately.
		// We will wait until after the primary results (when we get the QueryCompletionInformation table) and then send it.
		res, err := NewIterativeTableFromDataTable(d, dt)
		if err != nil {
			d.reportError(err)
			return false
		}
		*queryProperties = res.(*iterativeTable)
	case QueryCompletionInformationKind:
		if *queryProperties == nil {
			d.sendTable(*queryProperties)
		}

		res, err := NewIterativeTableFromDataTable(d, dt)
		if err != nil {
			d.reportError(err)
			return false
		}
		d.sendTable(res.(*iterativeTable))

	default:
		d.reportError(errors.ES(d.Op(), errors.KInternal, "unknown secondary table - %s %s", dt.TableName(), dt.TableKind()))
	}

	return true
}

func handleTableCompletion(d *iterativeDataset, table *iterativeTable, tc TableCompletion) bool {
	if table == nil {
		err := errors.ES(d.Op(), errors.KInternal, "received a TableCompletion frame while no streaming table was open")
		d.reportError(err)
		return false
	}
	if int(table.Index()) != tc.TableId() {
		err := errors.ES(d.Op(), errors.KInternal, "received a TableCompletion frame for table %d while table %d was open", tc.TableId(), int(table.Index()))
		d.reportError(err)
	}

	table.close(tc.OneApiErrors())

	if table.RowCount() != tc.RowCount() {
		err := errors.ES(d.Op(), errors.KInternal, "received a TableCompletion frame for table %d with row count %d while %d rows were received", tc.TableId(), tc.RowCount(), table.RowCount())
		d.reportError(err)
	}

	table = nil

	return true
}

func handleTableFragment(d *iterativeDataset, table *iterativeTable, tf TableFragment) bool {
	if table == nil {
		err := errors.ES(d.Op(), errors.KInternal, "received a TableFragment frame while no streaming table was open")
		d.reportError(err)
		return false
	}
	if int(table.Index()) != tf.TableId() {
		err := errors.ES(d.Op(), errors.KInternal, "received a TableFragment frame for table %d while table %d was open", tf.TableId(), int(table.Index()))
		d.reportError(err)
	}

	table.addRawRows(tf.Rows())

	return true
}

func handleTableHeader(d *iterativeDataset, table *iterativeTable, th TableHeader) bool {
	if table != nil {
		err := errors.ES(d.Op(), errors.KInternal, "received a TableHeader frame while a streaming table was still open")
		d.reportError(err)
		return false
	}

	if th.TableKind() != PrimaryResultTableKind {
		err := errors.ES(d.Op(), errors.KInternal, "Received a TableHeader frame for a table that is not a primary result table")
		d.reportError(err)
		return false
	}

	// Read the table header, set it as the current table, and send it to the user (so they can start reading rows)

	t, err := NewIterativeTable(d, th)
	if err != nil {
		d.reportError(err)
		return false
	}

	table = t.(*iterativeTable)
	d.sendTable(table)

	return true
}

func handleDatasetHeader(d *iterativeDataset, header DataSetHeader) bool {
	if header.Version() != version {
		d.reportError(errors.ES(d.Op(), errors.KInternal, "results that are not version 2 are not supported"))
		return false
	}
	if header.IsProgressive() {
		d.reportError(errors.ES(d.Op(), errors.KInternal, "progressive results are not supported"))
		return false
	}
	if !header.IsFragmented() {
		d.reportError(errors.ES(d.Op(), errors.KInternal, "non-fragmented results are not supported"))
		return false
	}

	return true
}
