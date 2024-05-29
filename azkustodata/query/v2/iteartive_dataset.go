package v2

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"io"
	"time"
)

// DefaultFrameCapacity is the default capacity of the channel that receives frames from the Kusto service. Lower capacity means less memory usage, but might cause the channel to block if the frames are not consumed fast enough.
const DefaultFrameCapacity = 10

const DefaultRowCapacity = 1000

const DefaultFragmentCapacity = 1

const version = "v2.0"
const PrimaryResultTableKind = "PrimaryResult"

// iterativeDataset contains the main logic of parsing a v2 dataset.
// v2 is made from a series of frames, which are decoded by turn.
type iterativeDataset struct {
	query.BaseDataset

	// reader is an io.ReadCloser used to read the data from the Kusto service.
	reader io.ReadCloser
	// frames is a channel that receives all the frames from the data set as they are parsed.
	frames chan *EveryFrame
	// results is a channel that sends the parsed results as they are decoded.
	results chan query.TableResult

	fragmentCapacity int
	rowCapacity      int

	readFramesDone bool
}

func NewIterativeDataset(ctx context.Context, r io.ReadCloser, capacity int, rowCapacity int, fragmentCapacity int) (query.IterativeDataset, error) {
	d := &iterativeDataset{
		BaseDataset:      query.NewBaseDataset(ctx, errors.OpQuery, PrimaryResultTableKind),
		reader:           r,
		frames:           make(chan *EveryFrame, capacity),
		results:          make(chan query.TableResult, 1),
		fragmentCapacity: fragmentCapacity,
		rowCapacity:      rowCapacity,
	}

	br, err := prepareReadBuffer(d.reader)
	if err != nil {
		closeErr := d.reader.Close()
		if closeErr != nil {
			return nil, errors.TryCombinedError(err, closeErr)
		}

		return nil, err
	}

	go func() {
		defer func() {
			err := d.reader.Close()
			if err != nil {
				d.reportError(err)
			}

			d.readFramesDone = true
		}()

		err := readFramesIterative(br, d.frames)
		if err != nil {
			d.reportError(err)
		}
	}()

	go decodeTables(d)

	return d, nil
}

func (d *iterativeDataset) getNextFrame() *EveryFrame {
	var f *EveryFrame = nil

	for {
		select {
		case <-d.Context().Done():
			d.reportError(errors.ES(d.Op(), errors.KInternal, "context cancelled"))
			return nil
		case fc := <-d.frames:
			f = fc
			return f
		}
	}
}

func (d *iterativeDataset) reportError(err error) {
	ticker := time.NewTicker(100)
	defer ticker.Stop()

	for {
		select {
		case d.results <- query.TableResultError(err):
			return
		case <-d.Context().Done():
			return
		case <-ticker.C:
			if d.readFramesDone {
				// If the dataset is closed, we don't want to block the goroutine that is sending the results.
				return
			}
		}
	}
}

func (d *iterativeDataset) sendTable(tb query.IterativeTable) {
	select {
	case d.results <- query.TableResultSuccess(tb):
		return
	case <-d.Context().Done():
		return
	}
}

func (d *iterativeDataset) Tables() <-chan query.TableResult {
	return d.results
}

func (d *iterativeDataset) Close() error {
	return nil
}

func (d *iterativeDataset) ToDataset() (query.Dataset, error) {
	tables := make([]query.Table, 0, len(d.results))

	for tb := range d.Tables() {
		if tb.Err() != nil {
			return nil, tb.Err()
		}

		table, err := tb.Table().ToTable()
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return query.NewDataset(d, tables), nil
}

// decodeTables decodes the frames from the frames channel and sends the results to the results channel.
func decodeTables(d *iterativeDataset) {
	op := d.Op()

	gotDataSetCompletion := false
	var currentTable *iterativeTable
	var queryProperties query.IterativeTable

	defer func() {
		if currentTable != nil {
			currentTable.finishTable([]OneApiError{})
		}
		close(d.results)
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
			if !handleTableHeader(d, &currentTable, th) {
				break
			}
		} else if tf := f.AsTableFragment(); tf != nil {
			if !handleTableFragment(d, currentTable, tf) {
				break
			}
		} else if tc := f.AsTableCompletion(); tc != nil {
			if !handleTableCompletion(d, &currentTable, tc) {
				break
			}
		} else if prog := f.AsTableProgress(); prog != nil {
			d.reportError(errors.ES(op, errors.KInternal, "Unexpected TableProgress frame - progressive results are not supported"))
			break
		} else {
			// Not a frame we know how to handle
			d.reportError(errors.ES(op, errors.KInternal, "unknown frame type"))
			break
		}
	}
}

func handleDatasetCompletion(d *iterativeDataset, c DataSetCompletion) {
	if c.HasErrors() && c.OneApiErrors() != nil {
		for _, e := range c.OneApiErrors() {
			d.reportError(&e)
		}
	}
}

func handleDataTable(d *iterativeDataset, queryProperties *query.IterativeTable, dt DataTable) bool {
	if dt.TableKind() == PrimaryResultTableKind {
		d.reportError(errors.ES(d.Op(), errors.KInternal, "received a DataTable frame for a primary result table"))
		return false
	}
	switch dt.TableKind() {
	case QueryPropertiesKind:
		// When we get this, we want to store it and not send it to the user immediately.
		// We will wait until after the primary results (when we get the QueryCompletionInformation table) and then send it.
		res, err := newTable(d, dt)
		if err != nil {
			d.reportError(err)
			return false
		}
		*queryProperties = iterativeWrapper{res}
	case QueryCompletionInformationKind:
		if *queryProperties != nil {
			d.sendTable(*queryProperties)
		}

		res, err := newTable(d, dt)
		if err != nil {
			d.reportError(err)
			return false
		}
		d.sendTable(iterativeWrapper{res})

	default:
		d.reportError(errors.ES(d.Op(), errors.KInternal, "unknown secondary table - %s %s", dt.TableName(), dt.TableKind()))
	}

	return true
}

func handleTableCompletion(d *iterativeDataset, tablePtr **iterativeTable, tc TableCompletion) bool {
	if *tablePtr == nil {
		err := errors.ES(d.Op(), errors.KInternal, "received a TableCompletion frame while no streaming table was open")
		d.reportError(err)
		return false
	}
	if int((*tablePtr).Index()) != tc.TableId() {
		err := errors.ES(d.Op(), errors.KInternal, "received a TableCompletion frame for table %d while table %d was open", tc.TableId(), int((*tablePtr).Index()))
		d.reportError(err)
	}

	(*tablePtr).finishTable(tc.OneApiErrors())

	*tablePtr = nil

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

func handleTableHeader(d *iterativeDataset, table **iterativeTable, th TableHeader) bool {
	if *table != nil {
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

	*table = t.(*iterativeTable)
	d.sendTable(*table)

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
