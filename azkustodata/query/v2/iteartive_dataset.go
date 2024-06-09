package v2

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"io"
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
	// results is a channel that sends the parsed results as they are decoded.
	results chan query.TableResult

	frames chan interface{}

	fragmentCapacity int
	rowCapacity      int
	errorChannel     chan error
}

func NewIterativeDataset(ctx context.Context, r io.ReadCloser, capacity int, rowCapacity int, fragmentCapacity int) (query.IterativeDataset, error) {
	d := &iterativeDataset{
		BaseDataset:      query.NewBaseDataset(ctx, errors.OpQuery, PrimaryResultTableKind),
		reader:           r,
		results:          make(chan query.TableResult, 1),
		fragmentCapacity: fragmentCapacity,
		rowCapacity:      rowCapacity,
		frames:           make(chan interface{}, capacity),
		errorChannel:     make(chan error, 1),
	}

	go decodeTables(d)

	go func() {
		defer close(d.frames)
		err := readDataSet(r, d.frames)
		if err != nil {
			select {
			case d.errorChannel <- err:
				break
			default:
				break
			}
		}
	}()

	return d, nil
}

func readDataSet(reader io.Reader, frames chan interface{}) error {
	r, err := newFrameReader(reader)
	err = r.advance()
	if err != nil {
		return err
	}

	err = r.validateDataSetHeader()
	if err != nil {
		return err
	}

	err = r.advance()
	if err != nil {
		return err
	}

	properties, err := r.readQueryProperties()
	if err != nil {
		return err
	}
	frames <- properties

	for {
		err = r.advance()
		if err != nil {
			return err
		}

		frameType, err := r.frameType()
		if err != nil {
			return err
		}

		if frameType == DataTableFrameType {
			queryCompletion, err := r.readQueryCompletionInformation()
			if err != nil {
				return err
			}
			frames <- queryCompletion
			break
		}

		if frameType == TableHeaderFrameType {
			if err = readPrimaryTable(r, frames); err != nil {
				return err
			}
			continue
		}

		return errors.ES(errors.OpQuery, errors.KInternal, "unexpected frame type %s", frameType)
	}

	err = r.advance()
	if err != nil {
		return err
	}

	completion := DataSetCompletion{}
	err = r.unmarshal(&completion)
	if err != nil {
		return err
	}
	if completion.HasErrors {
		return errors.ES(errors.OpQuery, errors.KInternal, "query completion has errors")
	}

	return nil
}

func readPrimaryTable(r *frameReader, frames chan interface{}) error {
	header := TableHeader{}
	err := r.unmarshal(&header)
	if err != nil {
		return err
	}

	frames <- header

	for {
		err = r.advance()
		if err != nil {
			return err
		}
		frameType, err := r.frameType()
		if err != nil {
			return err
		}

		if frameType == TableFragmentFrameType {
			fragment := TableFragment{Columns: header.Columns}
			err = r.unmarshal(&fragment)
			if err != nil {
				return err
			}
			frames <- fragment
			continue
		}

		if frameType == TableCompletionFrameType {
			completion := TableCompletion{}
			err = r.unmarshal(&completion)
			frames <- completion

			break
		}

		return errors.ES(errors.OpQuery, errors.KInternal, "unexpected frame type %s", frameType)
	}

	return nil
}

func (d *iterativeDataset) getNextFrame() interface{} {
	select {
	case err := <-d.errorChannel:
		if err != nil {
			d.tryReportError(err)
		}
		break
	case <-d.Context().Done():
		d.tryReportError(errors.ES(d.Op(), errors.KInternal, "context cancelled"))
		break
	case fc := <-d.frames:
		return fc
	}

	return nil
}

func (d *iterativeDataset) reportError(err error) {
	d.results <- query.TableResultError(err)
}

func (d *iterativeDataset) tryReportError(err error) {
	select {
	case d.results <- query.TableResultError(err):
		return
	default:
		return
	}
}

func (d *iterativeDataset) sendTable(tb query.IterativeTable) {
	select {
	case <-d.errorChannel:
		return
	case d.results <- query.TableResultSuccess(tb):
		return
	}
}

func (d *iterativeDataset) Tables() <-chan query.TableResult {
	return d.results
}

func (d *iterativeDataset) Close() error {
	// try to write to the error channel
	select {
	case err := <-d.errorChannel:
		if err != nil {
			return err
		}
		break
	case d.errorChannel <- nil:
		break
	default:
		break
	}

	return nil
}

func (d *iterativeDataset) ToDataset() (query.Dataset, error) {
	tables := make([]query.Table, 0, len(d.results))

	defer d.Close()

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

		if dataTable, ok := f.(DataTable); ok {
			if !handleDataTable(d, &queryProperties, dataTable) {
				return
			}
		} else if tableHeader, ok := f.(TableHeader); ok {
			if !handleTableHeader(d, &currentTable, tableHeader) {
				return
			}
		} else if tableFragment, ok := f.(TableFragment); ok {
			if !handleTableFragment(d, currentTable, tableFragment) {
				return
			}
		} else if tableCompletion, ok := f.(TableCompletion); ok {
			if !handleTableCompletion(d, &currentTable, tableCompletion) {
				return
			}
		} else {
			d.reportError(errors.ES(op, errors.KInternal, "unknown frame type"))
		}
	}
}

func handleDataTable(d *iterativeDataset, queryProperties *query.IterativeTable, dt DataTable) bool {
	if dt.TableKind == PrimaryResultTableKind {
		d.reportError(errors.ES(d.Op(), errors.KInternal, "received a DataTable frame for a primary result table"))
		return false
	}
	switch dt.TableKind {
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
		d.reportError(errors.ES(d.Op(), errors.KInternal, "unknown secondary table - %s %s", dt.TableName, dt.TableKind))
	}

	return true
}

func handleTableCompletion(d *iterativeDataset, tablePtr **iterativeTable, tc TableCompletion) bool {
	if *tablePtr == nil {
		err := errors.ES(d.Op(), errors.KInternal, "received a TableCompletion frame while no streaming table was open")
		d.reportError(err)
		return false
	}
	if int((*tablePtr).Index()) != tc.TableId {
		err := errors.ES(d.Op(), errors.KInternal, "received a TableCompletion frame for table %d while table %d was open", tc.TableId, int((*tablePtr).Index()))
		d.reportError(err)
	}

	(*tablePtr).finishTable(tc.OneApiErrors)

	*tablePtr = nil

	return true
}

func handleTableFragment(d *iterativeDataset, table *iterativeTable, tf TableFragment) bool {
	if table == nil {
		err := errors.ES(d.Op(), errors.KInternal, "received a TableFragment frame while no streaming table was open")
		d.reportError(err)
		return false
	}

	table.addRawRows(tf.Rows)

	return true
}

func handleTableHeader(d *iterativeDataset, table **iterativeTable, th TableHeader) bool {
	if *table != nil {
		err := errors.ES(d.Op(), errors.KInternal, "received a TableHeader frame while a streaming table was still open")
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
