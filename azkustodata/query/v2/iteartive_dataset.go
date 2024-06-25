package v2

import (
	"bytes"
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/goccy/go-json"
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

	// results is a channel that sends the parsed results as they are decoded.
	results chan query.TableResult

	fragmentCapacity int
	rowCapacity      int
	cancel           context.CancelFunc
	currentTable     *iterativeTable
	queryProperties  query.IterativeTable
	jsonData         chan interface{}
}

func NewIterativeDataset(ctx context.Context, r io.ReadCloser, ioCapacity int, rowCapacity int, fragmentCapacity int) (query.IterativeDataset, error) {

	ctx, cancel := context.WithCancel(ctx)

	d := &iterativeDataset{
		BaseDataset:     query.NewBaseDataset(ctx, errors.OpQuery, PrimaryResultTableKind),
		results:         make(chan query.TableResult, fragmentCapacity),
		rowCapacity:     rowCapacity,
		cancel:          cancel,
		currentTable:    nil,
		queryProperties: nil,
		jsonData:        make(chan interface{}, ioCapacity),
	}
	reader, err := newFrameReader(r, ctx)
	if err != nil {
		cancel()
		r.Close()
		return nil, err
	}

	go parseRoutine(d, reader, cancel)
	go readRoutine(reader, d)

	return d, nil
}

func readRoutine(reader *frameReader, d *iterativeDataset) {
	defer close(d.jsonData)
	for {
		err := reader.advance()
		if err != nil {
			if err != io.EOF {
				select {
				case d.jsonData <- err:
				case <-d.Context().Done():
				}
			}
			return
		} else {
			select {
			case d.jsonData <- reader.line:
			case <-d.Context().Done():
				return
			}
		}
	}
}

func parseRoutine(d *iterativeDataset, reader *frameReader, cancel context.CancelFunc) {

	err := readDataSet(d)
	if err != nil {
		select {
		case d.results <- query.TableResultError(err):
		case <-d.Context().Done():
		}
		cancel()
	}

	if d.currentTable != nil {
		d.currentTable.finishTable([]OneApiError{}, err)
	}

	cancel()
	reader.Close()
	close(d.results)
}

func readDataSet(d *iterativeDataset) error {

	var err error

	if header, _, err := nextFrame(d); err == nil {
		if err = validateDataSetHeader(header); err != nil {
			return err
		}
	} else {
		return err
	}

	if decoder, frameType, err := nextFrame(d); err == nil {
		if frameType != DataTableFrameType {
			return errors.ES(errors.OpQuery, errors.KInternal, "unexpected frame type %s, expected DataTable", frameType)
		}

		if err = handleDataTable(d, decoder); err != nil {
			return err
		}
	} else {
		return err
	}

	for decoder, frameType, err := nextFrame(d); err == nil; decoder, frameType, err = nextFrame(d) {
		if frameType == DataTableFrameType {
			if err = handleDataTable(d, decoder); err != nil {
				return err
			}
			continue
		}

		if frameType == TableHeaderFrameType {
			if err = readPrimaryTable(d, decoder); err != nil {
				return err
			}
			continue
		}

		if frameType == DataSetCompletionFrameType {
			err = readDataSetCompletion(decoder)
			if err != nil {
				return err
			}
			return nil
		}

		return errors.ES(errors.OpQuery, errors.KInternal, "unexpected frame type %s, expected DataTable, TableHeader, or DataSetCompletion", frameType)
	}

	return err
}

func nextFrame(d *iterativeDataset) (*json.Decoder, FrameType, error) {
	var line []byte
	select {
	case <-d.Context().Done():
		return nil, "", errors.ES(errors.OpQuery, errors.KInternal, "context cancelled")
	case val := <-d.jsonData:
		if err, ok := val.(error); ok {
			return nil, "", err
		}
		line = val.([]byte)
	}

	frameType, err := peekFrameType(line)
	if err != nil {
		return nil, "", err
	}

	return json.NewDecoder(bytes.NewReader(line)), frameType, nil
}

func readDataSetCompletion(dec *json.Decoder) error {
	completion := DataSetCompletion{}
	err := dec.Decode(&completion)
	if err != nil {
		return err
	}
	if completion.HasErrors {
		return combineOneApiErrors(completion.OneApiErrors)
	}
	return nil
}

func combineOneApiErrors(errs []OneApiError) error {
	c := errors.NewCombinedError()
	for _, e := range errs {
		c.AddError(&e)
	}
	return c.Unwrap()
}

func readPrimaryTable(d *iterativeDataset, dec *json.Decoder) error {
	header := TableHeader{}
	err := dec.Decode(&header)
	if err != nil {
		return err
	}

	if err := handleTableHeader(d, header); err != nil {
		return err
	}

	for i := 0; ; {
		dec, frameType, err := nextFrame(d)
		if err != nil {
			return err
		}
		if frameType == TableFragmentFrameType {
			fragment := TableFragment{Columns: header.Columns, PreviousIndex: i}
			err = dec.Decode(&fragment)
			if err != nil {
				return err
			}
			i += len(fragment.Rows)
			if err = handleTableFragment(d, fragment); err != nil {
				return err
			}
			continue
		}

		if frameType == TableCompletionFrameType {
			completion := TableCompletion{}
			err = dec.Decode(&completion)
			if err != nil {
				return err
			}

			if err = handleTableCompletion(d, completion); err != nil {
				return err
			}

			break
		}

		return errors.ES(errors.OpQuery, errors.KInternal, "unexpected frame type %s, expected TableFragment or TableCompletion", frameType)
	}

	return nil
}

func (d *iterativeDataset) sendTable(tb query.IterativeTable) {
	select {
	case <-d.Context().Done():
		return
	case d.results <- query.TableResultSuccess(tb):
		return
	}
}

func (d *iterativeDataset) Tables() <-chan query.TableResult {
	return d.results
}

func (d *iterativeDataset) Close() error {
	d.cancel()
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

func handleDataTable(d *iterativeDataset, dec *json.Decoder) error {
	var dt DataTable
	if err := dec.Decode(&dt); err != nil {
		return err
	}

	if dt.Header.TableKind == PrimaryResultTableKind {
		return errors.ES(d.Op(), errors.KInternal, "received a DataTable frame for a primary result table")
	}
	switch dt.Header.TableKind {
	case QueryPropertiesKind:
		// When we get this, we want to store it and not send it to the user immediately.
		// We will wait until after the primary results (when we get the QueryCompletionInformation table) and then send it.
		res, err := newTable(d, dt)
		if err != nil {
			return err
		}
		d.queryProperties = iterativeWrapper{res}
	case QueryCompletionInformationKind:
		if d.queryProperties != nil {
			d.sendTable(d.queryProperties)
		}

		res, err := newTable(d, dt)
		if err != nil {
			return err
		}
		d.sendTable(iterativeWrapper{res})

	default:
		return errors.ES(d.Op(), errors.KInternal, "unknown secondary table - %s %s", dt.Header.TableName, dt.Header.TableKind)
	}

	return nil
}

func handleTableCompletion(d *iterativeDataset, tc TableCompletion) error {
	if d.currentTable == nil {
		return errors.ES(d.Op(), errors.KInternal, "received a TableCompletion frame while no streaming table was open")
	}
	if int(d.currentTable.Index()) != tc.TableId {
		return errors.ES(d.Op(), errors.KInternal, "received a TableCompletion frame for table %d while table %d was open", tc.TableId, int((d.currentTable).Index()))
	}

	d.currentTable.finishTable(tc.OneApiErrors, nil)

	d.currentTable = nil

	return nil
}

func handleTableFragment(d *iterativeDataset, tf TableFragment) error {
	if d.currentTable == nil {
		return errors.ES(d.Op(), errors.KInternal, "received a TableFragment frame while no streaming table was open")
	}

	d.currentTable.addRawRows(tf.Rows)

	return nil
}

func handleTableHeader(d *iterativeDataset, th TableHeader) error {
	if d.currentTable != nil {
		return errors.ES(d.Op(), errors.KInternal, "received a TableHeader frame while a streaming table was still open")
	}

	// Read the table header, set it as the current table, and send it to the user (so they can start reading rows)

	t, err := NewIterativeTable(d, th)
	if err != nil {
		return err
	}

	d.currentTable = t.(*iterativeTable)
	d.sendTable(d.currentTable)

	return nil
}
