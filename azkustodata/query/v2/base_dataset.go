package v2

import (
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/Azure/azure-kusto-go/azkustodata/utils"
	"sync"
)

// DefaultFrameCapacity is the default capacity of the channel that receives frames from the Kusto service. Lower capacity means less memory usage, but might cause the channel to block if the frames are not consumed fast enough.
const DefaultFrameCapacity = 5

const version = "v2.0"
const PrimaryResultTableKind = "PrimaryResult"

// baseDataset contains the main logic of parsing a v2 dataset.
// v2 is made from a series of frames, which are decoded by turn.
// This supports both full and streaming datasets, via fullDataset and iterativeDataset  respectively.
type baseDataset struct {
	query.Dataset
	// DataSetHeader is the header of the data set. It's the first frame received.
	header *DataSetHeader
	// Completion is the completion status of the data set. It's the last frame received.
	completion *DataSetCompletion
	// queryProperties contains the information from the "QueryProperties" table.
	queryProperties []QueryProperties
	// queryCompletionInformation contains the information from the "QueryCompletionInformation" table.
	queryCompletionInformation []QueryCompletionInformation
	// currentTable is a reference to the current table, which is still receiving rows.
	currentTable table
	lock         utils.RWMutex
}

func newBaseDataset(dataset query.Dataset, fakeLock bool) *baseDataset {
	var lock utils.RWMutex = &utils.FakeMutex{}
	if !fakeLock {
		lock = &sync.RWMutex{}
	}
	return &baseDataset{
		Dataset: dataset,
		lock:    lock,
	}
}

// decodeTables decodes the frames from the frames channel and sends the results to the results channel.
func decodeTables(d dataset) {
	defer func() {
		d.Close()
		table := d.getCurrentTable()
		if table != nil {
			table.close([]OneApiError{})
		}
	}()

	op := d.Op()

	for {
		f := d.getNextFrame()

		if f == nil {
			break
		}

		if d.Completion() != nil {
			d.reportError(errors.ES(op, errors.KInternal, "received a frame after DataSetCompletion"))
			break
		}

		if header, ok := f.(*DataSetHeader); ok {
			if !parseDatasetHeader(d, header) {
				break
			}
		} else if completion, ok := f.(*DataSetCompletion); ok {
			if completion.HasErrors && completion.OneApiErrors != nil {
				for _, e := range completion.OneApiErrors {
					d.reportError(errors.E(op, errors.KInternal, &e))
				}
			}
			d.setCompletion(completion)
		} else if dt, ok := f.(*DataTable); ok {
			t, err := NewDataTable(d, dt)
			if err != nil {
				d.reportError(err)
				break
			}
			if t.Kind() == "PrimaryResult" {
				newTable, err := d.newTableFromHeader(&TableHeader{
					TableId:   dt.TableId,
					TableKind: dt.TableKind,
					TableName: dt.TableName,
					Columns:   dt.Columns,
				})
				if err != nil {
					d.reportError(err)
					break
				}

				d.setCurrentTable(newTable)
				d.onFinishHeader()
				newTable.addRawRows(dt.Rows)
				d.onFinishTable()
				d.setCurrentTable(nil)
			}

			err = d.parseSecondaryTable(t)
			if err != nil {
				d.reportError(err)
				// Continuing here as it's not a fatal error - secondary tables can have errors, but we still want the partial results.
				continue
			}
		} else if parsePrimaryTable(d, f) {
			continue
		} else if _, ok := f.(*TableProgress); ok {
			// ignore
		} else {
			err := errors.ES(op, errors.KInternal, "unknown frame type")
			d.reportError(err)
			break
		}
	}
}

func parsePrimaryTable(d dataset, f Frame) bool {
	table := d.getCurrentTable()

	if th, ok := f.(*TableHeader); ok {
		if table != nil {
			err := errors.ES(d.Op(), errors.KInternal, "received a TableHeader frame while a streaming table was still open")
			d.reportError(err)
			return false
		}
		if th.TableKind != PrimaryResultTableKind {
			err := errors.ES(d.Op(), errors.KInternal, "Received a TableHeader frame for a table that is not a primary result table")
			d.reportError(err)
			return false
		}

		t, err := d.newTableFromHeader(th)
		if err != nil {
			d.reportError(err)
			return false
		}
		d.setCurrentTable(t)
		d.onFinishHeader()
	} else if tf, ok := f.(*TableFragment); ok {
		if table == nil {
			err := errors.ES(d.Op(), errors.KInternal, "received a TableFragment frame while no streaming table was open")
			d.reportError(err)
			return false
		}
		if int(table.Ordinal()) != tf.TableId {
			err := errors.ES(d.Op(), errors.KInternal, "received a TableFragment frame for table %d while table %d was open", tf.TableId, int(table.Ordinal()))
			d.reportError(err)
		}

		table.addRawRows(tf.Rows)
	} else if tc, ok := f.(*TableCompletion); ok {
		if table == nil {
			err := errors.ES(d.Op(), errors.KInternal, "received a TableCompletion frame while no streaming table was open")
			d.reportError(err)
			return false
		}
		if int(table.Ordinal()) != tc.TableId {
			err := errors.ES(d.Op(), errors.KInternal, "received a TableCompletion frame for table %d while table %d was open", tc.TableId, int(table.Ordinal()))
			d.reportError(err)
		}

		table.close(tc.OneApiErrors)

		if table.RowCount() != tc.RowCount {
			err := errors.ES(d.Op(), errors.KInternal, "received a TableCompletion frame for table %d with row count %d while %d rows were received", tc.TableId, tc.RowCount, table.RowCount())
			d.reportError(err)
		}

		d.onFinishTable()
		d.setCurrentTable(nil)
	}

	return true
}

func parseDatasetHeader(d dataset, header *DataSetHeader) bool {
	if header.Version != version {
		d.reportError(errors.ES(d.Op(), errors.KInternal, "received a DataSetHeader frame that is not version 2"))
		return false
	}
	if header.IsProgressive {
		d.reportError(errors.ES(d.Op(), errors.KInternal, "received a DataSetHeader frame that is progressive"))
		return false
	}
	d.setHeader(header)

	return true
}

func (d *baseDataset) setHeader(dataSetHeader *DataSetHeader) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.header = dataSetHeader
}

func (d *baseDataset) Completion() *DataSetCompletion {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.completion
}

func (d *baseDataset) QueryProperties() []QueryProperties {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.queryProperties
}

func (d *baseDataset) QueryCompletionInformation() []QueryCompletionInformation {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.queryCompletionInformation
}

func (d *baseDataset) setCompletion(completion *DataSetCompletion) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.completion = completion
}

func (d *baseDataset) getCurrentTable() table {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.currentTable
}

func (d *baseDataset) setCurrentTable(currentTable table) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.currentTable = currentTable
}

func (d *baseDataset) Header() *DataSetHeader {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.header
}

func (d *baseDataset) onFinishHeader() {
}

func (d *baseDataset) onFinishTable() {
}

func (d *baseDataset) close() {
}
