package kusto

// Reader provides a Reader object for Querying Kusto and turning it into Go objects and types.

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sync"

	"github.com/Azure/azure-kusto-go/kusto/errors"
	"github.com/Azure/azure-kusto-go/kusto/types"
)

// Column describes a column descriptor.
type Column struct {
	// Name is the name of the column.
	Name string
	// Type is the type of value stored in this column. These are described
	// via constants starting with CT<type>.
	Type string
}

// Columns is a set of columns.
type Columns []Column

func (c Columns) validate() error {
	if len(c) == 0 {
		return fmt.Errorf("Columns is zero length")
	}

	names := make(map[string]bool, len(c))

	for i, col := range c {
		if col.Name == "" {
			return fmt.Errorf("column[%d].Name is empty string", i)
		}
		if names[col.Name] {
			return fmt.Errorf("column[%d].Name(%s) is already defined", i, col.Name)
		}
		names[col.Name] = true

		if !validCT[col.Type] {
			return fmt.Errorf("column[%d] if of type %q, which is not valid", i, col.Type)
		}
	}
	return nil
}

// Row represents a row of Kusto data. Methods are not thread-safe.
type Row struct {
	columns     Columns
	columnNames []string
	row         types.KustoValues
	op          errors.Op
}

// ColumnNames returns a list of all column names.
func (r *Row) ColumnNames() []string {
	if r.columnNames == nil {
		for _, col := range r.columns {
			r.columnNames = append(r.columnNames, col.Name)
		}
	}
	return r.columnNames
}

// Values returns a list of KustoValues that represent the row.
func (r *Row) Values() types.KustoValues {
	return r.row
}

// Size returns the number of columns contained in Row.
func (r *Row) Size() int {
	return len(r.columns)
}

// Columns fetches all the columns in the row at once.
// The value of the kth column will be decoded into the kth argument to Columns.
// The number of arguments must be equal to the number of columns.
// Pass nil to specify that a column should be ignored.
// ptrs may be either the *string or *types.Column type. An error in decoding may leave
// some ptrs set and others not.
func (r *Row) Columns(ptrs ...interface{}) error {
	if len(ptrs) != len(r.columns) {
		return errors.E(r.op, errors.KClientArgs, fmt.Errorf(".Columns() requires %d arguments for this row, had %d", len(r.columns), len(ptrs)))
	}

	for i, col := range r.columns {
		if ptrs[i] == nil {
			continue
		}
		switch v := ptrs[i].(type) {
		case *string:
			*v = col.Name
		case *Column:
			v.Name = col.Name
			v.Type = col.Type
		default:
			return errors.E(r.op, errors.KClientArgs, fmt.Errorf(".Columns() received argument at position %d that was not a *string, *types.Columns: was %T", i, ptrs[i]))
		}
	}

	return nil
}

// ToStruct fetches the columns in a row into the fields of a struct. p must be a pointer to struct.
// The rules for mapping a row's columns into a struct's exported fields are:
//
//   1. If a field has a `kusto: "column_name"` tag, then decode column
//      'column_name' into the field. A special case is the `column_name: "-"`
//      tag, which instructs ToStruct to ignore the field during decoding.
//
//   2. Otherwise, if the name of a field matches the name of a column (ignoring case),
//      decode the column into the field.
//
// Slice and pointer fields will be set to nil if the source column is a null value, and a
// non-nil value if the column is not NULL. To decode NULL values of other types, use
// one of the kusto types (Int, Long, Dynamic, ...) as the type of the destination field.
// You can check the .Valid field of those types to see if the value was set.
func (r *Row) ToStruct(p interface{}) error {
	// Check if p is a pointer to a struct
	if t := reflect.TypeOf(p); t == nil || t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return errors.E(r.op, errors.KClientArgs, fmt.Errorf("type %T is not a pointer to a struct", p))
	}
	if len(r.columns) != len(r.row) {
		return errors.E(r.op, errors.KClientArgs, fmt.Errorf("row does not have the correct number of values(%d) for the number of columns(%d)", len(r.row), len(r.columns)))
	}

	return decodeToStruct(r.columns, r.row, p)
}

// Rows is a set of rows.
type Rows []*Row

// RowIterator is used to iterate over the returned Row objects returned by Kusto.
type RowIterator struct {
	op     errors.Op
	ctx    context.Context
	cancel context.CancelFunc

	// RequestHeader is the http.Header sent in the request to the server.
	RequestHeader http.Header
	// ResponseHeader is the http.header sent in the response from the server.
	ResponseHeader http.Header

	// The following channels represent input entering the RowIterator.
	inColumns    chan Columns
	inRows       chan []types.KustoValues
	inProgress   chan tableProgress
	inNonPrimary chan dataTable
	inCompletion chan dataSetCompletion
	inErr        chan error

	rows chan types.KustoValues

	mu sync.Mutex

	// progressive indicates if we are receiving a progressive stream or not.
	progressive bool
	// progress provides a progress indicator if the frames are progressive.
	progress tableProgress
	// nonPrimary contains dataTables that are not the primary table.
	nonPrimary map[string]dataTable
	// dsCompletion is the completion frame for a non-progressive query.
	dsCompletion dataSetCompletion

	columns Columns

	// error holds an error that was encountered. Once this is set, all calls on Rowiterator will
	// just return the error here.
	error error

	// mock hold our MockRows data if it has been provided for tests.
	mock *MockRows
}

func newRowIterator(ctx context.Context, cancel context.CancelFunc, execResp execResp, header dataSetHeader, op errors.Op) (*RowIterator, chan struct{}) {
	ri := &RowIterator{
		RequestHeader:  execResp.reqHeader,
		ResponseHeader: execResp.respHeader,

		op:           op,
		ctx:          ctx,
		cancel:       cancel,
		progressive:  header.IsProgressive,
		inColumns:    make(chan Columns, 1),
		inRows:       make(chan []types.KustoValues, 100),
		inProgress:   make(chan tableProgress, 1),
		inNonPrimary: make(chan dataTable, 1),
		inCompletion: make(chan dataSetCompletion, 1),
		inErr:        make(chan error),

		rows:       make(chan types.KustoValues, 1000),
		nonPrimary: make(map[string]dataTable),
	}
	columnsReady := ri.start()
	return ri, columnsReady
}

func (r *RowIterator) start() chan struct{} {
	done := make(chan struct{})
	once := sync.Once{}
	closeDone := func() {
		once.Do(func() { close(done) })
	}

	go func() {
		defer closeDone() // Catchall

		for {
			select {
			case <-r.ctx.Done():
			case columns := <-r.inColumns:
				r.columns = columns
				closeDone()
			case inRows, ok := <-r.inRows:
				if !ok {
					close(r.rows)
					return
				}
				for _, row := range inRows {
					select {
					case <-r.ctx.Done():
					case r.rows <- row:
					}
				}
			case table := <-r.inProgress:
				r.mu.Lock()
				r.progress = table
				r.mu.Unlock()
			case table := <-r.inNonPrimary:
				r.mu.Lock()
				r.nonPrimary[table.TableKind] = table
				r.mu.Unlock()
			case table := <-r.inCompletion:
				r.mu.Lock()
				r.dsCompletion = table
				r.mu.Unlock()
			case err := <-r.inErr:
				r.setError(err)
				close(r.rows)
				return
			}
		}
	}()
	return done
}

// Mock is used to tell the RowIterator to return specific data for tests. This is useful when building
// fakes of the client's Query() call for hermetic tests. This can only be called in a test or it will panic.
func (r *RowIterator) Mock(m *MockRows) error {
	if !isTest() {
		panic("cannot call Mock outside a test")
	}
	if r.mock != nil {
		return fmt.Errorf("RowIterator already has mock data")
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	r.mock = m
	return nil
}

// Do calls f for every row returned by the query. If f returns a non-nil error,
// iteration stops.
func (r *RowIterator) Do(f func(r *Row) error) error {
	for {
		row, err := r.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := f(row); err != nil {
			return err
		}
	}
}

// Stop is called to stop any further iteration. Always defer a Stop() call after
// receiving a RowIterator.
func (r *RowIterator) Stop() {
	r.cancel()
	return
}

// Next gets the next Row from the query. io.EOF is returned if there are no more entries in the output.
// Once Next() returns an error, all subsequent calls will return the same error.
func (r *RowIterator) Next() (*Row, error) {
	if err := r.getError(); err != nil {
		return nil, err
	}

	if r.mock != nil {
		if r.ctx.Err() != nil {
			return nil, r.ctx.Err()
		}
		return r.mock.nextRow()
	}

	select {
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case kvs, ok := <-r.rows:
		if !ok {
			if err := r.getError(); err != nil {
				return nil, err
			}
			return nil, io.EOF
		}
		return &Row{columns: r.columns, row: kvs, op: r.op}, nil
	}
}

func (r *RowIterator) getError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.error
}

func (r *RowIterator) setError(e error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.error = e
}

// Progress returns the progress of the query, 0-100%. This is only valid on Progressive data returns.
func (r *RowIterator) Progress() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.progress.TableProgress
}

// Progressive indicates if the RowIterator is unpacking progressive (streaming) frames.
func (r *RowIterator) Progressive() bool {
	return r.progressive
}

// getNonPrimary will return a non-primary dataTable if it exists from the last query. The non-primary table kinds
// are defined as constants starting with TK<name>.
// Returns io.ErrUnexpectedEOF if not found. May not have all tables until RowIterator has reached io.EOF.
func (r *RowIterator) getNonPrimary(ctx context.Context, tableKind string, tableName string) (dataTable, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, table := range r.nonPrimary {
		if table.TableKind == tableKind && table.TableName == tableName {
			return table, nil
		}
	}
	return dataTable{}, io.ErrUnexpectedEOF
}

func isTest() bool {
	if flag.Lookup("test.v") == nil {
		return false
	}
	return true
}
