package frames

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/data/value"
	"github.com/Azure/azure-kusto-go/kusto/data/errors"
)

const (
	// TypeDataTable is the .FrameType that indicates a Kusto DataTable.
	TypeDataTable = "DataTable"
	// TypeDataSetCompletion is the .FrameType that indicates a Kusto DataSetCompletion.
	TypeDataSetCompletion = "DataSetCompletion"
	// TypeDataSetHeader is the .FrameType that indicates a Kusto DataSetHeader.
	TypeDataSetHeader = "DataSetHeader"
	// TypeTableHeader is the .FrameType that indicates a Kusto TableHeader.
	TypeTableHeader = "TableHeader"
	// TypeTableFragment is the .FrameType that indicates a Kusto TableFragment.
	TypeTableFragment = "TableFragment"
	// TypeTableProgress is the .FrameType that indicates a Kusto TableProgress.
	TypeTableProgress = "TableProgress"
	// TypeTableCompletion is the .FrameType that indicates a Kusto TableCompletion.
	TypeTableCompletion = "TableCompletion"
)

// These constants represent keys for fields when unmarshalling various JSON dicts representing Kusto frames.
const (
	FieldFrameType         = "FrameType"
	FieldTableID           = "TableId"
	FieldTableKind         = "TableKind"
	FieldTableName         = "TableName"
	FieldColumns           = "Columns"
	FieldRows              = "Rows"
	FieldColumnName        = "ColumnName"
	FieldColumnType        = "ColumnType"
	FieldCount             = "FieldCount"
	FieldTableFragmentType = "TableFragmentType"
	FieldTableProgress     = "TableProgress"
	FieldRowCount          = "RowCount"
)

// TableKind describes the kind of table.
type TableKind string

const (
	// QueryProperties is a dataTable.TableKind that contains properties about the query itself.
	// The dataTable.TableName is usually ExtendedProperties.
	QueryProperties TableKind = "QueryProperties"
	// PrimaryResult is a dataTable.TableKind that contains the query information the user wants.
	// The dataTable.TableName is PrimaryResult.
	PrimaryResult TableKind = "PrimaryResult"
	// QueryCompletionInformation contains information on how long the query took.
	// The dataTable.TableName is QueryCompletionInformation.
	QueryCompletionInformation TableKind = "QueryCompletionInformation"
	QueryTraceLog              TableKind = "QueryTraceLog"
	QueryPerfLog               TableKind = "QueryPerfLog"
	TableOfContents            TableKind = "TableOfContents"
	QueryPlan                  TableKind = "QueryPlan"
	ExtendedProperties         TableKind = "@ExtendedProperties"
	UnknownTableKind           TableKind = "Unknown"
)

var tkDetection = map[TableKind]bool{
	QueryProperties:            true,
	PrimaryResult:              true,
	QueryCompletionInformation: true,
	QueryTraceLog:              true,
	QueryPerfLog:               true,
	TableOfContents:            true,
	QueryPlan:                  true,
	UnknownTableKind:           true,
}

// Decoder provides a function that will decode an incoming data stream and return a channel of Frame objects.
type Decoder interface {
	// Decode decodes an io.Reader representing a stream of Kusto frames into our Frame representation.
	// The type and order of frames is dependent on the REST interface version and the progressive frame settings.
	Decode(ctx context.Context, r io.ReadCloser, op errors.Op) chan Frame
}

// Frame is a type of Kusto frame as defined in the reference document.
type Frame interface {
	IsFrame()
}

// Pool provides a package level pool of map[string]interface{} to lower our allocations for decoding.
var Pool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{}, 10)
	},
}

// PoolCh provides a package level channel that sends a unused map to the package pool,
// allowing all instances of decoder to share the same map pool.
var PoolCh = make(chan map[string]interface{}, 100)

// poolIn provides a background goroutine that pushes unused maps into our pool for resuse.
func poolIn() {
	for m := range PoolCh {
		for k := range m {
			delete(m, k)
		}
		Pool.Put(m)
	}
}

// init starts our poolIn background goroutine.
func init() {
	// TODO(jdoak): At some point I will need to sit down and find the optimal value.
	for i := 0; i < 5; i++ {
		go poolIn()
	}
}

// Error is not actually a Kusto frame, but is used to signal the end of a stream
// where we encountered an error. Error implements error.
type Error struct {
	Msg string
}

// Error implements error.Error().
func (e Error) Error() string {
	return e.Msg
}

func (Error) IsFrame() {}

// Errorf write a frames.Error to ch with fmt.Sprint(s, a...).
func Errorf(ctx context.Context, ch chan Frame, s string, a ...interface{}) {
	select {
	case <-ctx.Done():
	case ch <- Error{Msg: fmt.Sprintf(s, a...)}:
	}
}

// Conversion has keys that are Kusto data types, represented by CT* constants
// to functions that convert the JSON value into our concrete KustoValue types.
var Conversion = map[types.Column]func(i interface{}) (value.Kusto, error){
	types.Bool: func(i interface{}) (value.Kusto, error) {
		v := value.Bool{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	types.DateTime: func(i interface{}) (value.Kusto, error) {
		v := value.DateTime{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	types.Dynamic: func(i interface{}) (value.Kusto, error) {
		v := value.Dynamic{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	types.GUID: func(i interface{}) (value.Kusto, error) {
		v := value.GUID{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	types.Int: func(i interface{}) (value.Kusto, error) {
		v := value.Int{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	types.Long: func(i interface{}) (value.Kusto, error) {
		v := value.Long{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	types.Real: func(i interface{}) (value.Kusto, error) {
		v := value.Real{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	types.String: func(i interface{}) (value.Kusto, error) {
		v := value.String{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	types.Timespan: func(i interface{}) (value.Kusto, error) {
		v := value.Timespan{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	types.Decimal: func(i interface{}) (value.Kusto, error) {
		v := value.Decimal{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
}
