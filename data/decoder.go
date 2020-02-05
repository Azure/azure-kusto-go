package data

// decoder.go provides a JSON stream decoder into Go native Kusto Frames.

// Performance note: This JSON decoding is not anywhere close to the fastest decoder
// that we can make. We are using convenience methods like map[string]interface{} that
// require some allocation and garbage collection we could throw out.
// However, at the time of this writing (10/16/2019) the Kusto service sends slower
// than the decoder decodes. Tested our outgoing buffers and we have an average of 500ns between
// complete frame receipt from the upstream (dec.More()) and the next frame.
// At least with Progressive frames, there does not seem to be much reason to try and switch this
// out for something faster but also harder to support/read.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/Azure/azure-kusto-go/data/errors"
	"github.com/Azure/azure-kusto-go/data/types"
)

// pool provides a package level pool of map[string]interface{} to lower our allocations for decoding.
var pool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{}, 10)
	},
}

// poolCh provides a package level channel that sends a unused map to the package pool,
// allowing all instances of decoder to share the same map pool.
var poolCh = make(chan map[string]interface{}, 100)

// poolIn provides a background goroutine that pushes unused maps into our pool for resuse.
func poolIn() {
	for m := range poolCh {
		for k := range m {
			delete(m, k)
		}
		pool.Put(m)
	}
}

// init starts our poolIn background goroutine.
func init() {
	// TODO(jdoak): At some point I will need to sit down and find the optimal value.
	for i := 0; i < 5; i++ {
		go poolIn()
	}
}

// decoder reads a JSON stream from Kusto and decodes them into Frames.
type decoder struct {
	dec *json.Decoder
	// columns is used to store the columns from a tableHeader needed on the
	// decode of the next TableFragment frame.
	columns Columns
	op      errors.Op
}

// newDecoder creates a new frame decoder. Create a new decoder for each stream.
func newDecoder(b io.Reader, op errors.Op) *decoder {
	dec := json.NewDecoder(b)
	dec.UseNumber()
	dec.DisallowUnknownFields()

	d := &decoder{
		dec: dec,
		op:  op,
	}
	return d
}

// decode returns a channel that decodes the incoming byte stream into Kusto Frames.
// If the frame is an errorFrame then the byte stream had a problem. If you cancel the passed
// context an error frame may not be returned and the stream should be considered broken.
// Always look for a dataSetCompletion frame at the end.
func (d *decoder) decodeV2(ctx context.Context) chan frame {
	ch := make(chan frame, 1) // Channel is sized to 1. We read from the channel faster than we put on the channel.

	go func() {
		defer close(ch)

		// We should receive a '[' indicating the start of the JSON list of Frames.
		t, err := d.dec.Token()
		if err == io.EOF {
			return
		}
		if err != nil {
			d.error(ctx, ch, err.Error())
			return
		}
		if t != json.Delim('[') {
			d.error(ctx, ch, "Expected '[' delimiter")
			return
		}

		// Extract the initial Frame, a dataSetHeader.
		dsh, err := d.dataSetHeader()
		if err != nil {
			d.error(ctx, ch, "first frame had error: %s", err)
			return
		}
		ch <- dsh

		// Start decoding the rest of the frames.
		d.decodeFrames(ctx, ch)
	}()

	return ch
}

type columnV1 struct {
	DataType   string
	ColumnType string
	ColumnName string
}

type dataTableV1 struct {
	TableName string
	Columns   []columnV1
	Rows      []interface{}
}

type dataSetV1 struct {
	Tables []dataTableV1
}

func (d *decoder) decodeV1(ctx context.Context) chan frame {
	ch := make(chan frame, 100)

	go func() {
		defer close(ch)
		var dataset dataSetV1
		if e := d.dec.Decode(&dataset); e != nil {
			panic(e)
		}

		for index, table := range dataset.Tables {
			dt := dataTable{
				baseFrame: baseFrame{
					FrameType: ftDataTable,
				},
				TableID:   index,
				TableKind: tkPrimaryResult,
				TableName: table.TableName,
				op:        0,
			}

			columns := make([]Column, len(table.Columns))
			for i, c := range table.Columns {
				columns[i] = Column{
					ColumnName: c.ColumnName,
					ColumnType: c.ColumnType,
				}
			}
			dt.Columns = columns

			if e := dt.unmarshalRows(table.Rows); e != nil {
				panic(e)
			}

			ch <- dt
		}
	}()

	return ch
}

// error write an errorFrame to ch with fmt.Sprint(s, a...).
func (d *decoder) error(ctx context.Context, ch chan frame, s string, a ...interface{}) {
	select {
	case <-ctx.Done():
	case ch <- errorFrame{Msg: fmt.Sprintf(s, a...)}:
	}
}

// dataSetHeader decodes the byte stream into a dataSetHeader.
func (d *decoder) dataSetHeader() (dataSetHeader, error) {
	dsh := dataSetHeader{op: d.op}
	err := d.dec.Decode(&dsh)
	return dsh, err
}

// decodeFrames is used to decode incoming frames after the dataSetHeader has been received.
func (d *decoder) decodeFrames(ctx context.Context, ch chan frame) {
	for d.dec.More() {
		if err := d.decodeToMap(ctx, ch); err != nil {
			d.error(ctx, ch, err.Error())
			return
		}
	}

	// Expect to recieve the end of our JSON list of frames, marked by the ']' delimiter.
	t, err := d.dec.Token()
	if err != nil {
		d.error(ctx, ch, err.Error())
		return
	}

	if t != json.Delim(']') {
		d.error(ctx, ch, "Expected ']' delimiter")
		return
	}
}

func (d *decoder) decodeToMap(ctx context.Context, ch chan frame) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	m := pool.Get().(map[string]interface{})
	defer func() {
		select {
		case poolCh <- m:
		default:
		}
	}()

	err := d.dec.Decode(&m)
	if err != nil {
		return err
	}

	if _, ok := m[kFrameType]; !ok {
		return fmt.Errorf("incoming frame did not have .FrameType")
	}
	if _, ok := m[kFrameType].(string); !ok {
		return fmt.Errorf("incoming frame had FrameType that was not a string, was %T", m[kFrameType])
	}

	switch ft := m[kFrameType].(string); ft {
	case ftDataTable:
		dt := dataTable{}
		if err := dt.Unmarshal(m); err != nil {
			return err
		}
		dt.op = d.op
		ch <- dt
	case ftDataSetCompletion:
		dc := dataSetCompletion{}
		if err := dc.Unmarshal(m); err != nil {
			return err
		}
		dc.op = d.op
		ch <- dc
	case ftTableHeader:
		th := tableHeader{}
		if err := th.Unmarshal(m); err != nil {
			return err
		}
		th.op = d.op
		d.columns = th.Columns
		ch <- th
	case ftTableFragment:
		tf := tableFragment{columns: d.columns}
		if err := tf.Unmarshal(m); err != nil {
			return err
		}
		tf.op = d.op
		ch <- tf
	case ftTableProgress:
		tp := tableProgress{}
		if err := tp.Unmarshal(m); err != nil {
			return err
		}
		tp.op = d.op
		ch <- tp
	case ftTableCompletion:
		tc := tableCompletion{}
		if err := tc.Unmarshal(m); err != nil {
			return err
		}
		tc.op = d.op
		d.columns = nil
		ch <- tc
	default:
		return fmt.Errorf("received FrameType %s, which we did not expect", ft)
	}
	return nil
}

// These constants represent the value type stored in a Column.
const (
	// CTBool indicates that a Column stores a Kusto boolean value.
	CTBool = "bool"
	// CTDateTime indicates that a Column stores a Kusto datetime value.
	CTDateTime = "datetime"
	// CTDynamic indicates that a Column stores a Kusto dynamic value.
	CTDynamic = "dynamic"
	// CTGUID indicates that a Column stores a Kusto guid value.
	CTGUID = "guid"
	// CTInt indicates that a Column stores a Kusto int value.
	CTInt = "int"
	// CTLong indicates that a Column stores a Kusto long value.
	CTLong = "long"
	// CTReal indicates that a Column stores a Kusto real value.
	CTReal = "real"
	// CTString indicates that a Column stores a Kusto string value.
	CTString = "string"
	// CTTimespan indicates that a Column stores a Kusto timespan value.
	CTTimespan = "timespan"
	// CTDecimal indicates that a Column stores a Kusto decimal value.
	CTDecimal = "decimal" // We have NOT written a conversion
)

// conversion has keys that are Kusto data types, represented by CT* constants
// to functions that convert the JSON value into our concrete KustoValue types.
var conversion = map[string]func(i interface{}) (types.KustoValue, error){
	CTBool: func(i interface{}) (types.KustoValue, error) {
		v := types.Bool{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	CTDateTime: func(i interface{}) (types.KustoValue, error) {
		v := types.DateTime{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	CTDynamic: func(i interface{}) (types.KustoValue, error) {
		v := types.Dynamic{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	CTGUID: func(i interface{}) (types.KustoValue, error) {
		v := types.GUID{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	CTInt: func(i interface{}) (types.KustoValue, error) {
		v := types.Int{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	CTLong: func(i interface{}) (types.KustoValue, error) {
		v := types.Long{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	CTReal: func(i interface{}) (types.KustoValue, error) {
		v := types.Real{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	CTString: func(i interface{}) (types.KustoValue, error) {
		v := types.String{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
	CTTimespan: func(i interface{}) (types.KustoValue, error) {
		v := types.Timespan{}
		if err := v.Unmarshal(i); err != nil {
			return nil, err
		}
		return v, nil
	},
}
