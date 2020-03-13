package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/internal/frames"
)

// Decoder impolements frames.Decoder on the REST v2 frames.
type Decoder struct {
	columns table.Columns
	dec     *json.Decoder
	op      errors.Op
}

// Decode implements frames.Decoder.Decode(). This is not thread safe.
func (d *Decoder) Decode(ctx context.Context, r io.ReadCloser, op errors.Op) chan frames.Frame {
	d.columns = nil
	d.dec = json.NewDecoder(r)
	d.dec.UseNumber()
	d.op = op

	ch := make(chan frames.Frame, 1) // Channel is sized to 1. We read from the channel faster than we put on the channel.

	go func() {
		defer r.Close()
		defer close(ch)

		// We should receive a '[' indicating the start of the JSON list of Frames.
		t, err := d.dec.Token()
		if err == io.EOF {
			return
		}
		if err != nil {
			frames.Errorf(ctx, ch, err.Error())
			return
		}
		if t != json.Delim('[') {
			frames.Errorf(ctx, ch, "Expected '[' delimiter")
			return
		}

		// Extract the initial Frame, a dataSetHeader.
		dsh, err := d.dataSetHeader(ctx)
		if err != nil {
			frames.Errorf(ctx, ch, "first frame had error: %s", err)
			return
		}
		ch <- dsh

		// Start decoding the rest of the frames.
		d.decodeFrames(ctx, ch)
	}()

	return ch
}

// dataSetHeader decodes the byte stream into a DataSetHeader.
func (d *Decoder) dataSetHeader(ctx context.Context) (DataSetHeader, error) {
	dsh := DataSetHeader{Op: d.op}
	err := d.dec.Decode(&dsh)
	return dsh, err
}

// decodeFrames is used to decode incoming frames after the DataSetHeader has been received.
func (d *Decoder) decodeFrames(ctx context.Context, ch chan frames.Frame) {
	for d.dec.More() {
		if err := d.decodetoMap(ctx, ch); err != nil {
			frames.Errorf(ctx, ch, err.Error())
			return
		}
	}

	// Expect to recieve the end of our JSON list of frames, marked by the ']' delimiter.
	t, err := d.dec.Token()
	if err != nil {
		frames.Errorf(ctx, ch, err.Error())
		return
	}

	if t != json.Delim(']') {
		frames.Errorf(ctx, ch, "Expected ']' delimiter")
		return
	}
}

func (d *Decoder) decodetoMap(ctx context.Context, ch chan frames.Frame) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	m := frames.Pool.Get().(map[string]interface{})
	defer func() {
		select {
		case frames.PoolCh <- m:
		default:
		}
	}()

	err := d.dec.Decode(&m)
	if err != nil {
		return err
	}

	if _, ok := m[frames.FieldFrameType]; !ok {
		return fmt.Errorf("incoming frame did not have .FrameType")
	}
	if _, ok := m[frames.FieldFrameType].(string); !ok {
		return fmt.Errorf("incoming frame had FrameType that was not a string, was %T", m[frames.FieldFrameType])
	}

	switch ft := m[frames.FieldFrameType].(string); ft {
	case frames.TypeDataTable:
		dt := DataTable{}
		if err := dt.Unmarshal(m); err != nil {
			return err
		}
		dt.Op = d.op
		ch <- dt
	case frames.TypeDataSetCompletion:
		dc := DataSetCompletion{}
		if err := dc.Unmarshal(m); err != nil {
			return err
		}
		dc.Op = d.op
		ch <- dc
	case frames.TypeTableHeader:
		th := TableHeader{}
		if err := th.Unmarshal(m); err != nil {
			return err
		}
		th.Op = d.op
		d.columns = th.Columns
		ch <- th
	case frames.TypeTableFragment:
		tf := TableFragment{Columns: d.columns}
		if err := tf.Unmarshal(m); err != nil {
			return err
		}
		tf.Op = d.op
		ch <- tf
	case frames.TypeTableProgress:
		tp := TableProgress{}
		if err := tp.Unmarshal(m); err != nil {
			return err
		}
		tp.Op = d.op
		ch <- tp
	case frames.TypeTableCompletion:
		tc := TableCompletion{}
		if err := tc.Unmarshal(m); err != nil {
			return err
		}
		tc.Op = d.op
		d.columns = nil
		ch <- tc
	default:
		return fmt.Errorf("received FrameType %s, which we did not expect", ft)
	}
	return nil
}
