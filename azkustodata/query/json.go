package query

import (
	"encoding/json"
	"fmt"
	"io"
)

func (f *EveryFrame) Decode() (Frame, error) {
	switch f.FrameType {
	case DataSetHeaderFrameType:
		return &DataSetHeader{
			IsProgressive:           f.IsProgressive,
			Version:                 f.Version,
			IsFragmented:            f.IsFragmented,
			ErrorReportingPlacement: f.ErrorReportingPlacement,
		}, nil
	case DataTableFrameType:
		return &DataTable{
			TableId:   f.TableId,
			TableKind: f.TableKind,
			TableName: f.TableName,
			Columns:   f.Columns,
			Rows:      f.Rows,
		}, nil
	case TableHeaderFrameType:
		return &TableHeader{
			TableId:   f.TableId,
			TableKind: f.TableKind,
			TableName: f.TableName,
			Columns:   f.Columns,
		}, nil
	case TableFragmentFrameType:
		return &TableFragment{
			TableFragmentType: f.TableFragmentType,
			TableId:           f.TableId,
			Rows:              f.Rows,
		}, nil
	case TableCompletionFrameType:
		return &TableCompletion{
			TableId:      f.TableId,
			RowCount:     f.RowCount,
			OneApiErrors: f.OneApiErrors,
		}, nil
	case DataSetCompletionFrameType:
		return &DataSetCompletion{
			HasErrors:    f.HasErrors,
			Cancelled:    f.Cancelled,
			OneApiErrors: f.OneApiErrors,
		}, nil
	default:
		return nil, fmt.Errorf("unknown frame type: %s", f.FrameType)
	}
}

func ReadFrames(r io.Reader, ch chan<- Frame) error {
	defer close(ch)

	dec := json.NewDecoder(&skipReader{r: r})

	for {
		frame := EveryFrame{}
		err := dec.Decode(&frame)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		f, err := frame.Decode()

		if err != nil {
			return err
		}

		ch <- f
	}
}

type skipReader struct {
	r          io.Reader
	afterStart bool
}

func (s *skipReader) Read(p []byte) (n int, err error) {
	// skip '[' at the beginning
	if !s.afterStart {
		s.afterStart = true

		buf := make([]byte, 1)
		amt, err := s.r.Read(buf)
		if err != nil {
			return 0, err
		}
		if amt != 1 || buf[0] != '[' {
			return 0, fmt.Errorf("expected '[' at the beginning of the stream, got '%c'", buf[0])
		}
	}

	cp := make([]byte, len(p)+1)
	amt, err := s.r.Read(cp[:len(p)])
	pIndex := 0
	skipNext := false

	if err != nil {
		return 0, err
	}

	if cp[amt-1] == '\n' {
		_, err = s.r.Read(cp[amt:])
		if err != nil {
			return 0, err
		}
		if cp[amt] != ']' && cp[amt] != ',' {
			return 0, fmt.Errorf("expected ']' or ',' at the end of the stream, got '%c'", cp[amt])
		}
		amt++
	}

	for i := 0; i < amt; i++ {
		if skipNext {
			skipNext = false
			next := cp[i]
			if next == ']' {
				return i, io.EOF
			} else if next != ',' {
				return 0, fmt.Errorf("expected ',' between frames, got '%c'", next)
			}
			continue
		}
		if cp[i] == '\n' {
			skipNext = true
		}
		p[pIndex] = cp[i]
		pIndex++

	}

	return pIndex, err
}
