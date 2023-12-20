package v2

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"io"
)

// this file contains the decoding of v2 frames, including parsing the format and converting the frames.

// UnmarshalJSON decodes a RawRow from JSON, it needs special handling because the row can be either a row (list of values) or an error (objecT).
func (r *RawRow) UnmarshalJSON(data []byte) error {
	var row []interface{}
	var errs struct {
		OneApiErrors []OneApiError `json:"OneApiErrors"`
	}

	var err error

	reader := bytes.NewReader(data)
	dec := json.NewDecoder(reader)
	dec.UseNumber()

	if err = dec.Decode(&row); err != nil {
		_, err := reader.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}

		if err = dec.Decode(&errs); err != nil {
			return err
		}
		r.Errors = errs.OneApiErrors
		r.Row = nil
		return nil
	}
	r.Row = row
	r.Errors = nil
	return nil
}

// Decode converts an unknown frame to a known frame type
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

// prepareReadBuffer checks for errors and returns a decoder
func prepareReadBuffer(r io.Reader) (io.Reader, error) {
	br := bufio.NewReader(r)
	peek, err := br.Peek(1)
	if err != nil {
		return nil, err
	}
	if peek[0] != '[' {
		all, err := io.ReadAll(br)
		if err != nil {
			return nil, err
		}
		return nil, errors.ES(errors.OpUnknown, errors.KInternal, "Got error: %v", string(all))
	}
	return br, nil

}

// decodeFrame decode a single frame from a decoder
func decodeFrame(dec *json.Decoder) (Frame, error) {
	frame := EveryFrame{}
	err := dec.Decode(&frame)
	if err != nil {
		return nil, err
	}

	f, err := frame.Decode()

	if err != nil {
		return nil, err
	}
	return f, nil
}

// readFramesIterative reads frames from a reader and sends them to a channel as they are read.
func readFramesIterative(br io.Reader, ch chan<- Frame) error {
	defer close(ch)

	dec := json.NewDecoder(&skipReader{r: br})
	dec.UseNumber()

	for {
		f, err := decodeFrame(dec)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		ch <- f
	}
}

// readFramesFull reads all frames from a reader and returns them as a slice.
func readFramesFull(r io.Reader) ([]Frame, error) {
	br, err := prepareReadBuffer(r)
	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(br)
	dec.UseNumber()

	var rawFrames []EveryFrame
	err = dec.Decode(&rawFrames)
	if err != nil {
		return nil, err
	}

	frames := make([]Frame, len(rawFrames))
	for i, f := range rawFrames {
		frames[i], err = f.Decode()
		if err != nil {
			return nil, err
		}
	}

	return frames, nil
}

// skipReader is an io.Reader that filters out specific characters from a wrapped
// io.Reader, specifically intended to convert from a JSON array to the jsonlines
// format by removing the opening '[', the closing ']', and the commas between objects.
type skipReader struct {
	r          io.Reader // Source reader from which the JSON array is read.
	afterStart bool      // afterStart indicates if the reader has passed the initial '['.
	shouldSkip bool      // shouldSkip indicates if the next character should be skipped.
	finished   bool      // finished indicates if the reader has reached the end of the stream.
}

// Read reads bytes from the wrapped io.Reader, filters out unwanted characters,
// and writes the resulting bytes into the provided byte slice.
func (s *skipReader) Read(buffer []byte) (int, error) {
	if s.finished {
		// If the stream has already been finished, return EOF.
		return 0, io.EOF
	}

	err := s.skipInitialBracket()
	if err != nil {
		return 0, err
	}

	// Create a temporary buffer to store bytes read from the source reader.
	tempBuffer := make([]byte, len(buffer))
	amt, err := s.r.Read(tempBuffer)
	if err != nil && err != io.EOF {
		// Return any read errors other than EOF.
		return 0, err
	}

	// Process the temporary buffer and filter out the characters.
	writeIndex := 0 // Index where the next byte will be written in 'buffer'.
	for i := 0; i < amt; i++ {
		if s.shouldSkip {
			// If we need to skip the next character,
			// check if it is either ']' or ',', and act accordingly.
			s.shouldSkip = false
			nextChar := tempBuffer[i]
			if nextChar == ']' {
				// If it is the closing bracket, the reader has finished.
				s.finished = true
				return writeIndex, nil
			} else if nextChar != ',' {
				return 0, fmt.Errorf("expected ',' between objects, got '%c'", nextChar)
			}
			// Continue to the next character if we successfully skipped ',' or ']'.
			continue
		}

		if tempBuffer[i] == '\n' {
			// If the character is a newline, it needs to be followed by a skip,
			// likely the next character is a ',' in the JSON array.
			s.shouldSkip = true
			continue
		}

		// Copy the character to the provided buffer 'buffer'.
		buffer[writeIndex] = tempBuffer[i]
		writeIndex++
	}

	// Return the number of bytes written to 'buffer' and any error encountered.
	return writeIndex, err
}

// skipInitialBracket skips the initial '[' at the beginning of the JSON array.
func (s *skipReader) skipInitialBracket() error {
	if !s.afterStart {
		s.afterStart = true

		initialByte := make([]byte, 1)
		amt, err := s.r.Read(initialByte)
		if err != nil {
			return err
		}
		if amt != 1 || initialByte[0] != '[' {
			return fmt.Errorf("expected '[' at the beginning of the stream, got '%c'", initialByte[0])
		}
	}
	return nil
}
