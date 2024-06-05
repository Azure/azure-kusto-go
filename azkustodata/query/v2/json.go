package v2

import (
	"bufio"
	"bytes"
	"encoding/json"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"io"
)

type frameReader struct {
	reader bufio.Reader
	line   []byte
}

func newFrameReader(r io.Reader) (*frameReader, error) {
	br, err := prepareReadBuffer(r)
	if err != nil {
		return nil, err
	}
	return &frameReader{reader: *br}, nil
}

func prepareReadBuffer(r io.Reader) (*bufio.Reader, error) {
	br := bufio.NewReader(r)
	first, err := br.Peek(1)
	if err != nil {
		return nil, err
	}
	if len(first) == 0 {
		return nil, errors.ES(errors.OpUnknown, errors.KInternal, "No data")
	}

	if first[0] != '[' {
		all, err := io.ReadAll(br)
		if err != nil {
			return nil, err
		}
		return nil, errors.ES(errors.OpUnknown, errors.KInternal, "Got error: %v", string(all))
	}
	return br, nil
}

func (fr *frameReader) advance() error {
	line, err := fr.reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	if len(line) == 0 {
		return errors.ES(errors.OpUnknown, errors.KInternal, "No data")
	}

	if line[0] == ']' {
		return io.EOF
	}

	fr.line = line

	return nil
}

func (fr *frameReader) frameType() (string, error) {
	//"FrameType":"DataSetHeader"

	// find :
	colon := bytes.IndexByte(fr.line, ':')

	if colon == -1 {
		return "", errors.ES(errors.OpUnknown, errors.KInternal, "Missing colon in frame")
	}

	// find "
	quote := bytes.IndexByte(fr.line[colon+2:], '"')
	if quote == -1 {
		return "", errors.ES(errors.OpUnknown, errors.KInternal, "Missing quote in frame")
	}

	return string(fr.line[colon+2 : colon+2+quote]), nil
}

func assertToken(dec *json.Decoder, expected json.Token) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}
	if t != expected {
		return errors.ES(errors.OpUnknown, errors.KInternal, "Expected %v, got %v", expected, t)
	}
	return nil
}

func assertStringProperty(dec *json.Decoder, name string, value json.Token) error {
	if err := assertToken(dec, json.Token(name)); err != nil {
		return err
	}
	if err := assertToken(dec, value); err != nil {
		return err
	}
	return nil
}

func (fr *frameReader) validateDataSetHeader() error {
	dec := json.NewDecoder(bytes.NewReader(fr.line))
	if err := assertToken(dec, json.Delim('{')); err != nil {
		return err
	}

	if err := assertStringProperty(dec, "FrameType", json.Token(DataSetHeaderFrameType)); err != nil {
		return err
	}

	if err := assertStringProperty(dec, "IsProgressive", json.Token(false)); err != nil {
		return err
	}

	if err := assertStringProperty(dec, "Version", json.Token("v2.0")); err != nil {
		return err
	}

	if err := assertStringProperty(dec, "IsFragmented", json.Token(true)); err != nil {
		return err
	}

	if err := assertStringProperty(dec, "ErrorReportingPlacement", json.Token("EndOfTable")); err != nil {
		return err
	}

	return nil
}

// read a DataTable frame
func (fr *frameReader) readDataTable() (DataTable, error) {
	var dt DataTable
	err := json.Unmarshal(fr.line, &dt)
	return dt, err
}

// read a TableHeader frame
func (fr *frameReader) readTableHeader() (TableHeader, error) {
	var th TableHeader
	err := json.Unmarshal(fr.line, &th)
	return th, err
}

// read a TableFragment frame
func (fr *frameReader) readTableFragment() (TableFragment, error) {
	var tf TableFragment
	err := json.Unmarshal(fr.line, &tf)
	return tf, err
}

// read a TableCompletion frame
func (fr *frameReader) readTableCompletion() (TableCompletion, error) {
	var tc TableCompletion
	err := json.Unmarshal(fr.line, &tc)
	return tc, err
}

// read a DataSetCompletion frame
func (fr *frameReader) readDataSetCompletion() (DataSetCompletion, error) {
	var dc DataSetCompletion
	err := json.Unmarshal(fr.line, &dc)
	return dc, err
}

// read a TableProgress frame
func (fr *frameReader) readTableProgress() (TableProgress, error) {
	var tp TableProgress
	err := json.Unmarshal(fr.line, &tp)
	return tp, err
}
