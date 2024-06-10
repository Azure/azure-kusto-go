package v2

import (
	"bufio"
	"bytes"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/goccy/go-json"
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

	fr.line = line[1:]

	return nil
}

// peekFrameType reads the line directly, so it can be used to determine the frame type without parsing the entire frame.
func (fr *frameReader) peekFrameType() (FrameType, error) {
	colon := bytes.IndexByte(fr.line, ':')

	if colon == -1 {
		return "", errors.ES(errors.OpUnknown, errors.KInternal, "Missing colon in frame")
	}

	firstQuote := bytes.IndexByte(fr.line[colon+1:], '"')
	if firstQuote == -1 {
		return "", errors.ES(errors.OpUnknown, errors.KInternal, "Missing quote in frame")
	}
	secondQuote := bytes.IndexByte(fr.line[colon+1+firstQuote+1:], '"')
	if secondQuote == -1 {
		return "", errors.ES(errors.OpUnknown, errors.KInternal, "Missing quote in frame")
	}

	return FrameType(fr.line[colon+1+firstQuote+1 : colon+1+firstQuote+1+secondQuote]), nil
}

func (fr *frameReader) validateDataSetHeader() error {
	dec := json.NewDecoder(bytes.NewReader(fr.line))
	if err := assertToken(dec, json.Delim('{')); err != nil {
		return err
	}

	if err := assertStringProperty(dec, "FrameType", json.Token(string(DataSetHeaderFrameType))); err != nil {
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

func (fr *frameReader) unmarshal(i interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(fr.line))
	return dec.Decode(i)
}
