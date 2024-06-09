package v2

import (
	"bufio"
	"bytes"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/goccy/go-json"
	"io"
)

func unmarhsalRow(
	buffer []byte,
	decoder *json.Decoder,
	onField func(field int, t json.Token) error) error {
	for {
		t, err := decoder.Token()
		if err != nil {
			return err
		}

		// end of outer array
		if t != json.Delim('[') {
			break
		}

		field := 0

		for ; decoder.More(); field++ {

			t, err = decoder.Token()
			if err != nil {
				return err
			}

			// If it's a nested object, just make it into a byte array
			if t == json.Delim('[') || t == json.Delim('{') {
				initialOffset := decoder.InputOffset() - 1
				for decoder.More() {
					_, err := decoder.Token()
					if err != nil {
						return err
					}
				}
				_, err := decoder.Token()
				if err != nil {
					return err
				}

				finalOffset := decoder.InputOffset()

				err = onField(field, json.Token(buffer[initialOffset:finalOffset]))
				if err != nil {
					return err
				}
				continue
			}

			err := onField(field, t)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

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

func (fr *frameReader) frameType() (FrameType, error) {
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

	return FrameType(fr.line[colon+2 : colon+2+quote]), nil
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

func getStringProperty(dec *json.Decoder, name string) (string, error) {
	if err := assertToken(dec, json.Token(name)); err != nil {
		return "", err
	}
	t, err := dec.Token()
	if err != nil {
		return "", err
	}
	if s, ok := t.(string); ok {
		return s, nil
	}
	return "", errors.ES(errors.OpUnknown, errors.KInternal, "Expected string, got %v", t)
}

func getIntProperty(dec *json.Decoder, name string) (int, error) {
	if err := assertToken(dec, json.Token(name)); err != nil {
		return 0, err
	}
	t, err := dec.Token()
	if err != nil {
		return 0, err
	}
	if s, ok := t.(json.Number); ok {
		i, err := s.Int64()
		if err != nil {
			return 0, err
		}
		return int(i), nil
	}
	return 0, errors.ES(errors.OpUnknown, errors.KInternal, "Expected string, got %v", t)
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

func (fr *frameReader) readQueryProperties() (DataTable, error) {
	tb := DataTable{}
	err := fr.unmarshal(&tb)
	return tb, err
}

func (fr *frameReader) readQueryCompletionInformation() (DataTable, error) {
	tb := DataTable{}
	err := fr.unmarshal(&tb)
	return tb, err
}
