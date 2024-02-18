package v2

import (
	"bufio"
	"bytes"
	"encoding/json"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"io"
)

// this file contains the decoding of v2 frames, including parsing the format and converting the frames.

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

// readFramesIterative reads frames from a reader and sends them to a channel as they are read.
func readFramesIterative(reader io.Reader, ch chan<- *EveryFrame) error {
	defer close(ch)

	// Crazily enough, json.Decoder always puts THE ENTIRE READER IN MEMORY
	// So we have to manually split the reader into lines and decode each line with a new decoder

	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 64*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()

		line, err := handleKustoJson(line)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()

		frame := EveryFrame{}
		err = dec.Decode(&frame)

		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		ch <- &frame
	}

	return nil
}

func handleKustoJson(line []byte) ([]byte, error) {
	if len(line) == 0 {
		return nil, errors.ES(errors.OpUnknown, errors.KInternal, "Unexpected empty line when reading json")
	}

	if line[0] == ']' {
		return nil, io.EOF
	}

	if line[0] != '[' && line[0] != ',' {
		return nil, errors.ES(errors.OpUnknown, errors.KInternal, "Unexpected prefix when reading json: %v", string(line))
	}

	return line[1:], nil
}
