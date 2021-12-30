package ingest

import (
	"bytes"
	"context"
	"testing"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/errors"
)

type from int

const (
	fromFile from = iota
	fromBlob
	fromReader
)

func TestOptions(t *testing.T) {
	t.Parallel()

	client := kusto.NewMockClient()

	queuedClient, err := New(client, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	streamingClient, err := NewStreaming(client, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var tests = []struct {
		desc     string
		option   FileOption
		ingestor Ingestor
		from     from
		op       errors.Op
		kind     errors.Kind
	}{
		// We expect the two valid ones to succeed on options validations, and then fail on http errors
		{
			desc:     "Valid for streaming ingestor",
			option:   FileFormat(CSV),
			ingestor: streamingClient,
			from:     fromFile,
			op:       errors.OpIngestStream,
			kind:     errors.KHTTPError,
		},
		{
			desc:     "Valid for queued ingestor",
			option:   FileFormat(CSV),
			ingestor: streamingClient,
			from:     fromFile,
			op:       errors.OpIngestStream,
			kind:     errors.KHTTPError,
		},
		{
			desc:     "Invalid option for streaming ingestor from file",
			option:   FlushImmediately(),
			ingestor: streamingClient,
			from:     fromFile,
			op:       errors.OpFileIngest,
			kind:     errors.KClientArgs,
		},
		{
			desc:     "Invalid option for queued ingestor from file",
			option:   ClientRequestId("1234"),
			ingestor: queuedClient,
			from:     fromFile,
			op:       errors.OpFileIngest,
			kind:     errors.KClientArgs,
		},
		{
			desc:     "Invalid option for queued ingestor from reader",
			option:   DeleteSource(),
			ingestor: queuedClient,
			from:     fromReader,
			op:       errors.OpFileIngest,
			kind:     errors.KClientArgs,
		},
		{
			desc:     "Invalid option for queued ingestor from blob",
			option:   IgnoreSizeLimit(),
			ingestor: queuedClient,
			from:     fromBlob,
			op:       errors.OpFileIngest,
			kind:     errors.KClientArgs,
		},
		{
			desc:     "Invalid option for streaming ingestor from reader",
			option:   DeleteSource(),
			ingestor: streamingClient,
			from:     fromReader,
			op:       errors.OpFileIngest,
			kind:     errors.KClientArgs,
		},
	}

	for _, test := range tests {
		ctx := context.Background()
		var err error = nil
		switch test.from {
		case fromFile:
			_, err = test.ingestor.FromFile(ctx, "file_options_test.go", test.option)
		case fromBlob:
			_, err = test.ingestor.FromFile(ctx, "https://", test.option)
		case fromReader:
			_, err = test.ingestor.FromReader(ctx, bytes.NewReader([]byte{}), test.option)
		}
		if e, ok := err.(*errors.Error); ok {
			if e.Op != test.op {
				t.Errorf("%s: expected error op %s, got %s", test.desc, test.op, e.Op)
			}
			if e.Kind != test.kind {
				t.Errorf("%s: expected error want %s, got %s", test.desc, test.kind, e.Kind)
			}
		} else {
			t.Errorf("%s: expected error, got %v", test.desc, err)
		}

		t.Logf("Success - %s: %v", test.desc, err)
	}
}
