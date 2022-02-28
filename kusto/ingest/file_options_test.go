package ingest

import (
	"bytes"
	"context"
	"testing"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)

	streamingClient, err := NewStreaming(client, "", "")
	require.NoError(t, err)

	managedClient, err := NewManaged(client, "", "")
	require.NoError(t, err)

	var tests = []struct {
		desc     string
		option   FileOption
		ingestor Ingestor
		from     from
		op       errors.Op
		kind     errors.Kind
	}{
		// We expect the valid streaming to succeed on options validations, and then fail on http errors
		{
			desc:     "Valid for streaming ingestor",
			option:   FileFormat(CSV),
			ingestor: streamingClient,
			from:     fromFile,
			op:       errors.OpIngestStream,
			kind:     errors.KHTTPError,
		},
		// We expect the valid managed streaming to succeed on options validations, and then fail on http errors
		{
			desc:     "Valid for managed streaming ingestor",
			option:   FileFormat(CSV),
			ingestor: streamingClient,
			from:     fromFile,
			op:       errors.OpIngestStream,
			kind:     errors.KHTTPError,
		},
		// We expect the valid ingest to succeed on options validations, and then fail on blob store error
		{
			desc:     "Valid for queued ingestor",
			option:   FileFormat(CSV),
			ingestor: queuedClient,
			from:     fromFile,
			op:       errors.OpFileIngest,
			kind:     errors.KBlobstore,
		},
		{
			desc:     "Invalid option for streaming ingestor from file",
			option:   FlushImmediately(),
			ingestor: streamingClient,
			from:     fromFile,
			op:       errors.OpIngestStream,
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
			desc:     "Invalid option for managed ingestor from file",
			option:   DeleteSource(),
			ingestor: managedClient,
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
			option:   DeleteSource(),
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
			op:       errors.OpIngestStream,
			kind:     errors.KClientArgs,
		},
		{
			desc:     "Invalid option for managed ingestor from reader",
			option:   DeleteSource(),
			ingestor: managedClient,
			from:     fromReader,
			op:       errors.OpFileIngest,
			kind:     errors.KClientArgs,
		},
	}

	for _, test := range tests {
		test := test // capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
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
				assert.Equal(t, test.op, e.Op)
				assert.Equal(t, test.kind, e.Kind)
			} else {
				assert.Fail(t, "Expected errors.Error, got %v", err)
			}

			t.Logf("Success - %s: %v", test.desc, err)
		})

	}
}
