package ingest

import (
	"context"
	"io"
	"io/ioutil"
	"os"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/conn"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/filesystem"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/gzip"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"
)

// StreamingIngestion provides data ingestion from external sources into Kusto.
type StreamingIngestion struct {
	db    string
	table string

	client *kusto.Client

	streamConn *conn.Conn
}

// New is the constructor for Ingestion.
func NewStreaming(client *kusto.Client, db, table string) (*StreamingIngestion, error) {
	streamConn, err := conn.New(client.Endpoint(), client.Auth())
	if err != nil {
		return nil, err
	}

	i := &StreamingIngestion{
		db:         db,
		table:      table,
		client:     client,
		streamConn: streamConn,
	}

	return i, nil
}

// StreamingIngestProps is an optional argument to FromFile().
type StreamingIngestProps struct {
	isCompressed     bool
	format           DataFormat
	mappingReference string
	leaveOpen        bool
	clientRequestId  string
}

type StreamingIngestProp func(props *StreamingIngestProps)

func Format(format DataFormat) StreamingIngestProp {
	return func(props *StreamingIngestProps) {
		props.format = format
	}
}

func MappingReference(mappingReference string) StreamingIngestProp {
	return func(props *StreamingIngestProps) {
		props.mappingReference = mappingReference
	}
}

func LeaveOpen(leaveOpen bool) StreamingIngestProp {
	return func(props *StreamingIngestProps) {
		props.leaveOpen = leaveOpen
	}
}

func ClientRequestId(clientRequestId string) StreamingIngestProp {
	return func(props *StreamingIngestProps) {
		props.clientRequestId = clientRequestId
	}
}

func Compressed(isCompressed bool) StreamingIngestProp {
	return func(props *StreamingIngestProps) {
		props.isCompressed = isCompressed
	}
}

// FromFile allows uploading a data file for Kusto from either a local path or a blobstore URI path.
// This method is thread-safe.
func (i *StreamingIngestion) FromFile(ctx context.Context, fPath string, props ...StreamingIngestProp) (*StreamingResult, error) {
	streamingProps := StreamingIngestProps{}

	local, err := filesystem.IsLocalPath(fPath)
	if err != nil {
		return nil, err
	}

	if !local {
		return nil, errors.ES(errors.OpFileIngest, errors.KClientArgs, "blobstore paths are not supported for streaming")
	}

	discovery := filesystem.CompressionDiscovery(fPath)

	if discovery != properties.CTUnknown && discovery != properties.CTNone {
		streamingProps.isCompressed = true
	}

	for _, prop := range props {
		prop(&streamingProps)
	}

	streamingProps.leaveOpen = false // Since we open the file, we have to close it

	file, err := os.Open(fPath)
	if err != nil {
		return nil, err
	}

	return streamImpl(i.db, i.table, i.streamConn, ctx, file, streamingProps)
}

// FromReader allows uploading a data file for Kusto from an io.Reader. The content is uploaded to Blobstore and
// ingested after all data in the reader is processed. Content should not use compression as the content will be
// compressed with gzip. This method is thread-safe.
func (i *StreamingIngestion) FromReader(ctx context.Context, reader io.Reader, props ...StreamingIngestProp) (*StreamingResult, error) {
	streamingProps := StreamingIngestProps{}

	for _, prop := range props {
		prop(&streamingProps)
	}

	return streamImpl(i.db, i.table, i.streamConn, ctx, reader, streamingProps)
}

var (
	// ErrTooLarge indicates that the data being passed to a StreamBlock is larger than the maximum StreamBlock size of 4MiB.
	ErrTooLarge = errors.ES(errors.OpIngestStream, errors.KClientArgs, "cannot add data larger than 4MiB")
)

func streamImpl(db, table string, c *conn.Conn, ctx context.Context, payload io.Reader, props StreamingIngestProps) (*StreamingResult, error) {
	if !props.isCompressed {
		var closer io.ReadCloser
		var ok bool
		if closer, ok = payload.(io.ReadCloser); !ok {
			closer = ioutil.NopCloser(payload)
		}
		zw := gzip.New()
		zw.Reset(closer)

		payload = zw
	}

	//TODO - should we keep this check? Or maybe just for buffers?
	if seeker, ok := payload.(io.Seeker); ok {
		seek, err := seeker.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, errors.E(errors.OpIngestStream, errors.KClientArgs, err)
		}
		if seek > 4*mib {
			return nil, ErrTooLarge
		}
		_, err = seeker.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}
	}

	err := c.Write(ctx, db, table, payload, props.format, props.mappingReference, props.leaveOpen, props.clientRequestId)

	if err != nil {
		return nil, errors.E(errors.OpIngestStream, errors.KClientArgs, err)
	}

	return &StreamingResult{
		statusCode: Succeeded,
		database:   db,
		table:      table,
	}, nil
}
