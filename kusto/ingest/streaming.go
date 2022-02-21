package ingest

import (
	"context"
	"io"
	"io/ioutil"
	"os"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/conn"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/filesystem"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/gzip"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"
)

// Streaming provides data ingestion from external sources into Kusto.
type Streaming struct {
	db         string
	table      string
	client     QueryClient
	streamConn *conn.Conn
}

// NewStreaming is the constructor for Streaming.
// More information can be found here:
// https://docs.microsoft.com/en-us/azure/kusto/management/create-ingestion-mapping-command
func NewStreaming(client QueryClient, db, table string) (*Streaming, error) {
	streamConn, err := conn.New(client.Endpoint(), client.Auth())
	if err != nil {
		return nil, err
	}

	i := &Streaming{
		db:         db,
		table:      table,
		client:     client,
		streamConn: streamConn,
	}

	return i, nil
}

// FromFile allows uploading a data file for Kusto from either a local path or a blobstore URI path.
// This method is thread-safe.
func (i *Streaming) FromFile(ctx context.Context, fPath string, options ...FileOption) (*Result, error) {
	local, err := filesystem.IsLocalPath(fPath)
	if err != nil {
		return nil, err
	}

	if !local {
		return nil, errors.ES(errors.OpFileIngest, errors.KClientArgs, "blobstore paths are not supported for streaming")
	}
	props := i.newProp()

	for _, option := range options {
		err := option.Run(&props, StreamingClient, FromFile)
		if err != nil {
			return nil, err
		}
	}

	compression := filesystem.CompressionDiscovery(fPath)
	if compression != properties.CTNone {
		props.Source.DontCompress = true
	}

	err = filesystem.CompleteFormatFromFileName(&props, fPath)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(fPath)
	if err != nil {
		return nil, err
	}

	return streamImpl(i.streamConn, ctx, file, props)
}

// FromReader allows uploading a data file for Kusto from an io.Reader. The content is uploaded to Blobstore and
// ingested after all data in the reader is processed. Content should not use compression as the content will be
// compressed with gzip. This method is thread-safe.
func (i *Streaming) FromReader(ctx context.Context, reader io.Reader, options ...FileOption) (*Result, error) {
	props := i.newProp()

	for _, prop := range options {
		err := prop.Run(&props, StreamingClient, FromReader)
		if err != nil {
			return nil, err
		}
	}

	return streamImpl(i.streamConn, ctx, reader, props)
}

func streamImpl(c *conn.Conn, ctx context.Context, payload io.Reader, props properties.All) (*Result, error) {
	compress := !props.Source.DontCompress
	if compress {
		var closer io.ReadCloser
		var ok bool
		if closer, ok = payload.(io.ReadCloser); !ok {
			closer = ioutil.NopCloser(payload)
		}
		zw := gzip.New()
		zw.Reset(closer)

		payload = zw
	}

	err := c.Write(ctx, props.Ingestion.DatabaseName, props.Ingestion.TableName, payload, props.Ingestion.Additional.Format,
		props.Ingestion.Additional.IngestionMappingRef,
		props.Streaming.ClientRequestId)

	if err != nil {
		if e, ok := err.(*errors.Error); ok {
			return nil, e
		}
		return nil, errors.E(errors.OpIngestStream, errors.KClientArgs, err)
	}

	result := newResult()
	result.putProps(props)
	result.record.Status = "Success"

	return result, nil
}

func (i *Streaming) newProp() properties.All {
	return properties.All{
		Ingestion: properties.Ingestion{
			DatabaseName: i.db,
			TableName:    i.table,
		},
	}
}
