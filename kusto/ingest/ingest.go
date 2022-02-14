package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/conn"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/filesystem"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/resources"
	"github.com/google/uuid"
)

type Ingestor interface {
	FromFile(ctx context.Context, fPath string, options ...FileOption) (*Result, error)
	FromReader(ctx context.Context, reader io.Reader, options ...FileOption) (*Result, error)
}

// Ingestion provides data ingestion from external sources into Kusto.
type Ingestion struct {
	db    string
	table string

	client QueryClient
	mgr    *resources.Manager

	fs *filesystem.Ingestion

	connMu     sync.Mutex
	streamConn *conn.Conn

	bufferSize int
	maxBuffers int
}

// Option is an optional argument to New().
type Option func(s *Ingestion)

// WithStaticBuffer configures the ingest client to upload data to Kusto using a set of one or more static memory buffers with a fixed size.
func WithStaticBuffer(bufferSize int, maxBuffers int) Option {
	return func(s *Ingestion) {
		s.bufferSize = bufferSize
		s.maxBuffers = maxBuffers
	}
}

// New is a constructor for Ingestion.
func New(client QueryClient, db, table string, options ...Option) (*Ingestion, error) {
	mgr, err := resources.New(client)
	if err != nil {
		return nil, err
	}

	i := &Ingestion{
		client: client,
		mgr:    mgr,
		db:     db,
		table:  table,
	}

	for _, option := range options {
		option(i)
	}

	fs, err := filesystem.New(db, table, mgr, filesystem.WithStaticBuffer(i.bufferSize, i.maxBuffers))
	if err != nil {
		return nil, err
	}

	i.fs = fs

	return i, nil
}

func (i *Ingestion) prepForIngestion(ctx context.Context, options []FileOption, scope SourceScope) (*Result, properties.All, error) {
	result := newResult()

	auth, err := i.mgr.AuthContext(ctx)
	if err != nil {
		return nil, properties.All{}, err
	}

	props := i.newProp(auth)
	for _, o := range options {
		if err := o.Run(&props, QueuedClient, scope); err != nil {
			return nil, properties.All{}, err
		}
	}

	if props.Ingestion.ReportLevel != properties.None {
		if props.Source.ID == uuid.Nil {
			props.Source.ID = uuid.New()
		}

		switch props.Ingestion.ReportMethod {
		case properties.ReportStatusToTable, properties.ReportStatusToQueueAndTable:
			managerResources, err := i.mgr.Resources()
			if err != nil {
				return nil, properties.All{}, err
			}

			if len(managerResources.Tables) == 0 {
				return nil, properties.All{}, fmt.Errorf("User requested reporting status to table, yet status table resource URI is not found")
			}

			props.Ingestion.TableEntryRef.TableConnectionString = managerResources.Tables[0].URL().String()
			props.Ingestion.TableEntryRef.PartitionKey = props.Source.ID.String()
			props.Ingestion.TableEntryRef.RowKey = uuid.Nil.String()
			break
		}
	}

	result.putProps(props)
	return result, props, nil
}

// FromFile allows uploading a data file for Kusto from either a local path or a blobstore URI path.
// This method is thread-safe.
func (i *Ingestion) FromFile(ctx context.Context, fPath string, options ...FileOption) (*Result, error) {
	local, err := filesystem.IsLocalPath(fPath)
	if err != nil {
		return nil, err
	}

	var scope SourceScope
	if local {
		scope = FromFile
	} else {
		scope = FromBlob
	}

	result, props, err := i.prepForIngestion(ctx, options, scope)
	if err != nil {
		return nil, err
	}

	result.record.IngestionSourcePath = fPath

	if local {
		err = i.fs.Local(ctx, fPath, props)
	} else {

		err = i.fs.Blob(ctx, fPath, 0, props)
	}

	if err != nil {
		return nil, err
	}

	result.putQueued(i.mgr)
	return result, nil
}

// FromReader allows uploading a data file for Kusto from an io.Reader. The content is uploaded to Blobstore and
// ingested after all data in the reader is processed. Content should not use compression as the content will be
// compressed with gzip. This method is thread-safe.
func (i *Ingestion) FromReader(ctx context.Context, reader io.Reader, options ...FileOption) (*Result, error) {
	result, props, err := i.prepForIngestion(ctx, options, FromReader)
	if err != nil {
		return nil, err
	}

	if props.Ingestion.Additional.Format == DFUnknown {
		return nil, fmt.Errorf("must provide option FileFormat() when using FromReader()")
	}

	path, err := i.fs.Reader(ctx, reader, props)
	if err != nil {
		return nil, err
	}

	result.record.IngestionSourcePath = path
	result.putQueued(i.mgr)
	return result, nil
}

// Deprecated: Stream usea streaming ingest client instead - `ingest.NewStreaming`.
// takes a payload that is encoded in format with a server stored mappingName, compresses it and uploads it to Kusto.
// More information can be found here:
// https://docs.microsoft.com/en-us/azure/kusto/management/create-ingestion-mapping-command
// The context object can be used with a timeout or cancel to limit the request time.
func (i *Ingestion) Stream(ctx context.Context, payload []byte, format DataFormat, mappingName string) error {
	c, err := i.getStreamConn()
	if err != nil {
		return err
	}

	props := properties.All{
		Ingestion: properties.Ingestion{
			DatabaseName: i.db,
			TableName:    i.table,
			Additional: properties.Additional{
				Format:              format,
				IngestionMappingRef: mappingName,
			},
		},
	}

	_, err = streamImpl(c, ctx, bytes.NewReader(payload), props)

	return err
}

func (i *Ingestion) getStreamConn() (*conn.Conn, error) {
	i.connMu.Lock()
	defer i.connMu.Unlock()

	if i.streamConn != nil {
		return i.streamConn, nil
	}

	sc, err := conn.New(i.client.Endpoint(), i.client.Auth())
	if err != nil {
		return nil, err
	}
	i.streamConn = sc
	return i.streamConn, nil
}

func (i *Ingestion) newProp(auth string) properties.All {
	return properties.All{
		Ingestion: properties.Ingestion{
			DatabaseName:        i.db,
			TableName:           i.table,
			RetainBlobOnSuccess: true,
			Additional: properties.Additional{
				AuthContext: auth,
			},
		},
	}
}
