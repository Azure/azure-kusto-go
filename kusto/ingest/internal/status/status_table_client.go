package status

import (
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/resources"
	"github.com/Azure/azure-sdk-for-go/storage"
)

const (
	defaultTimeout = 10000
	fullmetadata   = "application/json;odata=fullmetadata"
)

// TableClient allows reading and writing to azure tables.
type TableClient struct {
	tableURI resources.URI
	client   storage.Client
	service  storage.TableServiceClient
	table    *storage.Table
}

// NewTableClient Creates an azure table client.
func NewTableClient(uri resources.URI) (*TableClient, error) {
	c, err := storage.NewAccountSASClientFromEndpointToken(uri.URL().String(), uri.SAS().Encode())
	if err != nil {
		return nil, err
	}

	ts := c.GetTableService()
	tc := ts.GetTableReference(uri.ObjectName())

	atc := &TableClient{
		tableURI: uri,
		client:   c,
		service:  ts,
		table:    tc,
	}

	return atc, nil
}

// ReadIngestionStatus reads a table record cotaining ingestion status.
func (c *TableClient) ReadIngestionStatus(ingestionSourceID string) (map[string]interface{}, error) {
	entity := c.table.GetEntityReference(ingestionSourceID, "0")

	err := entity.Get(defaultTimeout, fullmetadata, nil)
	if err != nil {
		return nil, err
	}

	return entity.Properties, nil
}

// WriteIngestionStatus reads a table record cotaining ingestion status.
func (c *TableClient) WriteIngestionStatus(ingestionSourceID string, data map[string]interface{}) error {
	entity := c.table.GetEntityReference(ingestionSourceID, "0")
	entity.Properties = data

	options := &storage.EntityOptions{}
	options.Timeout = defaultTimeout

	err := entity.Insert(fullmetadata, options)
	if err != nil {
		return err
	}

	return nil
}
