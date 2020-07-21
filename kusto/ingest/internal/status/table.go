package status

import (
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/resources"
	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/google/uuid"
)

const (
	defaultTimeout = 1000
	metadataLevel  = "fullmetadata"
)

// AzureTableClient allows reading ana writing to azure tables
type AzureTableClient struct {
	tableURI resources.URI
	client   storage.Client
	service  storage.TableServiceClient
	table    *storage.Table
}

// New Creates an azure table client
func NewAzureTableClient(uri resources.URI) (*AzureTableClient, error) {
	// Create a table client
	c, err := storage.NewAccountSASClientFromEndpointToken(uri.URL().String(), uri.SAS().Get("SAS"))
	if err != nil {
		return nil, err
	}

	ts := c.GetTableService()
	tc := ts.GetTableReference(uri.ObjectName())

	atc := &AzureTableClient{
		tableURI: uri,
		client:   c,
		service:  ts,
		table:    tc,
	}

	return atc, nil
}

// ReadIngestionStatus reads a table record cotaining ingestion status
func (c *AzureTableClient) ReadIngestionStatus(ingestionSourceID uuid.UUID, data *IngestionStatusRecord) error {
	entity := storage.Entity{
		PartitionKey: ingestionSourceID.String(),
		RowKey:       "0",
		Table:        c.table,
	}

	options := &storage.GetEntityOptions{}

	err := entity.Get(defaultTimeout, metadataLevel, options)
	if err != nil {
		return err
	}

	data.FromMap(entity.Properties)
	return nil
}

// WriteIngestionStatus reads a table record cotaining ingestion status
func (c *AzureTableClient) WriteIngestionStatus(ingestionSourceID uuid.UUID, data *IngestionStatusRecord) error {
	entity := storage.Entity{
		PartitionKey: ingestionSourceID.String(),
		RowKey:       "0",
		Table:        c.table,
		Properties:   data.ToMap(),
	}

	options := &storage.EntityOptions{}
	options.Timeout = defaultTimeout

	err := entity.InsertOrReplace(options)
	if err != nil {
		return err
	}

	return nil
}
