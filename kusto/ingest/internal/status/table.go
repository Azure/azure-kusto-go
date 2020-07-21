package status

import (
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/resources"
	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/google/uuid"
)

// AzureTableClient allows reading ana writing to azure tables
type AzureTableClient struct {
	tableURI      resources.URI
	storageClient storage.Client
	tableService  storage.TableServiceClient
	tableClient   *storage.Table
}

// New Creates an azure table client
func New(uri resources.URI) (*AzureTableClient, error) {
	// Create a table client
	c, err := storage.NewAccountSASClientFromEndpointToken(uri.URL().String(), uri.SAS().Get("SAS"))
	if err != nil {
		return nil, err
	}

	ts := c.GetTableService()
	tc := ts.GetTableReference(uri.ObjectName())

	atc := &AzureTableClient{
		tableURI:      uri,
		storageClient: c,
		tableService:  ts,
		tableClient:   tc,
	}

	return atc, nil
}

// ReadIngestionStatus reads a table record cotaining ingestion status
func (c *AzureTableClient) ReadIngestionStatus(ingestionSourceID uuid.UUID) (*IngestionStatusRecord, *error) {
	return nil, nil
}

// WriteIngestionStatus reads a table record cotaining ingestion status
func (c *AzureTableClient) WriteIngestionStatus(ingestionSourceID uuid.UUID, data IngestionStatusRecord) *error {
	return nil
}
