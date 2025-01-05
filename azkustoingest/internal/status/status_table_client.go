package status

import (
	"context"
	"encoding/json"
	"github.com/Azure/azure-kusto-go/azkustoingest/internal/resources"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/google/uuid"
)

// TODO: figure out how to insert these to the new
const (
	defaultTimeoutMsec = 10000
	fullMetadata       = "application/json;odata=fullmetadata"
)

// TableClient allows reading and writing to azure tables.
type TableClient struct {
	tableURI resources.URI
	client   *aztables.Client

	service storage.TableServiceClient
	table   *storage.Table
}

// NewTableClient Creates an azure table client.
func NewTableClient(uri resources.URI) (*TableClient, error) {

	cred, err := aztables.NewClientWithNoCredential(uri.String(), &aztables.ClientOptions{
		ClientOptions: azcore.ClientOptions{},
	}) // TODO: pass http client
	if err != nil {
		return nil, err
	}

	return &TableClient{
		tableURI: uri,
		client:   cred,
	}, nil
}

// Read reads a table record cotaining ingestion status.
func (c *TableClient) Read(ctx context.Context, ingestionSourceID string) (map[string]interface{}, error) {
	var emptyID = uuid.Nil.String()
	entity, err := c.client.GetEntity(ctx, ingestionSourceID, emptyID, nil)
	if err != nil {
		return nil, err
	}

	bytes := entity.Value
	m := make(map[string]interface{})
	err = json.Unmarshal(bytes, &m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// Write reads a table record cotaining ingestion status.
func (c *TableClient) Write(ctx context.Context, ingestionSourceID string, data map[string]interface{}) error {
	dataCopy := make(map[string]interface{})
	for k, v := range data {
		dataCopy[k] = v
	}
	dataCopy["PartitionKey"] = ingestionSourceID
	dataCopy["RowKey"] = uuid.Nil.String()

	bytes, err := json.Marshal(dataCopy)
	if err != nil {
		return err
	}

	_, err = c.client.UpsertEntity(ctx, bytes, nil)

	return err
}
