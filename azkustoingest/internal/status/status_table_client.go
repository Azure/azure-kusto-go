package status

import (
	"context"
	"encoding/json"
	"github.com/Azure/azure-kusto-go/azkustoingest/internal/resources"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/google/uuid"
)

const (
	// todo: should we set this timeout?
	defaultTimeoutMsec = 10000
	fullMetadata       = aztables.MetadataFormatFull
)

// TableClient allows reading and writing to azure tables.
type TableClient struct {
	tableURI resources.URI
	client   *aztables.Client

	service storage.TableServiceClient
	table   *storage.Table
}

// NewTableClient Creates an azure table client.
func NewTableClient(client policy.Transporter, uri resources.URI) (*TableClient, error) {
	cred, err := aztables.NewClientWithNoCredential(uri.String(), &aztables.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: client,
		},
	})
	if err != nil {
		return nil, err
	}

	return &TableClient{
		tableURI: uri,
		client:   cred,
	}, nil
}

// Read reads a table record containing ingestion status.
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

// Write reads a table record containing ingestion status.
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

	format := fullMetadata

	_, err = c.client.AddEntity(ctx, bytes, &aztables.AddEntityOptions{
		Format: &format,
	})

	// TODO - what should we do in case it already exists?
	return err
}
