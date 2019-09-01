package azkustoingest

import (
	"azure-kusto-go/azure-kusto-data/azkustodata"
	"encoding/base64"
	"encoding/json"
)


type IngestClient struct {
	client          azkustodata.KustoClient
	resourceManager IngestionResourceProvider
}

func NewIngestClient(client azkustodata.KustoClient) (*IngestClient) {
	return &IngestClient{
		client: client,
	}
}

type StorageSourceOptions struct {
}

type StorageIngestor interface {
	IngestFromStorage(path string, options StorageSourceOptions) (error)
}

type IngestionBlobInfo struct {
	blob  string
	props map[string]string
	auth  string
}

func NewIngestionBlobInfo(blob string, props map[string]string, auth string) (*IngestionBlobInfo) {
	return &IngestionBlobInfo{
		blob:  blob,
		props: nil,
		auth:  "",
	}
}

func (ic *IngestClient) IngestFromStorage(path string, props map[string]string, options map[string]string) (error) {
	queues := ic.resourceManager.GetIngestionQueues()
	storage := ic.resourceManager.GetStorageAccount()
	queueSerivce := storage.GetQueueService()

	queue := random.choice(queues)

	ingestionBlobInfo := NewIngestionBlobInfo(path, props, "auth")
	ingestion_blob_info_json, err := json.Marshal(ingestionBlobInfo)

	if err != nil {
		return err;
	}
	var encoded []byte;
	base64.StdEncoding.Encode(encoded, []byte(ingestion_blob_info_json))
	queue_service.putMessage(queue_name = queue_details.object_name, content = encoded)

	return nil;
}
