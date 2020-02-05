package ingest

import (
	"azure-kusto-go/data"
)

type StreamingIngestClient struct {
	client          *data.Client
	resourceManager resourceManager
}

func NewStreamingIngestClient(dmEndpoint string, authorization data.Authorization) *IngestClient {
	dmClient, _ := data.New(dmEndpoint, authorization);
	return &IngestClient{
		client: dmClient,
		resourceManager: resourceManager{
			client:    dmClient,
			resources: nil,
		},
	}
}


func (ic StreamingIngestClient) IngestFromStream(stream chan []byte, options StreamSourceOptions) {

}
