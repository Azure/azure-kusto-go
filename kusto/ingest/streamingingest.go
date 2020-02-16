package ingest

import "github.com/Azure/azure-kusto-go/kusto"

type StreamingIngestClient struct {
	client          *kusto.Client
	resourceManager resourceManager
}

func NewStreamingIngestClient(dmEndpoint string, authorization kusto.Authorization) *IngestClient {
	dmClient, _ := kusto.New(dmEndpoint, authorization);
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
