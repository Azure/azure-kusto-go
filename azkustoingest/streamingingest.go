package azkustoingest

import (
	"github.com/Azure/azure-kusto-go/azkustodata"
)

type StreamingIngestClient struct {
	client          *azkustodata.Client
	resourceManager resourceManager
}

func NewStreamingIngestClient(dmEndpoint string, authorization azkustodata.Authorization) *IngestClient {
	dmClient, _ := azkustodata.New(dmEndpoint, authorization);
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
