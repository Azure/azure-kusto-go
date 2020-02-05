package ingest

import (
	"github.com/Azure/azure-kusto-go/data"
)

type streamPacket struct {
	data chan []byte
	props IngestionProperties
}

type StreamingIngestClient struct {
	client          *data.Client
	resourceManager resourceManager
	out chan[1] streamPacket
}


func (ic StreamingIngestClient) streamToKusto() {
	for sp := range ic.out{
		ic.client.Query()
	}
}

func NewStreaming(dmEndpoint string, authorization data.Authorization) *StreamingIngestClient {
	dmClient, _ := data.New(dmEndpoint, authorization);
	sic := &StreamingIngestClient{
		client: dmClient,
		resourceManager: resourceManager{
			client:    dmClient,
			resources: nil,
		},
		out: make(chan [1] streamPacket),
	}

	go sic.streamToKusto()

	return sic
}


func (ic StreamingIngestClient) IngestFromStream(in chan []byte, properties IngestionProperties, options map[string]string) {
	for b := range in{

	}
}
