package ingest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"github.com/Azure/azure-kusto-go/data"
)

type streamPacket struct {
	data    []byte
	props   IngestionProperties
	options *StreamSourceOptions
}

type StreamingIngestClient struct {
	client          *data.Client
	resourceManager resourceManager
	out             chan streamPacket
}

func (ic StreamingIngestClient) streamToKusto(context context.Context,done chan bool) {
	for sp := range ic.out {
		format := "csv"
		if sp.options != nil {
			format = sp.options.Format
		}

		err := ic.client.Stream(
			context,
			sp.props.DatabaseName,
			sp.props.TableName,
			sp.data,
			format,
			&sp.props.IngestionMappingRef,
		)

		if err != nil {
			panic(err)
		}
	}

	done <- true
}

func NewStreaming(dmEndpoint string, authorization data.Authorization) *StreamingIngestClient {
	dmClient, _ := data.New(dmEndpoint, authorization)
	sic := &StreamingIngestClient{
		client: dmClient,
		resourceManager: resourceManager{
			client:    dmClient,
			resources: nil,
		},
		out: nil,
	}

	return sic
}

const B = 1
const KB = 1024 * B
const MB = 1024 * KB
const MAX_STREAMING_PACKET_SIZE = 4 * MB

// The reason this method is on the ingest client vs the query client is that there are some preliminary steps that are needed.
// If one is to ingest directly via the client.Stream option, one is to handle the proper serialization by himself.
func (sic *StreamingIngestClient) IngestFromStream(in chan interface{}, properties IngestionProperties, options *StreamSourceOptions, done chan bool) {
	sic.out = make(chan streamPacket)

	// spin up consumer
	// TODO (daniel): should I pass external context here?
	go sic.streamToKusto(context.Background() ,done)

	// TODO: use buff pool from John's code
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	encoder := json.NewEncoder(gz)

	defer close(done)

	for obj := range in {
		if obj != nil {
			e := encoder.Encode(obj)
			// TODO (daniel): I probably shouldn't flush here
			gz.Flush()
			if e != nil {
				panic(e)
			}

			if buf.Len() > MAX_STREAMING_PACKET_SIZE {
				sic.out <- streamPacket{
					buf.Bytes(),
					properties,
					options,
				}

				// TODO (daniel): I'm sure there is a better way to do this
				gz.Reset(&buf)
			}
		}
	}

	gz.Flush()
	gz.Close()
	// One last flush
	sic.out <- streamPacket{
		buf.Bytes(),
		properties,
		options,
	}

	close(sic.out)

	<- done
}
