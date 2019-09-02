package azkustoingest

import (
	"azure-kusto-go/azure-kusto-data/azkustodata"
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-queue-go/azqueue"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
)

type IngestClient struct {
	client          azkustodata.KustoClient
	resourceManager ResourceManager
}

func NewIngestClient(client azkustodata.KustoClient) (*IngestClient) {
	return &IngestClient{
		client: client,
	}
}

type StorageIngestor interface {
	IngestFromStorage(path string, options StorageSourceOptions) (error)
}

func (ic IngestClient) IngestFromLocalStorage(path string, props map[string]string, options map[string]string) (error) {
	storages, err := ic.resourceManager.GetStorageAccounts()

	if err != nil {
		return err
	}

	storage := storages[rand.Intn(len(storages))]

	// Create a BlockBlobURL object to a blob in the container (we assume the container already exists).
	// blobName = fmt.Sprint("%s__%s__%s__%s", props["database"], props[]).format(
	//	db=ingestion_properties.database,
	//	table=ingestion_properties.table,
	//	guid=descriptor.source_id or uuid.uuid4(),
	//	file=descriptor.stream_name,
	//)

	u, _ := url.Parse(storage.String())

	//blockBlobURL := azblob.NewBlockBlobURL(*u, azblob.NewPipeline(credential, azblob.PipelineOptions{}))

	ctx := context.Background() // This example uses a never-expiring context

	// Create some data to test the upload stream
	blobSize := 8 * 1024 * 1024
	data := make([]byte, blobSize)
	rand.Read(data)

	reader, err := ioutil.ReadFile(path)

	if err != nil {
		return err
	}

	// Perform UploadStreamToBlockBlob
	bufferSize := 2 * 1024 * 1024 // Configure the size of the rotating buffers that are used when uploading
	maxBuffers := 3               // Configure the number of rotating buffers that are used when uploading

	// TODO: this is a placeholder for compilation
	print(blobSize, bufferSize, maxBuffers, reader, ctx, u, azblob.Version())
	//_, err = azblob.UploadStreamToBlockBlob(
	//	ctx,
	//	reader,
	//	//bytes.NewReader(data),
	//	blockBlobURL,
	//	azblob.UploadStreamToBlockBlobOptions{
	//		BufferSize: bufferSize,
	//		MaxBuffers: maxBuffers,
	//	}
	//)

	// Verify that upload was successful
	return nil
}

func (ic IngestClient) ingestFromCloudStorage(path string, props map[string]string, options map[string]string) (error) {
	queues, err := ic.resourceManager.GetIngestionQueues()

	if err != nil {
		return err
	}

	queue := queues[rand.Intn(len(queues))]

	//queueService := storage.GetQueueService(queue)
	u, _ := url.Parse(queue.String())

	// TODO: this should get proper creds using sas signature
	creds := azqueue.NewAnonymousCredential()
	pipeline := azqueue.NewPipeline(creds, azqueue.PipelineOptions{})
	queueService := azqueue.NewMessagesURL(*u, pipeline)

	source := make(map[string]string)
	source["path"] = path
	auth, err := ic.resourceManager.GetAuthContext()

	if err != nil {
		return err
	}

	ingestionBlobInfo := NewIngestionBlobInfo(source, props, auth)
	ingestionBlobInfoAsJSON, err := json.Marshal(ingestionBlobInfo)

	if err != nil {
		return err;
	}

	var message []byte;
	base64.StdEncoding.Encode(message, ingestionBlobInfoAsJSON)
	queueService.Enqueue(context.Background(), string(message), 0, 0)

	return nil;
}

func (ic IngestClient) IngestFromStorage(path string, props map[string]string, options map[string]string) (error) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return ic.ingestFromCloudStorage(path, props, options)
	} else {
		return ic.IngestFromLocalStorage(path, props, options)
	}

}
