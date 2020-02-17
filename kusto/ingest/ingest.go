package ingest

import (
	"github.com/Azure/azure-kusto-go/kusto"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-queue-go/azqueue"
	"math/rand"
	"net/url"
	"os"
)

// Kusto ingest client provides methods to allow ingestion into kusto (ADX).
// To learn more about the different types of ingestions and when to use each, visit:
// https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
type IngestClient struct {
	client          *kusto.Client
	resourceManager resourceManager
}

// Create a new Kusto ingest client
// Expects a uri that point to the data management endpoint (`https://ingest-mycluster.kusto.windows.net`)
func New(dmEndpoint string, authorization kusto.Authorization) *IngestClient {
	dmClient, _ := kusto.New(dmEndpoint, authorization);
	return &IngestClient{
		client: dmClient,
		resourceManager: resourceManager{
			client:    dmClient,
			resources: nil,
		},
	}
}

type StorageIngestor interface {
	IngestFromStorage(path string, options StorageSourceOptions) (error)
}

type StreamIngestor interface {
	IngestFromStream(stream chan []byte, options StreamSourceOptions)
}

func (ic IngestClient) IngestFromStream(stream chan []byte, options StreamSourceOptions) {

}

func uploadFileToBlobStorage(ctx context.Context, fileName string, containerURL azblob.ContainerURL) azblob.BlockBlobURL {
	// Here's how to upload a blob.
	blobURL := containerURL.NewBlockBlobURL(fileName)
	file, err := os.Open(fileName)

	if err != nil {
		panic(err)
	}

	// The high-level API UploadFileToBlockBlob function uploads blocks in parallel for optimal performance, and can handle large files as well.
	// This function calls StageBlock/CommitBlockList for files larger 256 MBs, and calls Upload for any file smaller
	_, err = azblob.UploadFileToBlockBlob(ctx, file, blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16})

	if err != nil {
		panic(err)
	}

	return blobURL
}

// Issues a queued ingestion based on the given path (local FS) and properties.
// To learn more about ingestion properties, go to:
// https://docs.microsoft.com/en-us/azure/kusto/management/data-ingestion/#ingestion-properties
// To learn more about ingestion methods go to:
// https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
func (ic IngestClient) IngestFromLocalStorage(
	path string,
	props IngestionProperties,
	options map[string]string) (error) {
	storages, err := ic.resourceManager.getStorageAccounts(context.Background())

	if err != nil {
		return err
	}

	// Upload local file to temporary dm storage
	storage := storages[rand.Intn(len(storages))]
	creds := azblob.NewAnonymousCredential()

	if err != nil {
		return err
	}

	pipeline := azblob.NewPipeline(creds, azblob.PipelineOptions{})
	storageUrl, _ := url.Parse(storage.String())
	containerUrl := azblob.NewContainerURL(*storageUrl, pipeline)
	ctx := context.Background()
	blobUrl := uploadFileToBlobStorage(ctx, path, containerUrl)

	// upload as if this is just a regular cloud storage
	return ic.IngestFromCloudStorage(fmt.Sprint(blobUrl), props, options)
}

// Issues a queued ingestion based on the given path (URL) and properties.
// To learn more about ingestion properties, go to:
// https://docs.microsoft.com/en-us/azure/kusto/management/data-ingestion/#ingestion-properties
// To learn more about ingestion methods go to:
// https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
func (ic IngestClient) IngestFromCloudStorage(path string, props IngestionProperties, options map[string]string) (error) {
	queues, err := ic.resourceManager.getIngestionQueues(context.Background())

	if err != nil {
		return err
	}

	queue := queues[rand.Intn(len(queues))]

	creds := azqueue.NewAnonymousCredential()
	p := azqueue.NewPipeline(creds, azqueue.PipelineOptions{})

	u, _ := url.Parse(fmt.Sprintf("https://%s.queue.core.windows.net?%s", queue.storageAccountName, queue.sas))

	serviceUrl := azqueue.NewServiceURL(*u, p)
	queueUrl := serviceUrl.NewQueueURL(queue.objectName)
	messageUrl := queueUrl.NewMessagesURL()

	// TODO: better description of source
	source := map[string]string{"path": path}

	auth, err := ic.resourceManager.getAuthContext(context.Background())

	if err != nil {
		return err
	}

	ingestionBlobInfo := newIngestionBlobInfo(source, props, auth.AuthorizationContext.Value)
	ingestionBlobInfoAsJSON, err := json.Marshal(ingestionBlobInfo)

	if err != nil {
		return err;
	}

	_, e := messageUrl.Enqueue(context.Background(), base64.StdEncoding.EncodeToString(ingestionBlobInfoAsJSON), 0, 0)

	if e != nil {
		return e
	}

	return nil;
}

// Issues a queued ingestion based on the given path (URL / local FS) and properties.
// To learn more about ingestion properties, go to:
// https://docs.microsoft.com/en-us/azure/kusto/management/data-ingestion/#ingestion-properties
// To learn more about ingestion methods go to:
// https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
func (ic IngestClient) IngestFromStorage(path string, props IngestionProperties, options map[string]string) error {
	if _, err := url.ParseRequestURI(path); err == nil {
		return ic.IngestFromCloudStorage(path, props, options)
	} else {
		return ic.IngestFromLocalStorage(path, props, options)
	}
}
