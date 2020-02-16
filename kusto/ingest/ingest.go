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

type IngestClient struct {
	client          *kusto.Client
	resourceManager resourceManager
}

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
	fmt.Printf("Uploading the file with blob name: %s\n", fileName)
	_, err = azblob.UploadFileToBlockBlob(ctx, file, blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16})

	if err != nil {
		panic(err)
	}

	return blobURL
}


func (ic IngestClient) ingestFromLocalStorage(
	path string,
	props IngestionProperties,
	options map[string]string) (error) {
	storages, err := ic.resourceManager.getStorageAccounts(context.Background())

	if err != nil {
		return err
	}

	// Upload local file to temporary dm storage
	storage := storages[rand.Intn(len(storages))]
	creds, err := azblob.NewSharedKeyCredential("accountname", "accountkey")

	if err != nil {
		return err
	}

	pipeline := azblob.NewPipeline(creds, azblob.PipelineOptions{})
	storageUrl, _ := url.Parse(storage.String())
	containerUrl := azblob.NewContainerURL(*storageUrl, pipeline)
	ctx := context.Background()
	blobUrl := uploadFileToBlobStorage(ctx, path, containerUrl)

	// upload as if this is just a regular cloud storage
	return ic.ingestFromCloudStorage(fmt.Sprint(blobUrl), props, options)
}

func (ic IngestClient) ingestFromCloudStorage(path string, props IngestionProperties, options map[string]string) (error) {
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

func (ic IngestClient) IngestFromStorage(path string, props IngestionProperties, options map[string]string) error {
	if _, err := url.ParseRequestURI(path); err == nil {
		return ic.ingestFromCloudStorage(path, props, options)
	} else {
		return ic.ingestFromLocalStorage(path, props, options)
	}
}
