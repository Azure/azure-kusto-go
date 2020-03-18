// Package filesystem provides a client with the ability to import data into Kusto via a variety of fileystems
// such as local storage or blobstore.
package filesystem

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/gzip"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/resources"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-queue-go/azqueue"
)

// uploadBlobStream provides a type that mimics azblob.UploadStreamToBlockBlob to allow fakes for testing.
type uploadBlobStream func(context.Context, io.Reader, azblob.BlockBlobURL, azblob.UploadStreamToBlockBlobOptions) (azblob.CommonResponse, error)

// uploadBlobFile provides a type that mimics azblob.UploadFileToBlockBlob to allow fakes for test
type uploadBlobFile func(context.Context, *os.File, azblob.BlockBlobURL, azblob.UploadToBlockBlobOptions) (azblob.CommonResponse, error)

// Ingestion provides methods for taking data from a filesystem of some type and ingesting it into Kusto.
// This object is scoped for a single database and table.
type Ingestion struct {
	db    string
	table string
	mgr   *resources.Manager

	uploadBlobStream uploadBlobStream
	uploadBlobFile   uploadBlobFile
}

// New is the constructor for Ingestion.
func New(db, table string, mgr *resources.Manager) (*Ingestion, error) {
	i := &Ingestion{
		db:               db,
		table:            table,
		mgr:              mgr,
		uploadBlobStream: azblob.UploadStreamToBlockBlob,
		uploadBlobFile:   azblob.UploadFileToBlockBlob,
	}
	return i, nil
}

// Local ingests a local file into Kusto.
func (i *Ingestion) Local(ctx context.Context, from string, props properties.All) error {
	to, err := i.upstreamContainer()
	if err != nil {
		return err
	}

	resources, err := i.mgr.Resources()
	if err != nil {
		return err
	}

	// We want to check the queue size here so so we don't upload a file and then find we don't have a Kusto queue to stick
	// it in. If we don't have a container, that is handled by containerQueue().
	if len(resources.Queues) == 0 {
		return errors.ES(errors.OpFileIngest, errors.KBlobstore, "no Kusto queue resources are defined, there is no queue to upload to").SetNoRetry()
	}

	blobURL, size, err := i.localToBlob(ctx, from, to, &props)
	if err != nil {
		return err
	}

	// We always want to delete the blob we create when we ingest from a local file.
	props.Ingestion.RetainBlobOnSuccess = false

	if err := i.Blob(ctx, blobURL.String(), size, props); err != nil {
		return err
	}

	if props.Source.DeleteLocalSource {
		if err := os.Remove(from); err != nil {
			return errors.ES(errors.OpFileIngest, errors.KLocalFileSystem, "file was uploaded successfully, but we could not delete the local file: %s", err)
		}
	}

	return nil
}

// Blob ingests a file from Azure Blob Storage into Kusto.
func (i *Ingestion) Blob(ctx context.Context, from string, fileSize int64, props properties.All) error {
	// To learn more about ingestion properties, go to:
	// https://docs.microsoft.com/en-us/azure/kusto/management/data-ingestion/#ingestion-properties
	// To learn more about ingestion methods go to:
	// https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods

	to, err := i.upstreamQueue()
	if err != nil {
		return err
	}

	props.Ingestion.BlobPath = from
	if fileSize != 0 {
		props.Ingestion.RawDataSize = fileSize
	}

	// If they did not tell us how the file was encoded, try to discover it from the file extension.
	if props.Ingestion.Additional.Format == properties.DFUnknown {
		et := FormatDiscovery(from)
		if et == properties.DFUnknown {
			return errors.ES(errors.OpFileIngest, errors.KClientArgs, "could not discover the file format from name of the file(%s)", from).SetNoRetry()
		}
		props.Ingestion.Additional.Format = et
	}

	j, err := props.Ingestion.MarshalJSONString()
	if err != nil {
		return errors.ES(errors.OpFileIngest, errors.KInternal, "could not marshal the ingestion blob info: %s", err).SetNoRetry()
	}

	if _, err := to.Enqueue(ctx, j, 0, 0); err != nil {
		return errors.E(errors.OpFileIngest, errors.KBlobstore, err)
	}

	return nil
}

// upstreamContainer randomly selects a container queue in which to upload our file to blobstore.
func (i *Ingestion) upstreamContainer() (azblob.ContainerURL, error) {
	resources, err := i.mgr.Resources()
	if err != nil {
		return azblob.ContainerURL{}, errors.E(errors.OpFileIngest, errors.KBlobstore, err)
	}

	if len(resources.Containers) == 0 {
		return azblob.ContainerURL{}, errors.ES(
			errors.OpFileIngest,
			errors.KBlobstore,
			"no Blob Storage container resources are defined, there is no where to upload to",
		).SetNoRetry()
	}

	storageURI := resources.Containers[rand.Intn(len(resources.Containers))]
	service, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net?%s", storageURI.Account(), storageURI.SAS().Encode()))

	creds := azblob.NewAnonymousCredential()
	pipeline := azblob.NewPipeline(creds, azblob.PipelineOptions{})

	return azblob.NewServiceURL(*service, pipeline).NewContainerURL(storageURI.ObjectName()), nil
}

func (i *Ingestion) upstreamQueue() (azqueue.MessagesURL, error) {
	resources, err := i.mgr.Resources()
	if err != nil {
		return azqueue.MessagesURL{}, err
	}

	if len(resources.Queues) == 0 {
		return azqueue.MessagesURL{}, errors.ES(
			errors.OpFileIngest,
			errors.KBlobstore,
			"no Kusto queue resources are defined, there is no where to upload to",
		).SetNoRetry()
	}

	queue := resources.Queues[rand.Intn(len(resources.Queues))]
	service, _ := url.Parse(fmt.Sprintf("https://%s.queue.core.windows.net?%s", queue.Account(), queue.SAS().Encode()))

	creds := azqueue.NewAnonymousCredential()
	p := azqueue.NewPipeline(creds, azqueue.PipelineOptions{})

	return azqueue.NewServiceURL(*service, p).NewQueueURL(queue.ObjectName()).NewMessagesURL(), nil
}

var nower = time.Now

// localToBlob copies from a local to to an Azure Blobstore blob. It returns the URL of the Blob, the local file info and an
// error if there was one.
func (i *Ingestion) localToBlob(ctx context.Context, from string, to azblob.ContainerURL, props *properties.All) (azblob.BlockBlobURL, int64, error) {
	compression := CompressionDiscovery(from)
	blobName := fmt.Sprintf("%s_%s_%s_%s", filepath.Base(from), nower(), i.db, i.table)
	if compression == properties.CTNone {
		blobName = from + ".gz"
	}

	// Here's how to upload a blob.
	blobURL := to.NewBlockBlobURL(blobName)

	file, err := os.Open(from)
	if err != nil {
		return azblob.BlockBlobURL{}, 0, errors.ES(
			errors.OpFileIngest,
			errors.KLocalFileSystem,
			"problem retrieving source file %q: %s", from, err,
		).SetNoRetry()
	}

	stat, err := file.Stat()
	if err != nil {
		return azblob.BlockBlobURL{}, 0, errors.ES(
			errors.OpFileIngest,
			errors.KLocalFileSystem,
			"could not Stat the file(%s): %s", from, err,
		).SetNoRetry()
	}

	if compression == properties.CTNone {
		gstream := gzip.New()
		gstream.Reset(file)

		_, err = i.uploadBlobStream(
			ctx,
			gstream,
			blobURL,
			azblob.UploadStreamToBlockBlobOptions{BufferSize: 1, MaxBuffers: 1},
		)

		if err != nil {
			return azblob.BlockBlobURL{}, 0, errors.ES(errors.OpFileIngest, errors.KBlobstore, "problem uploading to Blob Storage: %s", err)
		}
		return blobURL, 10, nil
	}

	// The high-level API UploadFileToBlockBlob function uploads blocks in parallel for optimal performance, and can handle large files as well.
	// This function calls StageBlock/CommitBlockList for files larger 256 MBs, and calls Upload for any file smaller
	_, err = i.uploadBlobFile(
		ctx,
		file,
		blobURL,
		azblob.UploadToBlockBlobOptions{
			BlockSize:   4 * 1024 * 1024,
			Parallelism: 16,
		},
	)

	if err != nil {
		return azblob.BlockBlobURL{}, 0, errors.ES(errors.OpFileIngest, errors.KBlobstore, "problem uploading to Blob Storage: %s", err)
	}

	return blobURL, stat.Size(), nil
}

// FormatDiscovery looks at the file name and tries to discern what the file format is.
func FormatDiscovery(fName string) properties.DataFormat {
	name := fName

	u, err := url.Parse(fName)
	if err == nil && u.Scheme != "" {
		name = u.Path
	}

	ext := filepath.Ext(strings.TrimSuffix(strings.TrimSuffix(strings.ToLower(name), ".zip"), ".gz"))

	switch ext {
	case ".avro":
		return properties.AVRO
	case ".csv":
		return properties.CSV
	case ".json":
		return properties.JSON
	case ".orc":
		return properties.ORC
	case ".parquet":
		return properties.Parquet
	case ".psv":
		return properties.PSV
	case ".raw":
		return properties.Raw
	case ".scsv":
		return properties.SCSV
	case ".sohsv":
		return properties.SOHSV
	case ".tsv":
		return properties.TSV
	case ".txt":
		return properties.TXT
	}
	return properties.DFUnknown
}

// CompressionDiscovery looks at the file extension. If it is one we support, we return that
// CompressionType that represents that value. Otherwise we return CTNone to indicate that the
// file should not be compressed.
func CompressionDiscovery(fName string) properties.CompressionType {
	var ext string
	if strings.HasPrefix(strings.ToLower(fName), "http") {
		ext = strings.ToLower(filepath.Ext(path.Base(fName)))
	} else {
		ext = strings.ToLower(filepath.Ext(fName))
	}

	switch ext {
	case ".gz":
		return properties.GZIP
	case ".zip":
		return properties.ZIP
	}
	return properties.CTNone
}
