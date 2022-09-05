package ingest_test

import (
	"context"
	"time"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
)

func ExampleIngestion_FromFile() {
	var err error

	kcsb := kusto.GetConnectionStringBuilder(`endpoint;tenantid=tenantID;appclientid=clientID;appkey=clientSecret`)

	client, err := kusto.New(kcsb)
	if err != nil {
		// Do something
	}
	// Be sure to close the client when you're done. (Error handling omitted for brevity.)
	defer client.Close()

	ingestor, err := ingest.New(client, "database", "table")
	if err != nil {
		// Do something
	}
	// Closing the ingestor will not close the client (since the client may be used separately),
	//but it is still important to close the ingestor when you're done.
	defer ingestor.Close()

	// Setup a maximum time for completion to be 10 minutes.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Upload our file WITHOUT status reporting.
	// When completed, delete the file on local storage we are uploading.
	_, err = ingestor.FromFile(ctx, "/path/to/file", ingest.DeleteSource())
	if err != nil {
		// The ingestion command failed to be sent, Do something
	}

	// Upload our file WITH status reporting.
	// When completed, delete the file on local storage we are uploading.
	status, err := ingestor.FromFile(ctx, "/path/to/file", ingest.DeleteSource(), ingest.ReportResultToTable())
	if err != nil {
		// The ingestion command failed to be sent, Do something
	}

	err = <-status.Wait(ctx)
	if err != nil {
		// the operation complete with an error
		if ingest.IsRetryable(err) {
			// Handle reties
		} else {
			// inspect the failure
			// statusCode, _ := ingest.GetIngestionStatus(err)
			// failureStatus, _ := ingest.GetIngestionFailureStatus(err)
		}
	}
}
