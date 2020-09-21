package ingest_test

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"

	"github.com/Azure/go-autorest/autorest/azure/auth"
)

func ExampleIngestion_FromFile() {
	var err error

	authConfig := auth.NewClientCredentialsConfig("clientID", "clientSecret", "tenantID")
	/*
		Alteratively, you could so something like:
		authorizer, err := auth.NewMSIConfig().Authorizer()
		or
		authorizer, err := auth.NewAuthorizerFromEnvironment()
		or
		auth.New...()

		then
		kusto.Authorization{Authorizer: authorizer}
	*/

	client, err := kusto.New("endpoint", kusto.Authorization{Config: authConfig})
	if err != nil {
		// Do something
	}

	ingestor, err := ingest.New(client, "database", "table")
	if err != nil {
		// Do something
	}

	// Setup a maximum time for completion to be 10 minutes.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Upload our file. When completed, delete the file on local storage we are uploading.
	res, err := ingestor.FromFile(ctx, "/path/to/file", ingest.DeleteSource(), ingest.ReportResultToTable())
	if err != nil {
		// The ingestion command failed to be sent, Do something
	}

	err = <-res.Wait(ctx)
	if err != nil {
		// the ingestion failed for some reason
		code, _ := ingest.GetIngestionStatus(err)
		failureStat, _ := ingest.GetIngestionFailureStatus(err)
		retry, _ := ingest.IsRetryable(err)

		fmt.Printf("Ingestion failed with Status %s; Failure is %s; details:\n%s\n", code, failureStat, err)
		if retry {
			// retry ingestion ?
		}
	}
}
