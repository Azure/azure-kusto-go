// Package ingestion - in charge of ingesting the given data - based on the configuration file.
package ingestion

import (
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustoingest"
	"github.com/Azure/azure-kusto-go/quickstart/utils"
	"time"
)

// WaitForIngestionToComplete Halts the program for WaitForIngestSeconds, allowing the queued ingestion process to complete
func WaitForIngestionToComplete(waitForIngestSeconds int) {
	fmt.Printf("Sleeping %d seconds for queued ingestion to complete. Note: This may take longer depending on"+
		" the file size and ingestion batching policy.\n", waitForIngestSeconds)

	for i := 0; i <= waitForIngestSeconds; i++ {
		fmt.Print("\r")

		fmt.Print(i)
		for j := 0; j <= i; j++ {
			fmt.Print(".")
		}
		time.Sleep(time.Second)
	}
}

// IngestSource Ingests both files and blob sources and handles error accordingly
func IngestSource(ingestClient *azkustoingest.Ingestion, DataSourceUri string, ctx context.Context, options []azkustoingest.FileOption, databaseName string, tableName string, source string) {
	_, err := ingestClient.FromFile(ctx, DataSourceUri, options...)

	if err != nil {
		err := errors.ES(errors.OpFileIngest, errors.KOther, fmt.Sprintf("%s ingestion error", source))
		utils.ErrorHandler(fmt.Sprintf("Ingestion exception while trying to ingest '%s' into '%s.%s'", DataSourceUri, databaseName, tableName), err)
	}
}
