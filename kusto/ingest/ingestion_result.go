package ingest

import (
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/resources"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/status"
)

// IngestionResult provides a way for users track the state of ingestion jobs
type IngestionResult struct {
	// StatusRecord holds the current ingestion status information
	StatusRecord StatusRecord

	statusTableClient *status.TableClient
}

// NewIngestionResult creates an initial ingestion status record
func NewIngestionResult(record StatusRecord, uri resources.URI) (*IngestionResult, error) {
	ret := &IngestionResult{}

	ret.StatusRecord = record
	if !record.Status.IsFinal() {
		client, err := status.NewTableClient(uri)
		if err != nil {
			return nil, err
		}

		ret.statusTableClient = client
	}

	return ret, nil
}
