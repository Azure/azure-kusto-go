package ingest

import (
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/resources"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/status"
)

// IngestionStatus provides a way for users track the state of ingestion jobs
type IngestionStatus struct {
	// StatusRecord holds the current ingestion status information
	StatusRecord status.IngestionStatusRecord

	statusTableClient *status.AzureTableClient
}

// NewIngestionStatus creates an initial ingestion status record
func NewIngestionStatus(record status.IngestionStatusRecord, uri resources.URI) (*IngestionStatus, error) {
	ret := &IngestionStatus{}

	ret.StatusRecord = record
	if !record.Status.IsFinal() {
		client, err := status.NewAzureTableClient(uri)
		if err != nil {
			return nil, err
		}

		ret.statusTableClient = client
	}

	return ret, nil
}

// IsFinal returns true if the ingestion state is permenant or false if it is transiant
func (i *IngestionStatus) IsFinal() bool {
	return i.StatusRecord.Status.IsFinal()
}

// Refresh gets an updated ingestion status from the data manager
func (i *IngestionStatus) Refresh() error {
	if i.IsFinal() {
		// no reason to refresh the state
		return nil
	}

	return i.statusTableClient.ReadIngestionStatus(i.StatusRecord.IngestionSourceID, &i.StatusRecord)
}
