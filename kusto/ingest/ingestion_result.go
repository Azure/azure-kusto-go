package ingest

import (
	"context"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/resources"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/status"
)

// IngestionResult provides a way for users track the state of ingestion jobs
type IngestionResult struct {
	record        StatusRecord
	uri           resources.URI
	reportToTable bool
	reportToQueue bool
}

// NewIngestionResult creates an initial ingestion status record
func NewIngestionResult(record StatusRecord, uri resources.URI) (*IngestionResult, error) {
	ret := &IngestionResult{}

	ret.record = record
	ret.uri = uri

	// TODO [Yochai] get this from options
	ret.reportToTable = true

	// report to queue not supported at the moment
	ret.reportToQueue = false

	return ret, nil
}

// WaitForIngestionComplete returns a channel that can be checked for ingestion results
// In order to check actual status please use the IngestionStatus option when ingesting data
func (r *IngestionResult) WaitForIngestionComplete(ctx context.Context) chan StatusRecord {
	ch := make(chan StatusRecord, 1)

	if r.record.Status.IsFinal() || !r.reportToTable {
		ch <- r.record
		close(ch)
	} else {
		go r.pollIngestionStatusTable(ctx, ch)
	}

	return ch
}

func (r *IngestionResult) pollIngestionStatusTable(ctx context.Context, ch chan StatusRecord) {
	// create a table client
	client, err := status.NewTableClient(r.uri)
	if err != nil {
		r.record.Status = StatusRetrievalFailed
		r.record.FailureStatus = Transient
		r.record.Details = "Failed Creating a Status Table client: " + err.Error()
		ch <- r.record
		close(ch)
	}

	// Create a ticker to poll the table in 10 second intervals
	ticker := time.NewTicker(10 * time.Second)
	run := true

	for run {
		select {
		// In case the user canceled the wait, return current known state
		case <-ctx.Done():
			// return a canceld state
			r.record.Status = StatusRetrievalCanceled
			r.record.FailureStatus = Transient
			ch <- r.record
			close(ch)
			run = false

		// Whenever the ticker fires
		case <-ticker.C:
			// read the current state
			smap, err := client.ReadIngestionStatus(r.record.IngestionSourceID)
			if err != nil {
				// Read failure
				r.record.Status = StatusRetrievalFailed
				r.record.FailureStatus = Transient
				r.record.Details = "Failed reading from Status Table: " + err.Error()
				ch <- r.record
				close(ch)
			} else {
				// convert the data into a record and send it if the state is final
				r.record.FromMap(smap)
				if r.record.Status.IsFinal() {
					ch <- r.record
					close(ch)
				}
			}
		}
	}
}
