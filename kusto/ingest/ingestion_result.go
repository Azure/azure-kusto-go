package ingest

import (
	"context"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"
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

// newIngestionResult creates an initial ingestion status record
func newIngestionResult() *IngestionResult {
	ret := &IngestionResult{}

	ret.record = newStatusRecord()
	ret.reportToTable = false
	ret.reportToQueue = false

	return ret
}

// updateFromError sets the record to a failure state and adds the error to the record details
func (r *IngestionResult) updateFromError(err error) *IngestionResult {
	return r.updateFromErrorString(err.Error())
}

// updateFromErrorString sets the record to a failure state and adds the error to the record details
func (r *IngestionResult) updateFromErrorString(err string) *IngestionResult {
	r.record.Status = ClientError
	r.record.FailureStatus = Permanent
	r.record.Details = err

	return r
}

// updateFromProps sets the record to a failure state and adds the error to the record details
func (r *IngestionResult) updateFromProps(props properties.All) *IngestionResult {
	r.reportToTable = props.Ingestion.ReportMethod == properties.ReportStatusToTable || props.Ingestion.ReportMethod == properties.ReportStatusToQueueAndTable
	r.record.FromProps(props)

	return r
}

// WaitForIngestionComplete returns a channel that can be checked for ingestion results
// In order to check actual status please use the IngestionStatus option when ingesting data
func (r *IngestionResult) WaitForIngestionComplete(ctx context.Context) chan StatusRecord {
	ch := make(chan StatusRecord, 1)

	if r.record.Status.IsFinal() || r.reportToTable == false {
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
	} else {
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
					run = false

				} else {
					// convert the data into a record and send it if the state is final
					r.record.FromMap(smap)
					if r.record.Status.IsFinal() {
						run = false
					}
				}
			}
		}
	}

	ch <- r.record
	close(ch)
}
