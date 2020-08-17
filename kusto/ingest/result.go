package ingest

import (
	"context"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/resources"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/status"
)

// Result provides a way for users track the state of ingestion jobs.
type Result struct {
	record        StatusRecord
	uri           resources.URI
	reportToTable bool
	reportToQueue bool
}

// newResult creates an initial ingestion status record.
func newResult() *Result {
	ret := &Result{}

	ret.record = newStatusRecord()
	return ret
}

// putErr sets the record to a failure state and adds the error to the record details.
func (r *Result) putErr(err error) *Result {
	return r.putErrStr(err.Error())
}

// putErrStr sets the record to a failure state and adds the error to the record details.
func (r *Result) putErrStr(err string) *Result {
	r.record.Status = ClientError
	r.record.FailureStatus = Permanent
	r.record.Details = err

	return r
}

// putProps sets the record to a failure state and adds the error to the record details.
func (r *Result) putProps(props properties.All) *Result {
	r.reportToTable = props.Ingestion.ReportMethod == properties.ReportStatusToTable || props.Ingestion.ReportMethod == properties.ReportStatusToQueueAndTable
	r.record.FromProps(props)

	return r
}

// Wait returns a channel that can be checked for ingestion results.
// In order to check actual status please use the IngestionStatus option when ingesting data.
func (r *Result) Wait(ctx context.Context) chan StatusRecord {
	ch := make(chan StatusRecord, 1)

	if r.record.Status.IsFinal() || r.reportToTable == false {
		ch <- r.record
		close(ch)
	} else {
		go r.poll(ctx, ch)
	}

	return ch
}

func (r *Result) poll(ctx context.Context, ch chan StatusRecord) {
	// create a table client
	client, err := status.NewTableClient(r.uri)
	if err != nil {
		r.record.Status = StatusRetrievalFailed
		r.record.FailureStatus = Transient
		r.record.Details = "Failed Creating a Status Table client: " + err.Error()
	} else {
		// Create a ticker to poll the table in 10 second intervals.
		ticker := time.NewTicker(10 * time.Second)
		run := true

		for run {
			select {
			// In case the user canceled the wait, return current known state.
			case <-ctx.Done():
				// return a canceld state.
				r.record.Status = StatusRetrievalCanceled
				r.record.FailureStatus = Transient
				run = false

			// Whenever the ticker fires.
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
					// convert the data into a record and send it if the state is final.
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
