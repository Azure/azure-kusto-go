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
	tableClient   *status.TableClient
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

// putQueued sets the initial success status depending on status reporting state
func (r *Result) putQueued(mgr *resources.Manager) *Result {
	// If not checking status, just return queued
	if !r.reportToTable {
		r.record.Status = Queued
		return r
	}

	// Get table URI
	resources, err := mgr.Resources()
	if err != nil {
		r.record.Status = StatusRetrievalFailed
		r.record.FailureStatus = Transient
		r.record.Details = "Failed getting status table URI: " + err.Error()
		return r
	}

	if len(resources.Tables) == 0 {
		r.record.Status = StatusRetrievalFailed
		r.record.FailureStatus = Transient
		r.record.Details = "Ingestion resources do not include a status table URI: " + err.Error()
		return r
	}

	// create a table client
	client, err := status.NewTableClient(*resources.Tables[0])
	if err != nil {
		r.record.Status = StatusRetrievalFailed
		r.record.FailureStatus = Transient
		r.record.Details = "Failed Creating a Status Table client: " + err.Error()
		return r
	}

	// Write initial record
	r.record.Status = Pending
	recordMap := r.record.ToMap()
	err = client.WriteIngestionStatus(r.record.IngestionSourceID, recordMap)
	if err != nil {
		r.putErr(err)
	} else {
		r.tableClient = client
	}

	return r
}

// Wait returns a channel that can be checked for ingestion results.
// In order to check actual status please use the IngestionStatus option when ingesting data.
func (r *Result) Wait(ctx context.Context) chan StatusRecord {
	ch := make(chan StatusRecord, 1)

	go func() {
		defer close(ch)

		if !r.record.Status.IsFinal() && r.reportToTable == true {
			r.poll(ctx)
		}

		ch <- r.record
	}()

	return ch
}

func (r *Result) poll(ctx context.Context) {
	// create a table client
	if r.tableClient != nil {
		// Create a ticker to poll the table in 10 second intervals.
		ticker := time.NewTicker(10 * time.Second)

		for {
			select {
			// In case the user canceled the wait, return current known state.
			case <-ctx.Done():
				// return a canceld state.
				r.record.Status = StatusRetrievalCanceled
				r.record.FailureStatus = Transient
				return

			// Whenever the ticker fires.
			case <-ticker.C:
				// read the current state
				smap, err := r.tableClient.ReadIngestionStatus(r.record.IngestionSourceID)
				if err != nil {
					// Read failure
					r.record.Status = StatusRetrievalFailed
					r.record.FailureStatus = Transient
					r.record.Details = "Failed reading from Status Table: " + err.Error()
					return
				}

				// convert the data into a record and send it if the state is final.
				r.record.FromMap(smap)
				if r.record.Status.IsFinal() {
					return
				}
			}
		}
	}
}
