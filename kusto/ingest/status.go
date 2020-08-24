package ingest

import (
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"
	"github.com/google/uuid"
)

// StatusCode is the ingestion status
type StatusCode string

const (
	// Pending status represents a temporary status.
	// Might change during the course of ingestion based on the
	// outcome of the data ingestion operation into Kusto.
	Pending StatusCode = "Pending"
	// Succeeded status represents a permanent status.
	// The data has been successfully ingested to Kusto.
	Succeeded StatusCode = "Succeeded"
	// Failed Status represents a permanent status.
	// The data has not been ingested to Kusto.
	Failed StatusCode = "Failed"
	// Queued status represents a permanent status.
	// The data has been queued for ingestion &  status tracking was not requested.
	// (This does not indicate that the ingestion was successful.)
	Queued StatusCode = "Queued"
	// Skipped status represents a permanent status.
	// No data was supplied for ingestion. The ingest operation was skipped.
	Skipped StatusCode = "Skipped"
	// PartiallySucceeded status represents a permanent status.
	// Part of the data was successfully ingested to Kusto, while other parts failed.
	PartiallySucceeded StatusCode = "PartiallySucceeded"

	// StatusRetrievalFailed means the client ran into truble reading the status from the service
	StatusRetrievalFailed StatusCode = "StatusRetrievalFailed"
	// StatusRetrievalCanceled means the user canceld the status check
	StatusRetrievalCanceled StatusCode = "StatusRetrievalCanceled"
	// ClientError an error was detected on the client side
	ClientError StatusCode = "ClientError"
)

// IsFinal returns true if the ingestion status is a final status, or false if the status is temporary
func (i StatusCode) IsFinal() bool {
	return i != Pending
}

// FailureStatusCode indicates the status of failuted ingestion attempts
type FailureStatusCode string

const (
	// Unknown represents an undefined or unset failure state
	Unknown FailureStatusCode = "Unknown"
	// Permanent represnets failure state that will benefit from a retry attempt
	Permanent FailureStatusCode = "Permanent"
	// Transient represnet a retryable failure state
	Transient FailureStatusCode = "Transient"
	// Exhausted represents a retryable failure that has exhusted all retry attempts
	Exhausted FailureStatusCode = "Exhausted"
)

// StatusRecord is a record containing information regarding the status of an ingation command
type StatusRecord struct {
	// Status - The ingestion status returned from the service. Status remains 'Pending' during the ingestion process and
	// is updated by the service once the ingestion completes. When <see cref="IngestionReportMethod"/> is set to 'Queue', the ingestion status
	// will always be 'Queued' and the caller needs to query the reports queues for ingestion status, as configured. To query statuses that were
	// reported to queue, see: <see href="https://docs.microsoft.com/en-us/azure/kusto/api/netfx/kusto-ingest-client-status#ingestion-status-in-azure-queue"/>.
	// When <see cref="IngestionReportMethod"/> is set to 'Table', call <see cref="IKustoIngestionResult.GetIngestionStatusBySourceId"/> or
	// <see cref="IKustoIngestionResult.GetIngestionStatusCollection"/> to retrieve the most recent ingestion status.
	Status StatusCode

	// IngestionSourceID - A unique identifier representing the ingested source. It can be supplied during the ingestion execution.
	IngestionSourceID uuid.UUID

	// The URI of the blob, potentially including the secret needed to access
	// the blob. This can be a filesystem URI (on-premises deployments only),
	// or an Azure Blob Storage URI (including a SAS key or a semicolon followed
	// by the account key).
	IngestionSourcePath string

	// Database - The name of the database holding the target table.
	Database string

	// Table - The name of the target table into which the data will be ingested.
	Table string

	// UpdatedOn - The last updated time of the ingestion status.
	UpdatedOn time.Time

	// OperationID - The ingestion's operation ID.
	OperationID uuid.UUID

	// ActivityID - The ingestion's activity ID.
	ActivityID uuid.UUID

	// ErrorCode In case of a failure, indicates the failure's error code.
	// TODO [Yochai, July 2020] make this into a const list
	ErrorCode string

	// FailureStatus - In case of a failure, indicates the failure's status.
	FailureStatus FailureStatusCode

	// Details - In case of a failure, indicates the failure's details.
	Details string

	// OriginatesFromUpdatePolicy - In case of a failure, indicates whether or not the failure originated from an Update Policy.
	OriginatesFromUpdatePolicy bool
}

// newStatusRecord creates a new record initialized with defaults and user provided data.
func newStatusRecord() StatusRecord {
	rec := StatusRecord{
		Status:                     Failed,
		IngestionSourceID:          uuid.Nil,
		IngestionSourcePath:        "Undefined",
		Database:                   "Undefined",
		Table:                      "Undefined",
		UpdatedOn:                  time.Now(),
		OperationID:                uuid.Nil,
		ActivityID:                 uuid.Nil,
		ErrorCode:                  "Unknown",
		FailureStatus:              Unknown,
		Details:                    "",
		OriginatesFromUpdatePolicy: false,
	}

	return rec
}

// FromProps takes in data from ingestion options.
func (r *StatusRecord) FromProps(props properties.All) {
	r.IngestionSourceID = props.Source.ID
	r.Database = props.Ingestion.DatabaseName
	r.Table = props.Ingestion.TableName
	r.UpdatedOn = time.Now()

	if props.Ingestion.BlobPath != "" && r.IngestionSourcePath == "Undefined" {
		r.IngestionSourcePath = props.Ingestion.BlobPath
	}
}

// FromMap converts an ingestion status record to a key value map.
func (r *StatusRecord) FromMap(data map[string]interface{}) {
	if data["Status"] != nil {
		r.Status = StatusCode(data["Status"].(string))
	}

	if data["IngestionSourceId"] != nil {
		r.IngestionSourceID = data["IngestionSourceId"].(uuid.UUID)
	}

	if data["IngestionSourcePath"] != nil {
		r.IngestionSourcePath = data["IngestionSourcePath"].(string)
	}

	if data["Database"] != nil {
		r.Database = data["Database"].(string)
	}

	if data["Table"] != nil {
		r.Table = data["Table"].(string)
	}

	t, err := time.Parse(time.RFC3339Nano, data["UpdatedOn"].(string))
	if err == nil {
		r.UpdatedOn = t
	}

	if data["OperationId"] != nil {
		r.OperationID = data["OperationId"].(uuid.UUID)
	}

	if data["ActivityId"] != nil {
		r.ActivityID = data["ActivityId"].(uuid.UUID)
	}

	if data["ErrorCode"] != nil {
		r.ErrorCode = data["ErrorCode"].(string)
	}

	if data["FailureStatus"] != nil {
		r.FailureStatus = FailureStatusCode(data["FailureStatus"].(string))
	}

	if data["Details"] != nil {
		r.Details = data["Details"].(string)
	}

	if data["OriginatesFromUpdatePolicy"] != nil {
		r.OriginatesFromUpdatePolicy = strings.EqualFold(data["OriginatesFromUpdatePolicy"].(string), "true")
	}

}

// ToMap converts an ingestion status record to a key value map.
func (r *StatusRecord) ToMap() map[string]interface{} {
	data := make(map[string]interface{})

	// Since we only create the initial record, It's not our responsibility to write the following fields:
	//   OperationID, AcitivityID, ErrorCode, FailureStatus, Details, OriginatesFromUpdatePolicy
	// Those will be read from the server if they have data in them
	data["Status"] = r.Status
	data["IngestionSourceId"] = r.IngestionSourceID
	data["IngestionSourcePath"] = r.IngestionSourcePath
	data["Database"] = r.Database
	data["Table"] = r.Table
	data["UpdatedOn"] = r.UpdatedOn.Format(time.RFC3339Nano)

	return data
}

// String converts an ingestion status record a printable  string.
func (r *StatusRecord) String() string {

	str := fmt.Sprintf("IngestionSourceID: '%s', IngestionSourcePath: '%s', Status: '%s',  FailureStatus: '%s', ErrorCode: '%s', Database: '%s', Table: '%s', UpdatedOn: '%s', OperationID: '%s', ActivityID: '%s', OriginatesFromUpdatePolicy: '%t', Details: '%s'",
		r.IngestionSourceID,
		r.IngestionSourcePath,
		r.Status,
		r.FailureStatus,
		r.ErrorCode,
		r.Database,
		r.Table,
		r.UpdatedOn,
		r.OperationID,
		r.ActivityID,
		r.OriginatesFromUpdatePolicy,
		r.Details)

	return str
}

// ToError converts an ingestion status to an error if failed or partially succeeded, or nil if succeeded.
func (r *StatusRecord) ToError() error {
	switch r.Status {
	case Succeeded:
	case Queued:
		return nil

	case PartiallySucceeded:
		return fmt.Errorf("Ingestion succeeded partially\n" + r.String())
	}

	return fmt.Errorf("Ingestion Failed\n" + r.String())
}
