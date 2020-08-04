package ingest

import (
	"fmt"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"
	"github.com/google/uuid"
)

// StatusCode is the ingestion status
type StatusCode int

const (
	// Pending status represents a temporary status.
	// Might change during the course of ingestion based on the
	// outcome of the data ingestion operation into Kusto.
	Pending StatusCode = 0
	// Succeeded status represents a permanent status.
	// The data has been successfully ingested to Kusto.
	Succeeded StatusCode = 1
	// Failed Status represents a permanent status.
	// The data has not been ingested to Kusto.
	Failed StatusCode = 2
	// Queued status represents a permanent status.
	// The data has been queued for ingestion &  status tracking was not requested.
	// (This does not indicate that the ingestion was successful.)
	Queued StatusCode = 4
	// Skipped status represents a permanent status.
	// No data was supplied for ingestion. The ingest operation was skipped.
	Skipped StatusCode = 5
	// PartiallySucceeded status represents a permanent status.
	// Part of the data was successfully ingested to Kusto, while other parts failed.
	PartiallySucceeded StatusCode = 6

	// StatusRetrievalFailed means the client ran into truble reading the status from the service
	StatusRetrievalFailed StatusCode = 100
	// StatusRetrievalCanceled means the user canceld the status check
	StatusRetrievalCanceled StatusCode = 101
	// ClientError an error was detected on the client side
	ClientError StatusCode = 102
)

// IsFinal returns true if the ingestion status is a final status, or false if the status is temporary
func (i StatusCode) IsFinal() bool {
	return i != Pending
}

// ToString returns a string represnetation of StatusCode
func (i StatusCode) ToString() string {
	switch i {
	case Pending:
		return "Pending"

	case Succeeded:
		return "Succeeded"

	case Failed:
		return "Failed"

	case Queued:
		return "Queued"

	case Skipped:
		return "Skipped"

	case PartiallySucceeded:
		return "PartiallySucceeded"

	case StatusRetrievalCanceled:
		return "StatusRetrievalCanceled"

	case StatusRetrievalFailed:
		return "StatusRetrievalFailed"

	case ClientError:
		return "ClientError"
	}

	return "Undefined Status Code Value: " + i.ToString()
}

// FailureStatusCode indicates the status of failuted ingestion attempts
type FailureStatusCode int

const (
	// Unknown represents an undefined or unset failure state
	Unknown FailureStatusCode = 0
	// Permanent represnets failure state that will benefit from a retry attempt
	Permanent FailureStatusCode = 1
	// Transient represnet a retryable failure state
	Transient FailureStatusCode = 2
	// Exhausted represents a retryable failure that has exhusted all retry attempts
	Exhausted FailureStatusCode = 3
)

// ToString returns a string represnetation of FailureStatusCode
func (f FailureStatusCode) ToString() string {
	switch f {
	case Unknown:
		return "Unknown"

	case Permanent:
		return "Permanent"

	case Transient:
		return "Transient"

	case Exhausted:
		return "Exhausted"
	}

	return "Undefined Failure Status Code Value: " + f.ToString()
}

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
	IngestionSourceID uuid.UUID `json:"Id"`

	// The URI of the blob, potentially including the secret needed to access
	// the blob. This can be a filesystem URI (on-premises deployments only),
	// or an Azure Blob Storage URI (including a SAS key or a semicolon followed
	// by the account key)
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
	ErrorCode int

	// FailureStatus - In case of a failure, indicates the failure's status.
	FailureStatus FailureStatusCode

	// Details - In case of a failure, indicates the failure's details.
	Details string `json:",omitempty"`

	// OriginatesFromUpdatePolicy - In case of a failure, indicates whether or not the failure originated from an Update Policy.
	OriginatesFromUpdatePolicy bool
}

// newStatusRecord creates a new record initialized with defaults and user provided data
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
		ErrorCode:                  0,
		FailureStatus:              Unknown,
		Details:                    "",
		OriginatesFromUpdatePolicy: false,
	}

	return rec
}

// FromProps takes in data from ingestion options
func (r *StatusRecord) FromProps(props properties.All) {
	r.IngestionSourceID = props.Source.ID
	r.Database = props.Ingestion.DatabaseName
	r.Table = props.Ingestion.TableName
	r.UpdatedOn = time.Now()

	if props.Ingestion.BlobPath != "" && r.IngestionSourcePath == "Undefined" {
		r.IngestionSourcePath = props.Ingestion.BlobPath
	}
}

// FromMap converts an ingestion status record to a key value map
func (r *StatusRecord) FromMap(data map[string]interface{}) {
	r.Status = data["Status"].(StatusCode)
	r.IngestionSourceID = data["IngestionSourceID"].(uuid.UUID)
	r.IngestionSourcePath = data["IngestionSourcePath"].(string)
	r.Database = data["Database"].(string)
	r.Table = data["Table"].(string)
	r.UpdatedOn = data["UpdatedOn"].(time.Time)
	r.OperationID = data["OperationID"].(uuid.UUID)
	r.ActivityID = data["ActivityID"].(uuid.UUID)
	r.ErrorCode = data["ErrorCode"].(int)
	r.FailureStatus = data["FailureStatus"].(FailureStatusCode)
	r.Details = data["Details"].(string)
	r.OriginatesFromUpdatePolicy = data["OriginatesFromUpdatePolicy"].(bool)
}

// ToMap converts an ingestion status record to a key value map
func (r *StatusRecord) ToMap() map[string]interface{} {
	data := make(map[string]interface{})

	data["Status"] = r.Status
	data["IngestionSourceID"] = r.IngestionSourceID
	data["IngestionSourcePath"] = r.IngestionSourcePath
	data["Database"] = r.Database
	data["Table"] = r.Table
	data["UpdatedOn"] = r.UpdatedOn
	data["OperationID"] = r.OperationID
	data["ActivityID"] = r.ActivityID
	data["ErrorCode"] = r.ErrorCode
	data["FailureStatus"] = r.FailureStatus
	data["Details"] = r.Details
	data["OriginatesFromUpdatePolicy"] = r.OriginatesFromUpdatePolicy

	return data
}

// ToString converts an ingestion status record a printable  string
func (r *StatusRecord) ToString() string {

	str := fmt.Sprintf("IngestionSourceID: '%s', IngestionSourcePath: '%s', Status: '%s',  FailureStatus: '%s', ErrorCode: '%d', Database: '%s', Table: '%s', UpdatedOn: '%s', OperationID: '%s', ActivityID: '%s', OriginatesFromUpdatePolicy: '%t', Details: '%s'",
		r.IngestionSourceID,
		r.IngestionSourcePath,
		r.Status.ToString(),
		r.FailureStatus.ToString(),
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

// ToError converts an ingestion status to an error if failed or partially succeeded, or nil if succeeded
func (r *StatusRecord) ToError() error {
	switch r.Status {
	case Succeeded:
	case Queued:
		return nil

	case PartiallySucceeded:
		return fmt.Errorf("Ingestion succeeded partially\n" + r.ToString())
	}

	return fmt.Errorf("Ingestion Failed\n" + r.ToString())
}
