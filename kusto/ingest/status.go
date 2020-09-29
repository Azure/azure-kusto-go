package ingest

import (
	"fmt"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/properties"
	"github.com/google/uuid"
	"github.com/kylelemons/godebug/pretty"
	storageuid "github.com/satori/go.uuid"
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
)

// IsFinal returns true if the ingestion status is a final status, or false if the status is temporary
func (i StatusCode) IsFinal() bool {
	return i != Pending
}

// IsSuccess returns true if the status code is a final successfull status code
func (i StatusCode) IsSuccess() bool {
	switch i {
	case Succeeded:
		return true

	case Queued:
		return true

	default:
		return false
	}
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

// IsRetryable indicates whether there's any merit in retying ingestion
func (i FailureStatusCode) IsRetryable() bool {
	switch i {
	case Transient:
		return true

	case Exhausted:
		return true

	default:
		return false
	}
}

// statusRecord is a record containing information regarding the status of an ingation command
type statusRecord struct {
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
func newStatusRecord() statusRecord {
	rec := statusRecord{
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
func (r *statusRecord) FromProps(props properties.All) {
	r.IngestionSourceID = props.Source.ID
	r.Database = props.Ingestion.DatabaseName
	r.Table = props.Ingestion.TableName
	r.UpdatedOn = time.Now()

	if props.Ingestion.BlobPath != "" && r.IngestionSourcePath == "Undefined" {
		r.IngestionSourcePath = props.Ingestion.BlobPath
	}
}

// FromMap converts an ingestion status record to a key value map.
func (r *statusRecord) FromMap(data map[string]interface{}) {

	var strStatus string
	safeSetString(data, "Status", &strStatus)
	if len(strStatus) > 0 {
		r.Status = StatusCode(strStatus)
	}

	safeSetString(data, "FailureStatus", &strStatus)
	if len(strStatus) > 0 {
		r.FailureStatus = FailureStatusCode(strStatus)
	}

	safeSetString(data, "IngestionSourcePath", &r.IngestionSourcePath)
	safeSetString(data, "Database", &r.Database)
	safeSetString(data, "Table", &r.Table)
	safeSetString(data, "ErrorCode", &r.ErrorCode)
	safeSetString(data, "Details", &r.Details)

	if data["IngestionSourceId"] != nil {
		if uid, err := getGoogleUUIDFromInterface(data["IngestionSourceId"]); err == nil {
			r.IngestionSourceID = uid
		}
	}

	if data["OperationId"] != nil {
		if uid, err := getGoogleUUIDFromInterface(data["OperationId"]); err == nil {
			r.OperationID = uid
		}
	}

	if data["ActivityId"] != nil {
		if uid, err := getGoogleUUIDFromInterface(data["ActivityId"]); err == nil {
			r.ActivityID = uid
		}
	}

	if data["UpdatedOn"] != nil {
		if t, err := getTimeFromInterface(data["UpdatedOn"]); err == nil {
			r.UpdatedOn = t
		}
	}

	if data["OriginatesFromUpdatePolicy"] != nil {
		if b, ok := data["OriginatesFromUpdatePolicy"].(bool); ok {
			r.OriginatesFromUpdatePolicy = b
		}
	}
}

// ToMap converts an ingestion status record to a key value map.
func (r *statusRecord) ToMap() map[string]interface{} {
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

// String implements fmt.Stringer.
func (r *statusRecord) String() string {
	return pretty.Sprint(r)
}

// ToError converts an ingestion status to an error if failed or partially succeeded, or nil if succeeded.
func (r statusRecord) Error() string {
	switch r.Status {
	case Succeeded:
		return fmt.Sprintf("Ingestion succeeded\n" + r.String())

	case Queued:
		return fmt.Sprintf("Ingestion Queued\n" + r.String())

	case PartiallySucceeded:
		return fmt.Sprintf("Ingestion succeeded partially\n" + r.String())

	default:
		return fmt.Sprintf("Ingestion Failed\n" + r.String())
	}
}

func getTimeFromInterface(x interface{}) (time.Time, error) {
	switch x.(type) {
	case string:
		return time.Parse(time.RFC3339Nano, x.(string))

	case time.Time:
		return x.(time.Time), nil

	default:
		return time.Now(), fmt.Errorf("getTimeFromInterface: Unexpected format %T", x)
	}
}

func getGoogleUUIDFromInterface(x interface{}) (uuid.UUID, error) {
	switch x.(type) {
	case string:
		return uuid.Parse(x.(string))

	case uuid.UUID:
		return x.(uuid.UUID), nil

	case storageuid.UUID:
		uid, err := uuid.ParseBytes(x.(storageuid.UUID).Bytes())
		if err != nil {
			return uuid.Nil, err
		}

		return uid, err

	default:
		return uuid.Nil, fmt.Errorf("getGoogleUUIDFromInterface: Unexpected format %T", x)
	}
}

func safeSetString(data map[string]interface{}, key string, target *string) {
	if v := data[key]; v != nil {
		if s, ok := v.(string); ok {
			*target = s
		}
	}
}