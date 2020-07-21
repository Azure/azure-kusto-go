package status

import (
	"time"

	"github.com/google/uuid"
)

// IngestionStatus is the ingestion status
type IngestionStatus = int

const (
	// Pending status represents a temporary status.
	// Might change during the course of ingestion based on the
	// outcome of the data ingestion operation into Kusto.
	Pending IngestionStatus = 0
	// Succeeded status represents a permanent status.
	// The data has been successfully ingested to Kusto.
	Succeeded IngestionStatus = 1
	// Failed Status represents a permanent status.
	// The data has not been ingested to Kusto.
	Failed IngestionStatus = 2
	// Queued status represents a permanent status.
	// The data has been queued for ingestion.
	// (This does not indicate that the ingestion was successful.)
	Queued IngestionStatus = 4
	// Skipped status represents a permanent status.
	// No data was supplied for ingestion. The ingest operation was skipped.
	Skipped IngestionStatus = 5
	// PartiallySucceeded status represents a permanent status.
	// Part of the data was successfully ingested to Kusto, while other parts failed.
	PartiallySucceeded IngestionStatus = 6
)

// IngestionFailureStatus indicates the status of failuted ingestion attempts
type IngestionFailureStatus = int

const (
	// Unknown represents an undefined or unset failure state
	Unknown IngestionFailureStatus = 0
	// Permanent represnets failure state that will benefit from a retry attempt
	Permanent IngestionFailureStatus = 1
	// Transient represnet a retryable failure state
	Transient IngestionFailureStatus = 2
	// Exhausted represents a retryable failure that has exhusted all retry attempts
	Exhausted IngestionFailureStatus = 3
)

// IngestionStatusRecord is a record containing information regarding the status of an ingation command
type IngestionStatusRecord struct {
	// The ingestion status returned from the service. Status remains 'Pending' during the ingestion process and
	// is updated by the service once the ingestion completes. When <see cref="IngestionReportMethod"/> is set to 'Queue', the ingestion status
	// will always be 'Queued' and the caller needs to query the reports queues for ingestion status, as configured. To query statuses that were
	// reported to queue, see: <see href="https://docs.microsoft.com/en-us/azure/kusto/api/netfx/kusto-ingest-client-status#ingestion-status-in-azure-queue"/>.
	// When <see cref="IngestionReportMethod"/> is set to 'Table', call <see cref="IKustoIngestionResult.GetIngestionStatusBySourceId"/> or
	// <see cref="IKustoIngestionResult.GetIngestionStatusCollection"/> to retrieve the most recent ingestion status.
	Status IngestionStatus

	// A unique identifier representing the ingested source. It can be supplied during the ingestion execution.
	IngestionSourceID uuid.UUID `json:"Id"`

	// The URI of the blob, potentially including the secret needed to access
	// the blob. This can be a filesystem URI (on-premises deployments only),
	// or an Azure Blob Storage URI (including a SAS key or a semicolon followed
	// by the account key)
	IngestionSourcePath string

	// The name of the database holding the target table.
	Database string

	// The name of the target table into which the data will be ingested.
	Table string

	// The last updated time of the ingestion status.
	UpdatedOn time.Time

	// The ingestion's operation ID.
	OperationID uuid.UUID

	// The ingestion's activity ID.
	ActivityID uuid.UUID

	// In case of a failure, indicates the failure's error code.
	// TODO make this into a const list
	ErrorCode int

	// In case of a failure, indicates the failure's status.
	FailureStatus IngestionFailureStatus

	// In case of a failure, indicates the failure's details.
	Details string `json:",omitempty"`

	// In case of a failure, indicates whether or not the failure originated from an Update Policy.
	OriginatesFromUpdatePolicy bool
}
