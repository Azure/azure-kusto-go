package ingest

import "time"

// Status is the ingestion status
type Status = int

const (
	// Pending status represents a temporary status.
	// Might change during the course of ingestion based on the
	// outcome of the data ingestion operation into Kusto.
	Pending Status = 0
	// Succeeded status represents a permanent status.
	// The data has been successfully ingested to Kusto.
	Succeeded Status = 1
	// Failed Status represents a permanent status.
	// The data has not been ingested to Kusto.
	Failed Status = 2
	// Queued status represents a permanent status.
	// The data has been queued for ingestion.
	// (This does not indicate that the ingestion was successful.)
	Queued Status = 4
	// Skipped status represents a permanent status.
	// No data was supplied for ingestion. The ingest operation was skipped.
	Skipped Status = 5
	// PartiallySucceeded status represents a permanent status.
	// Part of the data was successfully ingested to Kusto, while other parts failed.
	PartiallySucceeded Status = 6
)

// FailureStatus indicates the status of failuted ingestion attempts
type FailureStatus = int

const (
	// Unknown represents an undefined or unset failure state
	Unknown FailureStatus = 0
	// Permanent represnets failure state that will benefit from a retry attempt
	Permanent FailureStatus = 1
	// Transient represnet a retryable failure state
	Transient FailureStatus = 2
	// Exhausted represents a retryable failure that has exhusted all retry attempts
	Exhausted FailureStatus = 3
)

// TableIngestionInfo is a record containing information regarding the status of an ingation command
type TableIngestionInfo struct {
	// Azure Table Fields

	// PartitionKey
	PartitionKey string
	// RowKey
	RowKey string
	// Timestamp
	Timestamp time.Time
	// ETag
	ETag string

	// Ingestion Status Data
	
        // The ingestion status returned from the service. Status remains 'Pending' during the ingestion process and
        // is updated by the service once the ingestion completes. When <see cref="IngestionReportMethod"/> is set to 'Queue', the ingestion status
        // will always be 'Queued' and the caller needs to query the reports queues for ingestion status, as configured. To query statuses that were
        // reported to queue, see: <see href="https://docs.microsoft.com/en-us/azure/kusto/api/netfx/kusto-ingest-client-status#ingestion-status-in-azure-queue"/>.
        // When <see cref="IngestionReportMethod"/> is set to 'Table', call <see cref="IKustoIngestionResult.GetIngestionStatusBySourceId"/> or
        // <see cref="IKustoIngestionResult.GetIngestionStatusCollection"/> to retrieve the most recent ingestion status.
        // </summary>
        public Status Status { get; set; }

        
        // A unique identifier representing the ingested source. It can be supplied during the ingestion execution. 
        // </summary>
        public Guid IngestionSourceId { get; set; }

        
        // The URI of the blob, potentially including the secret needed to access
        // the blob. This can be a filesystem URI (on-premises deployments only),
        // or an Azure Blob Storage URI (including a SAS key or a semicolon followed
        // by the account key)
        // </summary>
        public string IngestionSourcePath { get; set; }

        
        // The name of the database holding the target table.
        // </summary>
        public string Database { get; set; }

        
        // The name of the target table into which the data will be ingested.
        // </summary>
        public string Table { get; set; }

        
        // The last updated time of the ingestion status.
        // </summary>
        public DateTime UpdatedOn { get; set; }

        
        // The ingestion's operation ID.
        // </summary>
        public Guid OperationId { get; set; }

        
        // The ingestion's activity ID.
        // </summary>
        public Guid ActivityId { get; set; }

        
        // In case of a failure, indicates the failure's error code.
        // </summary>
        public IngestionErrorCode ErrorCode { get; set; }

        
        // In case of a failure, indicates the failure's status.
        // </summary>
        public FailureStatus FailureStatus { get; set; }
        
        
        // In case of a failure, indicates the failure's details.
        // </summary>
        public string Details { get; set; }

        
        // In case of a failure, indicates whether or not the failure originated from an Update Policy.
        // </summary>
        public bool OriginatesFromUpdatePolicy { get; set; }
}
