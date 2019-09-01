package azkustoingest

import (
	"azure-kusto-go/azure-kusto-data/azkustodata"
	"fmt"
)

type IngestionResources struct {
	securedReadyForAggregationQueues []string
	containers                       []string
}

type ResourceManager struct {
	client    azkustodata.KustoClient
	resources IngestionResources
}

func NewResourceManager(client azkustodata.KustoClient) (*ResourceManager, error) {
	return &ResourceManager{
		client: client,
	}, nil;
}

type IngestionResourcesFetcher interface {
	FetchIngestionResources() (*IngestionResources, error)
}

type IngestionResourceProvider interface {
	GetIngestionQueues() ([]string, error)
	GetStorageAccount() (string, error)
}

func getResourceFromServer(client azkustodata.KustoClient) {

}

func (rm *ResourceManager) FetchIngestionResources() (*IngestionResources, error) {
	resourcesResponse, err := rm.client.Execute("NetDefaultDB", ".get ingestion resources")

	storage := make([]string, 0)
	securedReadyForAggregationQueues := make([]string, 0)

	primary, err := resourcesResponse.GetPrimaryResults()

	var resourceTypeCol int
	var resourceUriCol int
	for i, v := range primary[0].GetColumns() {
		if v.ColumnName == "ResourceType" {
			resourceTypeCol = i
		}
		if v.ColumnName == "ResourceUri" {
			resourceUriCol = i
		}

	}

	for _, row := range primary[0].GetRows() {
		switch row[resourceTypeCol] {
		case "SecuredReadyForAggregationQueue":
			{
				securedReadyForAggregationQueues = append(securedReadyForAggregationQueues, fmt.Sprint(row[resourceUriCol]))
			}
		case "TempStorage":
			{
				storage = append(storage, fmt.Sprint(row[resourceUriCol]))
			}
		}
	}

	if err != nil {
		return nil, err
	}

	//return &{
	//	securedReadyForAggregationQueues: securedReadyForAggregationQueues,
	//	containers: storage
	//}, nil;
}

func (rm *ResourceManager) GetIngestionQueues() ([]string, error) {
	return nil, nil;
}

func (rm *ResourceManager) GetStorageAccount() (string, error) {
	return nil, nil;
}


secured_ready_for_aggregation_queues = self._get_resource_by_name(table, )
failed_ingestions_queues = self._get_resource_by_name(table, "FailedIngestionsQueue")
successful_ingestions_queues = self._get_resource_by_name(table, "SuccessfulIngestionsQueue")
containers = self._get_resource_by_name(table, "")
status_tables = self._get_resource_by_name(table, "IngestionsStatusTable")

return _IngestClientResources(
secured_ready_for_aggregation_queues,
failed_ingestions_queues,
successful_ingestions_queues,
containers,
status_tables,
)

def _refresh_authorization_context(self):
if (
not self._authorization_context
or self._authorization_context.isspace()
or (self._authorization_context_last_update + self._refresh_period) <= datetime.utcnow()
):
self._authorization_context = self._get_authorization_context_from_service()
self._authorization_context_last_update = datetime.utcnow()

def _get_authorization_context_from_service(self):
return self._kusto_client.execute("NetDefaultDB", ".get kusto identity token").primary_results[0][0][
"AuthorizationContext"
]

def get_ingestion_queues(self):
self._refresh_ingest_client_resources()
return self._ingest_client_resources.secured_ready_for_aggregation_queues

def get_failed_ingestions_queues(self):
self._refresh_ingest_client_resources()
return self._ingest_client_resources.failed_ingestions_queues

def get_successful_ingestions_queues(self):
self._refresh_ingest_client_resources()
return self._ingest_client_resources.successful_ingestions_queues

def get_containers(self):
self._refresh_ingest_client_resources()
return self._ingest_client_resources.containers

def get_ingestions_status_tables(self):
self._refresh_ingest_client_resources()
return self._ingest_client_resources.status_tables

def get_authorization_context(self):
self._refresh_authorization_context()
return self._authorization_context
