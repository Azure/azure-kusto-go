package azkustoingest

import (
	"azure-kusto-go/azure-kusto-data/azkustodata"
	"fmt"
	"regexp"
)

type IngestionResources struct {
	securedReadyForAggregationQueues []ResourceUri
	containers                       []ResourceUri
}

type ResourceUri struct {
	storageAccountName string
	objectType         string
	objectName         string
	sas                string
}

func (ru ResourceUri) String() (string) {
	return fmt.Sprintf(`https://%s.%s.core.windows.net/%s?%s`, ru.storageAccountName, ru.objectType, ru.objectName, ru.sas)
}

var storageUriRegex = regexp.MustCompile(`https://(\\w+).(queue|blob|table).core.windows.net/([\\w,-]+)\\?(.*)`)

func ParseUri(uri string) (ResourceUri) {
	res := storageUriRegex.FindAllStringSubmatch(uri, -1)
	return ResourceUri{
		storageAccountName: res[0][0],
		objectType:         res[0][1],
		objectName:         res[0][2],
		sas:                res[0][3],
	}
}

type ResourceManager struct {
	client    azkustodata.KustoClient
	resources *IngestionResources
}

func NewResourceManager(client azkustodata.KustoClient) (*ResourceManager, error) {
	return &ResourceManager{
		client: client,
	}, nil;
}

type AuthContextProvider interface {
	GetAuthContext() (string, error)
}

type IngestionResourcesFetcher interface {
	FetchIngestionResources() (*IngestionResources, error)
}

type IngestionResourceProvider interface {
	GetIngestionQueues() ([]ResourceUri, error)
	GetStorageAccounts() ([]ResourceUri, error)
}

func (rm *ResourceManager) FetchIngestionResources() (error) {
	resourcesResponse, err := rm.client.Execute("NetDefaultDB", ".get ingestion resources")

	if err != nil {
		return err
	}

	containers := make([]ResourceUri, 0)
	securedReadyForAggregationQueues := make([]ResourceUri, 0)

	primary := resourcesResponse.GetPrimaryResults()

	var resourceTypeCol int
	var resourceUriCol int
	for i, v := range primary[0].GetColumns() {
		if v.ColumnName == "ResourceTypeName" {
			resourceTypeCol = i
		}
		if v.ColumnName == "StorageRoot" {
			resourceUriCol = i
		}

	}

	for _, row := range primary[0].GetRows() {
		resourceType := row[resourceTypeCol]
		resourceUri := ParseUri(fmt.Sprint(row[resourceUriCol]))

		switch resourceType {
		case "SecuredReadyForAggregationQueue":
			{
				securedReadyForAggregationQueues = append(securedReadyForAggregationQueues, resourceUri)
			}
		case "TempStorage":
			{
				containers = append(containers, resourceUri)
			}
		}
	}

	rm.resources = &IngestionResources{
		securedReadyForAggregationQueues: securedReadyForAggregationQueues,
		containers:                       containers,
	}

	return nil;
}

func (rm *ResourceManager) GetAuthContext() (string, error) {
	result, err := rm.client.Execute("NetDefaultDB", ".get kusto identity token")

	if err != nil {
		return "", err
	}

	primary := result.GetPrimaryResults()
	columns := primary[0].GetColumns()

	var authContextColIndex int = -1
	for i, col := range columns {
		if col.ColumnName == "AuthorizationContext" {
			authContextColIndex = i
			break
		}
	}

	return primary[0].GetRows()[0][authContextColIndex].(string), nil
}

func (rm *ResourceManager) GetIngestionQueues() ([]ResourceUri, error) {
	if rm.resources == nil {
		_ = rm.FetchIngestionResources()
	}

	return rm.resources.securedReadyForAggregationQueues, nil;
}

func (rm *ResourceManager) GetStorageAccounts() ([]ResourceUri, error) {
	if rm.resources == nil {
		_ = rm.FetchIngestionResources()
	}

	return rm.resources.securedReadyForAggregationQueues, nil;
}
