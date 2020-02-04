package azkustoingest

import (
	"azure-kusto-go/azure-kusto-data/azkustodata"
	"azure-kusto-go/azure-kusto-data/azkustodata/types"
	"context"
	"fmt"
	"regexp"
)

type ingestionResources struct {
	queues     []resourceUri
	containers []resourceUri
}

type resourceUri struct {
	storageAccountName string
	objectType         string
	objectName         string
	sas                string
}

func (ru resourceUri) String() string {
	return fmt.Sprintf(`https://%s.%s.core.windows.net/%s?%s`,
		ru.storageAccountName, ru.objectType, ru.objectName, ru.sas)
}

var storageUriRegex = regexp.MustCompile(`https://(\w+).(queue|blob|table).core.windows.net/([\w,-]+)\?(.*)`)

func ParseUri(uri string) resourceUri {
	res := storageUriRegex.FindAllStringSubmatch(uri, -1)
	return resourceUri{
		storageAccountName: res[0][1],
		objectType:         res[0][2],
		objectName:         res[0][3],
		sas:                res[0][4],
	}
}

type resourceManager struct {
	client    *azkustodata.Client
	resources *ingestionResources
}

type AuthContextProvider interface {
	getAuthContext() (string, error)
}

type ingestionResourcesFetcher interface {
	fetchIngestionResources() (*ingestionResources, error)
}

type IngestionResourceProvider interface {
	getIngestionQueues() ([]resourceUri, error)
	getStorageAccounts() ([]resourceUri, error)
}

type ingestionResource struct {
	ResourceTypeName types.String
	StorageRoot      types.String
}

func (rm *resourceManager) fetchIngestionResources(ctx context.Context) error {
	rows, err := rm.client.Mgmt(ctx, "NetDefaultDB", ".get ingestion resources")

	if err != nil {
		panic(err)
	}

	var containers []resourceUri
	var queues []resourceUri

	for _, row := range rows {

		rec := ingestionResource{}
		if err := row.ToStruct(&rec); err != nil {
			panic(err)
		}

		switch rec.ResourceTypeName.Value {
		case "TempStorage":
			containers = append(containers, ParseUri(rec.StorageRoot.Value))
		case "SecuredReadyForAggregationQueue":
			queues = append(queues, ParseUri(rec.StorageRoot.Value))
		}
	}

	rm.resources = &ingestionResources{
		queues:     queues,
		containers: containers,
	}

	return nil;
}

type kustoIdentityToken struct {
	AuthorizationContext types.String
}

func (rm *resourceManager) getAuthContext(ctx context.Context) (kustoIdentityToken, error) {
	rows, _ := rm.client.Mgmt(ctx, "NetDefaultDB", ".get kusto identity token")

	token := kustoIdentityToken{}

	if err := rows[0].ToStruct(&token); err != nil {
		panic(err)
	}

	return token, nil
}

func (rm *resourceManager) getIngestionQueues(ctx context.Context) ([]resourceUri, error) {
	if rm.resources == nil {
		_ = rm.fetchIngestionResources(ctx)
	}

	return rm.resources.queues, nil
}

func (rm *resourceManager) getStorageAccounts(ctx context.Context) ([]resourceUri, error) {
	if rm.resources == nil {
		_ = rm.fetchIngestionResources(ctx)
	}

	return rm.resources.containers, nil
}
