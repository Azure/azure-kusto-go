module github.com/Azure/azure-kusto-go/

go 1.13

require (
	github.com/Azure/azure-kusto-go/kusto v0.0.0
	github.com/Azure/azure-kusto-go/kusto/ingest v0.0.0-00010101000000-000000000000
	github.com/Azure/go-autorest v13.3.0+incompatible
	github.com/Azure/go-autorest/autorest/azure/auth v0.4.2
)

replace github.com/Azure/azure-kusto-go/kusto => ./kusto

replace github.com/Azure/azure-kusto-go/kusto/ingest => ./kusto/ingest
