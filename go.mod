module github.com/Azure/azure-kusto-go/sample

go 1.13

require (
	github.com/Azure/azure-kusto-go/ingest v0.0.0-00010101000000-000000000000 // indirect
	github.com/Azure/go-autorest v13.3.0+incompatible
)

replace github.com/Azure/azure-kusto-go/data => ./data

replace github.com/Azure/azure-kusto-go/ingest => ./ingest
