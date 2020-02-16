module github.com/Azure/azure-kusto-go/kusto/ingest

go 1.13

require (
	github.com/Azure/azure-kusto-go/kusto v0.0.0
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Azure/azure-storage-queue-go v0.0.0-20191125232315-636801874cdd
	github.com/kr/pretty v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/satori/go.uuid v1.2.0
	golang.org/x/net v0.0.0-20200202094626-16171245cfb2 // indirect
	golang.org/x/sys v0.0.0-20200202164722-d101bd2416d5 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
)

replace github.com/Azure/azure-kusto-go/kusto => ../
