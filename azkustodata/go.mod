module github.com/Azure/azure-kusto-go/azkustodata

go 1.22

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.17.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.8.1
	// This is a faster drop-in replacement for encoding/json.
	// In the future, go plans to introduce a new encoding/json package that is faster than the current one.
	// We will switch to that when it is available.
	github.com/google/uuid v1.6.0
	github.com/kylelemons/godebug v1.1.0
	github.com/samber/lo v1.47.0
	github.com/shopspring/decimal v1.4.0
	github.com/stretchr/testify v1.10.0
	github.com/tj/assert v0.0.3
	go.uber.org/goleak v1.3.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.10.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.3.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang-jwt/jwt/v5 v5.2.1 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.32.0 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
