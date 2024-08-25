# Setup for End to End Integration Test

## Introduction

The Kusto SDK is setup to use hermetic tests throughout the SDK.  This makes testing faster and is preferable to large integration
tests that often miss all the corner cases.

For anyone who wants to make some change and runs:  `go test ./...`, everything should just work, except this integration test.  
It will be skipped until you setup the necessary config file. 

In addition, if using the `-short` option, the integration test will be skipped.

```
go test -short ./...
```

Before any changes will be accepted into the SDK, you will need to setup and pass the integration test. While integration tests are error prone, they do offer the only method to check the SDK against the real backend.

## Setup

The only thing needed to test Kusto is a:
- Kusto instance
- Credentials
- Principal names

Setting up a Kusto instance is beyond this short guide. See the [quickstart](https://docs.microsoft.com/en-us/azure/data-explorer/create-cluster-database-portal) guide.

Once the kusto instance is setup, you will need to create a `config.json` file in the directory where this README is located.

It will need to have the following content:

```json
{
    "Endpoint": "https://[cluster name].westus.kusto.windows.net",
	"Database": "[database_name]",
	"ClientID": "[client ID]",
	"ClientSecret": "[client secret]",
    "TenantID": "[tenant ID]"
}
```

## Running the test

Simply run this from the directory:

```
go test -timeout=10m
```

Or you can do this from the root:

```
go test -timeout=10m ./...
```

Normally this runs under 2 minutes.  The streaming test is our slow one.

## Caveats

There is no compatibility guarentee on tests or the config.json file. During any update, including minor or patch semver
changes, we may change the config.json format.
