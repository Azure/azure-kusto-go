# Changelog

## What's New
### Version 0.11.0
* Breaking - Add validation for trusted endpoints by @ohadbitt
    * There are now by default a limited number of valid endpoints for kusto
    * If you are using a standard endpoint - this shouldn't matter
    * The trusted_endpoint package is an api to modify them.
* Send http headers under the new unified format
* Internal Refactoring
### Version 0.10.2
* Fixed issue with managed identity parameters
### Version 0.10.1
* Fixed issue with queued ingestion to other clouds
### Version 0.10.0
* [BREAKING] - The minimal go version is now 1.19
* [BREAKING] - Moving to a connection-string based approach to creating and authenticating clients.  
  This change aligns the go SDK with the others, and gives the option to re-use connection strings between SDKs.   
  As part of this change use of `go-autorest` based authentication is deprecated in favor of Azure Identity.

  To initialize a client:
```go
    // OLD WAY - REMOVED
    authConfig := auth.NewClientCredentialsConfig("clientID", "clientSecret", "tenantID")
    client, err := kusto.New("endpoint", kusto.Authorization{Config: authConfig})
    
    // NEW WAY
    kcsb := kusto.NewConnectionStringBuilder(`endpoint`).WithAadAppKey("clientID", "clientSecret", "tenentID")
    client, err := kusto.New(kcsb)
```
* [BREAKING] - Upgraded the azblob library to 0.6.1 This solves compatibility issues with other libraries, but might cause errors to those who still depend on the old version.

* Implicit cloud detection.
* All of our operations now share the same HTTP client inside the kusto client object.  
  Using the option `WithHttpClient` will use the passed http client for all of the SDKs request, granting support for configuring proxies and other HTTP related settings.

* Fixed various goroutine leaks. Now there are automatic tests to make sure we are not leaking resources.
* Fetching ingestion resources is now done more consistently, without blocking the user.
* Removed the header caching mechanism from streaming ingestion, as it was using a lot of memory for no major benefit.

### Version 0.9.1
* Setting a mapping now implies the ingestion format
* Fixed possible context race
  com/Azure/azure-kusto-go/pull/134
* Json parsing errors now display the failed json string
* E2E tests require fewer prerequisites

### Version 0.9.0
* Deprecate AllowWrite - now it is the default like in other SDKs.
* Remove mutex from query client. Now queries can run in parallel, achieving much better performance.
* Fix Column.Type assignment. Was using string, now using types.  by @jesseward
* Lint and test fixes
### Version 0.8.1
* Added `Application` and `User` as `ClientRequestProperties` to set the `x-ms-app` and `x-ms-user` headers, and the matching fields in `.show queries`.
### Version 0.8.0
* Add all missing client request properties, and the ability to use custom ones using `CustomQueryOption`
* Add the option to not parse the response when querying, but to receive the json directly - `QueryToJson`
* Various lint fixes and code improvements

### Version 0.7.0
* Make clients closeable
* Support port in http host
* Add retry mechanism for throttled requests
* Added custom http options for all clients

### Version 0.6.0
#### Deprecations
* `Ingestion.Stream` has been deprecated in favor of dedicated streaming clients - `ingest.Streaming` and `ingest.Managed`.
  This API was very limited - it required you to create a queued ingestion client, it only accepted a byte array, and had no customization options.
* `RowIterator.Next` and `RowIterator.Do` are now deprecated and replaced by `RowIterator.NextRowOrError` and `RowIterator.DoOnRowOrError`.
  In previous versions, when encountering an error in-line with the results (also known as partial success), the SDK panicked. Now `RowIterator.Next` and `RowIterator.Do` will return the first error they encounter, including in-line errors or partials successes and finish.
  This means that there could be extra data that will be skipped when using these APIs. Fixed #81 
