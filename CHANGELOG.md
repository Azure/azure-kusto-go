# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-02-19

### Changed
- [BREAKING] The minimal go version is now 1.22
- [BREAKING]  Split the main module into two packages:
  - azkustodata - contains querying, management APIs.
  - azkustoingest - contains ingestion in all its forms.
- [BREAKING]  New API for querying, see MIGRATION.md for more details.
- [BREAKING]  Constructing ingest clients is now done using a KustoConnectionStringBuilder, and not a client struct.
- [BREAKING]  Changes in the kusto type system:
  - Kusto values will now return a pointer when they are nullable. This applies to all types except for string.
  - Decimal values are now represented as `decimal.Decimal` instead of `string`. This is to maintain efficiency and ease of use.
- [BREAKING] Aligned KCSB (Kusto Connection String Builder) parsing with other SDKS:
  - Removed keywords `InteractiveLogin` and `RedirectURL`
  - Keywords are now case-insensitive, and ignore spaces.
  - Added `GetConnectionString(includeSecrets bool)` method to KCSB, to get a canonical connection string, with or without secrets.
  - the `WithApplicationCertificate` on `KustoConnectionStringBuilder` was removed as it was ambiguous and not implemented correctly. Instead there are two new methods:
    - `WithAppCertificatePath` - Receives the path to the certificate file.
    - `WithAppCertificateBytes` - Receives the certificate bytes in-memory.  
      Both methods accept an optional password for the certificate.
  - `WithUserManagedIdentity` has been removed in favor of more specific functions:
    - `WithUserAssignedIdentityClientId` - Receives the MSI client id
    - `WithUserAssignedIdentityResourceId` - Receives the MSI resource id
- [BREAKING] The Dynamic type now returns a []byte of json, it's up to the user to marshall it to the desired type. It can also be null.
- [BREAKING] ManagedStreamingClient constructor now only requires the query endpoint, and will infer the ingest endpoint from it. If you want to use a different endpoint, use the `azkustoingest.WithCustomIngestConnectionString()` option.
- [BREAKING] Removed the old deprecated Stream() method on queued ingest client, instead use azkustoingest.NewStreaming() or azkustoingest.NewManaged() for proper streaming ingest client.
- [BREAKING] Removed `QueryIngestion()` option for Query client. If you want to perform commands against the dm, create a query client with the "ingest-" endpoint.

  
# Added
- Added autocorrection for endpoints for ingest clients. When creating a client, the "ingest-" will be added or removed as needed. To avoid this behavior, use the `azkustoingest.WithoutEndpointCorrection()` option.
- Passing a default database and table for ingestion is not necessary anymore, and can be done using Options.
   ```go
   // before:
  	queryClient := kusto.New("https://ingest-somecluster.kusto.windows.net")
    client := ingest.New(quetryClient, "some-db", "some-table")
  
    // after:
    client := azkustoingest.New("https://ingest-somecluster.kusto.windows.net", azkustoingest.WithDefaultDatabase("someDb"), azkustoingest.WithDefaultTable("someTable"))
  ```


## [1.0.0-preview-5] - 2024-09-09

### Fixed
- Proper parsing of booleans for rare cases where the values are returned as integers.
- Reverted new json library to the default one, as it was causing issues with edge cases.

## [1.0.0-preview-4] - 2024-08-27
### Changed

- V2FrameCapacity was renamed to V2IoCapacity to better reflect its purpose.
- V2FragmentCapacity was renamed to V2TableCapacity to better reflect its purpose.
- Removed `Skip` option from `IterativeTable`, as the usecase for it was not clear.
- Better defaults for buffer sizes.

### Fixed
- Fixed Mapping Kind not working correctly with certain formats.
- Fixed plenty of sync issues.
- Reduced allocations.

### Security
- Use the new azqueue library.
- Various dependency updates.

## [1.0.0-preview-3] - 2024-06-05
### Added
- Row and fragment capacity options to iterative dataset creation.
- Added RawV2 method for manual parsing.
### Changed
- Changed frame defaults to be more reasonable

## [1.0.0-preview-2] - 2024-04-01
### Changed
- [BREAKING] The Dynamic type now returns a []byte of json, it's up to the user to marshall it to the desired type. It can also be null.
- E2E tests now avoid creating tables when possible.
### Added
- Methods for getting a guid value (were missing).
- Support obscure column aliases.
### Fixed
- Fixed a panic when using the xByY methods on a null value.
- Fixed race in closing the client.
- Fixed special float values not being parsed correctly.

## [1.0.0-preview] - 2024-03-11

### Added
- [BREAKING] The minimal go version is now 1.22
- [BREAKING] [MAJOR] Split the main module into two packages:
    - azkustodata - contains querying, management APIs.
    - azkustoingest - contains ingestion in all its forms.
- [BREAKING] [MAJOR] New API for querying, see MIGRATION.md for more details.
- [BREAKING] [MAJOR] Constructing ingest clients is now done using a KustoConnectionStringBuilder, and not a client struct.
- [BREAKING] [MAJOR] Changes in the kusto type system:
    - Kusto values will now return a pointer when they are nullable. This applies to all types except for string.
    - Decimal values are now represented as `decimal.Decimal` instead of `string`. This is to maintain efficiency and ease of use.
- In addition, passing a default database and table for ingestion is not necessary anymore, and can be done using Options.
   ```go
   // before:
  	queryClient := kusto.New("https://ingest-somecluster.kusto.windows.net")
    client := ingest.New(quetryClient, "some-db", "some-table")
  
    // after:
    client := azkustoingest.New("https://ingest-somecluster.kusto.windows.net", azkustoingest.WithDefaultDatabase("someDb"), azkustoingest.WithDefaultTable("someTable"))
  ```
- Added autocorrection for endpoints for ingest clients. When creating a client, the "ingest-" will be added or removed as needed. To avoid this behavior, use the `azkustoingest.WithoutEndpointCorrection()` option.
- ManagedStreamingClient constructor now only requires the query endpoint, and will infer the ingest endpoint from it. If you want to use a different endpoint, use the `azkustoingest.WithCustomIngestConnectionString()` option.
- Removed the old deprecated Stream() method on queued ingest client, instead use azkustoingest.NewStreaming() or azkustoingest.NewManaged() for proper streaming ingest client.
- Removed `QueryIngestion()` option for Query client. If you want to perform commands against the dm, create a query client with the "ingest-" endpoint.


## [0.15.1] - 2024-03-04

### Changed

- Binary data formats are no longer compressed, as it is inefficient.

### Fixed

- Type aliases for int32 now work correctly when converting.



## [0.15.0] - 2023-12-04

### Changed (BREAKING)
- Queries are no longer progressive by default.
- `ResultsProgressiveDisable()` has been removed.
- Use `ResultsProgressiveEnabled()` to enable progressive queries.

### Added
- Add file options: RawDataSize, CompressionType
- New package ingest/ingestoptions now contains Compression properties (in the future will hold DataFormat)

### Fixed

- String quoting in default value of query parameters

## [0.14.2] - 2023-11-08

### Fixed

-  Size used for RawDataSize taken from gzip reader was of the gzip size and not the original reader size

## [0.14.1] - 2023-09-27

### Added
- Support new playfab domain


### Fixed

- Fixed deadlock when having high number of concurrent queries
- Fixed wrong endpoint error not triggering

## [0.14.0] - 2023-08-10

### Added

- Support streaming for blob, for Managed client as well.
- Support more urls for kusto, including http and port.

### Fixed

* Fixed wrong context deadline setting
* Fixed accepting empty url.


## [0.13.1] - 2023-05-24

### Changed

- Modified `once.go` to reset `sync.Once` instance when an error occurs

## [0.13.0] - 2023-05-09

### Added

- `ServerTimeout` Query Option
    - The timeout to the server will be set to the value of this option, or to none if `RequestNoTimeout` is set to
      true.
    - If it is not provided, the timeout will be set by the context (the old behaviour).
    - If a context timeout is not provided, it will fall back to a default value by the type of request.
- Support for `IgnoreFirstRecord` ingestion option

### Changed

- `MgmtOption` is deprecated. From now on both `Query` and `Mgmt` accept `QueryOption`, `MgmtOption` will remain as an
  alias until the next version.

### Fixed

- `AttachPolicyClientOptions` method fixed by @JorTurFer

### Removed

- `AllowWrite` has been a no-op for a while. It is now finally removed.

### Security

## [0.12.1] - 2023-05-01

### Fixed

* Fixed parsing of errors in queries

## [0.12.0] - 2023-05-01

### Added

* Added kql.Builder struct for safe building of KQL statements from variables without use of 'Unsafe' mode.
    * Simpler handling of query parameters using kql.Parameters struct.
    * All of the docs and examples have been updated to use it
    * [DEPRECATED] The old query builder
* Added Quickstart app
* TokenCredential support for authentication.

### Security

* No redirects are allowed by default.

### Fixed

* Replace non-ascii characters in headers to be in line with the service.
* DefaultCredential now uses the same HTTP client as the rest of the SDK.

## [0.11.3] - 2023-03-20

### Added

* Support for new trident url

## [0.11.2] - 2023-03-14

### Fixed

* Fixed Queue Uri not being correct for different clouds

## [0.11.1] - 2023-03-01

### Changed

* Bumped azblob to 1.0.0

### Fixed

* Fixed Storage Uri not being correct for different clouds

### Security

* Bump golang.org/x/net from 0.4.0 to 0.7.0

## [0.11.0] - 2023-02-14

### Changed

- [BREAKING] Add validation for trusted endpoints by @ohadbitt
- Send http headers under the new unified format

### Fixed

- Internal Refactoring

## [0.10.2] - 2022-12-26

### Fixed

- Issue with managed identity parameters

## [0.10.1] - 2022-12-14

### Fixed

- Issue with queued ingestion to other clouds

## [0.10.0] - 2022-12-11

### Changed

- [BREAKING] The minimal go version is now 1.19
- [BREAKING] Moving to a connection-string based approach to creating and authenticating clients.

### Added

- Implicit cloud detection.
- All of our operations now share the same HTTP client inside the kusto client object.

### Fixed

- Various goroutine leaks.
- Fetching ingestion resources is now done more consistently.
- Removed the header caching mechanism from streaming ingestion.

## [0.9.2] - 2022-12-01

### Fixed

- Default values for parameters not parsing correctly
- Goroutine leak when streaming ingestion fails
- Leaks in tests
-

## [0.9.1] - 2022-11-20

### Changed

- Setting a mapping now implies the ingestion format

### Fixed

- Possible context race
- Json parsing errors now display the failed json string

## [0.9.0] - 2022-11-09

### Changed

- Deprecate AllowWrite - now it is the default like in other SDKs.
- Remove mutex from query client. Now queries can run in parallel, achieving much better performance.

### Fixed

- Column.Type assignment. Was using string, now using types. by @jesseward
- Lint and test fixes

## [0.8.1] - 2022-09-21

### Added

- `Application` and `User` as `ClientRequestProperties` to set the `x-ms-app` and `x-ms-user` headers, and the matching
  fields in `.show queries`.

## [0.8.0] - 2022-09-05

### Added

- All missing client request properties, and the ability to use custom ones using `CustomQueryOption`
- The option to not parse the response when querying, but to receive the json directly - `QueryToJson`

### Changed

- Various lint fixes and code improvements

## [0.7.0] - 2022-05-08

### Added

- Make clients closeable
- Support port in http host
- Retry mechanism for throttled requests
- Custom http options for all clients

## [0.6.0] - 2022-04-12

### Deprecated

* `Ingestion.Stream` has been deprecated in favor of dedicated streaming clients - `ingest.Streaming`
  and `ingest.Managed`.
* `RowIterator.Next` and `RowIterator.Do` are now deprecated and replaced by `RowIterator.NextRowOrError`
  and `RowIterator.DoOnRowOrError`.

### Fixed

* RowIterator.Next and RowIterator.Do will return the first error they encounter, including in-line errors or partials
  successes and finish.
