# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.14.0]
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
  - The timeout to the server will be set to the value of this option, or to none if `RequestNoTimeout` is set to true.
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
