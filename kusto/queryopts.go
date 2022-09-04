package kusto

// queryopts.go holds the varying QueryOption constructors as the list is so long that
// it clogs up the main kusto.go file.

import (
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/data/value"
)

// requestProperties is a POD used by clients to describe specific needs from the service.
// For more information please look at: https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties
// Not all of the documented options are implemented.
type requestProperties struct {
	Options    map[string]interface{}
	Parameters map[string]string
}

type queryOptions struct {
	requestProperties *requestProperties
}

// NoRequestTimeout enables setting the request timeout to its maximum value.
func NoRequestTimeout() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["norequesttimeout"] = true
		return nil
	}
}

// NoTruncation enables suppressing truncation of the query results returned to the caller.
func NoTruncation() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["notruncation"] = true
		return nil
	}
}

// ResultsProgressiveDisable disables the progressive query stream.
func ResultsProgressiveDisable() QueryOption {
	return func(q *queryOptions) error {
		delete(q.requestProperties.Options, "results_progressive_enabled")
		return nil
	}
}

// queryServerTimeout is the amount of time the server will allow a query to take.
// NOTE: I have made the serverTimeout private. For the moment, I'm going to use the context.Context timer
// to set timeouts via this private method.
func queryServerTimeout(d time.Duration) QueryOption {
	return func(q *queryOptions) error {
		if d > 1*time.Hour {
			return errors.ES(errors.OpQuery, errors.KClientArgs, "ServerTimeout option was set to %v, but can't be more than 1 hour", d)
		}
		q.requestProperties.Options["servertimeout"] = value.Timespan{Valid: true, Value: d}.Marshal()
		return nil
	}
}

// CustomQueryOption exists to allow a QueryOption that is not defined in the Go SDK, as all options
// are not defined. Please Note: you should always use the type safe options provided below when available.
// Also note that Kusto does not error on non-existent paramater names or bad values, it simply doesn't
// work as expected.
func CustomQueryOption(paramName string, i interface{}) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options[paramName] = i
		return nil
	}
}

// DeferPartialQueryFailures disables reporting partial query failures as part of the result set.
func DeferPartialQueryFailures() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["deferpartialqueryfailures"] = true
		return nil
	}
}

// MaxMemoryConsumptionPerQueryPerNode overrides the default maximum amount of memory a whole query
// may allocate per node.
func MaxMemoryConsumptionPerQueryPerNode(i uint64) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["max_memory_consumption_per_query_per_node"] = i
		return nil
	}
}

// MaxMemoryConsumptionPerIterator overrides the default maximum amount of memory a query operator may allocate.
func MaxMemoryConsumptionPerIterator(i uint64) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["maxmemoryconsumptionperiterator"] = i
		return nil
	}
}

// MaxOutputColumns overrides the default maximum number of columns a query is allowed to produce.
func MaxOutputColumns(i int) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["maxoutputcolumns"] = i
		return nil
	}
}

// PushSelectionThroughAggregation will push simple selection through aggregation .
func PushSelectionThroughAggregation() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["push_selection_through_aggregation"] = true
		return nil
	}
}

// QueryCursorAfterDefault sets the default parameter value of the cursor_after() function when
// called without parameters.
func QueryCursorAfterDefault(s string) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_cursor_after_default"] = s
		return nil
	}
}

// QueryCursorBeforeOrAtDefault sets the default parameter value of the cursor_before_or_at() function when called
// without parameters.
func QueryCursorBeforeOrAtDefault(s string) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_cursor_before_or_at_default"] = s
		return nil
	}
}

// QueryCursorCurrent overrides the cursor value returned by the cursor_current() or current_cursor() functions.
func QueryCursorCurrent(s string) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_cursor_current"] = s
		return nil
	}
}

// QueryCursorDisabled overrides the cursor value returned by the cursor_current() or current_cursor() functions.
func QueryCursorDisabled(s string) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_cursor_disabled"] = s
		return nil
	}
}

// QueryCursorScopedTables is a list of table names that should be scoped to cursor_after_default ..
// cursor_before_or_at_default (upper bound is optional).
func QueryCursorScopedTables(l []string) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_cursor_scoped_tables"] = l
		return nil
	}
}

// DataScope is used with QueryDataScope() to control a query's datascope.
type DataScope interface {
	isDataScope()
}

type dataScope string

func (dataScope) isDataScope() {}

const (
	// DSDefault is used to set a query's datascope to default.
	DSDefault dataScope = "default"
	// DSAll is used to set a query's datascope to all.
	DSAll dataScope = "all"
	// DSHotCache is used to set a query's datascope to hotcache.
	DSHotCache dataScope = "hotcache"
)

// QueryDataScope controls the query's datascope -- whether the query applies to all data or
// just part of it. ['default', 'all', or 'hotcache']
func QueryDataScope(ds DataScope) QueryOption {
	if ds == nil {
		return func(q *queryOptions) error {
			return nil
		}
	}
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_datascope"] = string(ds.(dataScope))
		return nil
	}
}

// QueryDateTimeScopeColumn controls the column name for the query's datetime scope
// (query_datetimescope_to / query_datetimescope_from)
func QueryDateTimeScopeColumn(s string) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_datetimescope_column"] = s
		return nil
	}
}

// QueryDateTimeScopeFrom controls the query's datetime scope (earliest) -- used as auto-applied filter on
// query_datetimescope_column only (if defined).
func QueryDateTimeScopeFrom(t time.Time) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_datetimescope_from"] = t.Format(time.RFC3339Nano)
		return nil
	}
}

// QueryDateTimeScopeTo controls the query's datetime scope (latest) -- used as auto-applied filter on
// query_datetimescope_column only (if defined).
func QueryDateTimeScopeTo(t time.Time) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_datetimescope_to"] = t.Format(time.RFC3339Nano)
		return nil
	}
}

// ClientMaxRedirectCount If set and positive, indicates the maximum number of HTTP redirects that the client will process.
func ClientMaxRedirectCount(i int64) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["client_max_redirect_count"] = i
		return nil
	}
}

// MaterializedViewShuffle A hint to use shuffle strategy for materialized views that are referenced in the query.
// The property is an array of materialized views names and the shuffle keys to use.
// Examples: 'dynamic([ { "Name": "V1", "Keys" : [ "K1", "K2" ] } ])' (shuffle view V1 by K1, K2) or 'dynamic([ { "Name": "V1" } ])' (shuffle view V1 by all keys)
func MaterializedViewShuffle(s string) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["materialized_view_shuffle"] = s
		return nil
	}
}

// QueryBinAutoAt When evaluating the bin_auto() function, the start value to use.
func QueryBinAutoAt(s string) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_bin_auto_at"] = s
		return nil
	}
}

// QueryBinAutoSize When evaluating the bin_auto() function, the bin size value to use.
func QueryBinAutoSize(s string) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_bin_auto_size"] = s
		return nil
	}
}

// QueryDistributionNodesSpan If set, controls the way the subquery merge behaves: the executing node will introduce an additional
// level in the query hierarchy for each subgroup of nodes; the size of the subgroup is set by this option.
func QueryDistributionNodesSpan(i int64) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_distribution_nodes_span"] = i
		return nil
	}
}

// QueryFanoutNodesPercent The percentage of nodes to fan out execution to.
func QueryFanoutNodesPercent(i int) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_fanout_nodes_percent"] = i
		return nil
	}
}

// QueryFanoutThreadsPercent The percentage of threads to fan out execution to.
func QueryFanoutThreadsPercent(i int) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_fanout_threads_percent"] = i
		return nil
	}
}

// QueryForceRowLevelSecurity If specified, forces Row Level Security rules, even if row_level_security policy is disabled
func QueryForceRowLevelSecurity() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_force_row_level_security"] = true
		return nil
	}
}

// QueryLanguage Controls how the query text is to be interpreted (Kql or Sql).
func QueryLanguage(s string) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_language"] = s
		return nil
	}
}

// QueryLogQueryParameters Enables logging of the query parameters, so that they can be viewed later in the .show queries journal.
func QueryLogQueryParameters() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_log_query_parameters"] = true
		return nil
	}
}

// QueryMaxEntitiesInUnion Overrides the default maximum number of entities in a union.
func QueryMaxEntitiesInUnion(i int64) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_max_entities_in_union"] = i
		return nil
	}
}

// QueryNow Overrides the datetime value returned by the now(0s) function.
func QueryNow(t time.Time) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_now"] = t.Format(time.RFC3339Nano)
		return nil
	}
}

// QueryPythonDebug If set, generate python debug query for the enumerated python node (default first).
func QueryPythonDebug(i int) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_python_debug"] = i
		return nil
	}
}

// QueryResultsApplyGetschema If set, retrieves the schema of each tabular data in the results of the query instead of the data itself.
func QueryResultsApplyGetschema() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_results_apply_getschema"] = true
		return nil
	}
}

// QueryResultsCacheMaxAge If positive, controls the maximum age of the cached query results the service is allowed to return
func QueryResultsCacheMaxAge(d time.Duration) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_results_cache_max_age"] = value.Timespan{Value: d, Valid: true}.Marshal()
		return nil
	}
}

// QueryResultsCachePerShard If set, enables per-shard query cache.
func QueryResultsCachePerShard() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_results_cache_per_shard"] = true
		return nil
	}
}

// QueryResultsProgressiveRowCount Hint for Kusto as to how many records to send in each update (takes effect only if OptionResultsProgressiveEnabled is set)
func QueryResultsProgressiveRowCount(i int64) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_results_progressive_row_count"] = i
		return nil
	}
}

// QueryResultsProgressiveUpdatePeriod Hint for Kusto as to how often to send progress frames (takes effect only if OptionResultsProgressiveEnabled is set)
func QueryResultsProgressiveUpdatePeriod(i int32) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_results_progressive_update_period"] = i
		return nil
	}
}

// QueryTakeMaxRecords Enables limiting query results to this number of records.
func QueryTakeMaxRecords(i int64) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["query_take_max_records"] = i
		return nil
	}
}

// QueryConsistency Controls query consistency
func QueryConsistency(c string) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["queryconsistency"] = c
		return nil
	}
}

// RequestAppName Request application name to be used in the reporting (e.g. show queries).
func RequestAppName(s string) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["request_app_name"] = s
		return nil
	}
}

// RequestBlockRowLevelSecurity If specified, blocks access to tables for which row_level_security policy is enabled.
func RequestBlockRowLevelSecurity() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["request_block_row_level_security"] = true
		return nil
	}
}

// RequestCalloutDisabled If specified, indicates that the request can't call-out to a user-provided service.
func RequestCalloutDisabled() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["request_callout_disabled"] = true
		return nil
	}
}

// RequestDescription Arbitrary text that the author of the request wants to include as the request description.
func RequestDescription(s string) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["request_description"] = s
		return nil
	}
}

// RequestExternalTableDisabled If specified, indicates that the request can't invoke code in the ExternalTable.
func RequestExternalTableDisabled() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["request_external_table_disabled"] = true
		return nil
	}
}

// RequestImpersonationDisabled If specified, indicates that the service should not impersonate the caller's identity.
func RequestImpersonationDisabled() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["request_impersonation_disabled"] = true
		return nil
	}
}

// RequestReadonly If specified, indicates that the request can't write anything.
func RequestReadonly() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["request_readonly"] = true
		return nil
	}
}

// RequestRemoteEntitiesDisabled If specified, indicates that the request can't access remote databases and clusters.
func RequestRemoteEntitiesDisabled() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["request_remote_entities_disabled"] = true
		return nil
	}
}

// RequestSandboxedExecutionDisabled If specified, indicates that the request can't invoke code in the sandbox.
func RequestSandboxedExecutionDisabled() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["request_sandboxed_execution_disabled"] = true
		return nil
	}
}

// RequestUser Request user to be used in the reporting (e.g. show queries).
func RequestUser(s string) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["request_user"] = s
		return nil
	}
}

// TruncationMaxRecords Overrides the default maximum number of records a query is allowed to return to the caller (truncation).
func TruncationMaxRecords(i int64) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["truncation_max_records"] = i
		return nil
	}
}

// TruncationMaxSize Overrides the default maximum data size a query is allowed to return to the caller (truncation).
func TruncationMaxSize(i int64) QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["truncation_max_size"] = i
		return nil
	}
}

// ValidatePermissions Validates user's permissions to perform the query and doesn't run the query itself.
func ValidatePermissions() QueryOption {
	return func(q *queryOptions) error {
		q.requestProperties.Options["validate_permissions"] = true
		return nil
	}
}
