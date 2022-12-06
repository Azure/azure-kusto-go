package kusto

/*
This file defines our Stmt, Definitions and Parameters types, which are used in Query() to query Kusto.
These provide injection safe querying for data retrieval and insertion.
*/

import (
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-kusto-go/kusto/data/value"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/Azure/azure-kusto-go/kusto/data/types"
	ilog "github.com/Azure/azure-kusto-go/kusto/internal/log"
	"github.com/Azure/azure-kusto-go/kusto/unsafe"

	"github.com/google/uuid"
)

// stringConstant is an internal type that cannot be created outside the package.  The only two ways to build
// a stringConstant is to pass a string constant or use a local function to build the stringConstant.
// This allows us to enforce the use of constants or strings built with injection protection.
type stringConstant string

// String implements fmt.Stringer.
func (s stringConstant) String() string {
	return string(s)
}

// ParamTypes is a list of parameter types and corresponding type data.
type ParamTypes map[string]ParamType

func (p ParamTypes) clone() ParamTypes {
	c := make(ParamTypes, len(p))
	for k, v := range p {
		c[k] = v
	}
	return c
}

// ParamType provides type and default value information about the query parameter
type ParamType struct {
	// Type is the type of Column type this QueryParam will represent.
	Type types.Column
	// Default is a default value to use if the query doesn't provide this value.
	// The value that can be set is defined by the Type:
	// CTBool must be a bool
	// CTDateTime must be a time.Time
	// CTDynamic cannot have a default value
	// CTGuid must be an uuid.UUID
	// CTInt must be an int32
	// CTLong must be an int64
	// CTReal must be an float64
	// CTString must be a string
	// CTTimespan must be a time.Duration
	// CTDecimal must be a string or *big.Float representing a decimal value
	Default interface{}

	name string
}

// isValid validates whether the value is of known kusto Types.
func isValid(kustoType types.Column, val interface{}) bool {
	if !kustoType.Valid() {
		return false
	}

	switch kustoType {
	case types.Bool:
		_, ok := val.(bool)
		return ok
	case types.DateTime:
		_, ok := val.(time.Time)
		return ok
	case types.Dynamic:
		return false
	case types.GUID:
		_, ok := val.(uuid.UUID)
		return ok
	case types.Int:
		switch val.(type) {
		case int32:
			return true
		case int:
			return true
		default:
			return false
		}
	case types.Long:
		_, ok := val.(int64)
		return ok
	case types.Real:
		_, ok := val.(float64)
		return ok
	case types.String:
		_, ok := val.(string)
		return ok
	case types.Timespan:
		_, ok := val.(time.Duration)
		return ok
	case types.Decimal:
		switch val.(type) {
		case string:
			if value.DecRE.MatchString(val.(string)) {
				return true
			}
			return false
		case float32:
			return true
		case float64:
			return true
		case *big.Float:
			return true
		case *big.Int:
			return true
		default:
			return false
		}
	}
	return false
}

// validate validates that Parameters is valid .
func (p ParamType) validate() error {
	if !p.Type.Valid() {
		return fmt.Errorf("the .Type was not a valid value, must be one of the values in this package starting with CT<type name>, was %s", p.Type)
	}
	if p.Default == nil {
		return nil
	}

	if !isValid(p.Type, p.Default) {
		return fmt.Errorf("received a field type %q we don't recognize", p.Type)
	}
	return nil
}

// convertTypeToKustoString Converts a valid kustoType val to a string
func convertTypeToKustoString(kustoType types.Column, val interface{}) (string, error) {

	if !isValid(kustoType, val) {
		return "", fmt.Errorf("received a field type %q we don't recognize", kustoType)
	}

	switch kustoType {
	case types.String:
		return AddQuotedString(fmt.Sprintf("%v", val), false), nil
	case types.DateTime:
		date := val.(time.Time)
		return fmt.Sprintf("datetime(%v)", date.Format(time.RFC3339Nano)), nil
	case types.Int:
		return fmt.Sprintf("int(%d)", val), nil
	case types.Bool:
		return fmt.Sprintf("bool(%t)", val), nil
	case types.Dynamic:
		b, err := json.Marshal(val)
		if err != nil {
			return "", fmt.Errorf("(dynamic) %T could not be marshalled into JSON, err: %s", val, err)
		}
		return fmt.Sprintf("dynamic(%s)", string(b)), nil
	case types.GUID:
		u, err := val.(uuid.UUID)
		if !err {
			return "", fmt.Errorf("%T which is not a uuid.UUID", val)
		}
		return fmt.Sprintf("guid(%s)", u.String()), nil
	case types.Long:
		return fmt.Sprintf("long(%d)", val), nil
	case types.Real:
		return fmt.Sprintf("real(%f)", val), nil
	case types.Timespan:
		d, err := val.(time.Duration)
		if !err {
			return "", fmt.Errorf("%T, which is not a time.Duration", val)
		}
		return fmt.Sprintf("timespan(%s)", value.Timespan{Value: d, Valid: true}.Marshal()), nil
	case types.Decimal:
		switch val.(type) {
		case string:
			return fmt.Sprintf("decimal(%s)", val), nil
		case float32:
			return fmt.Sprintf("decimal(%f)", val), nil
		case float64:
			return fmt.Sprintf("decimal(%f)", val), nil
		case *big.Float:
			return fmt.Sprintf("decimal(%s)", val.(*big.Float).String()), nil
		case *big.Int:
			return fmt.Sprintf("decimal(%s)", val.(*big.Int).String()), nil
		default:
			return "", fmt.Errorf("received a field type %q that we do not handle", kustoType)
		}
	}
	return "", fmt.Errorf("received a field type %q we don't recognize", kustoType)
}

func (p ParamType) string() string {
	if p.Default == nil {
		return fmt.Sprintf("%v:%v", p.name, p.Type)
	}
	kustoString, _ := convertTypeToKustoString(p.Type, p.Default)
	return fmt.Sprintf("%v:%v=%v", p.name, p.Type, kustoString)
}

// Definitions represents definitions of parameters that are substituted for variables in
// a Kusto Query. This provides both variable substitution in a Stmt and provides protection against
// SQL-like injection attacks.
// See https://docs.microsoft.com/en-us/azure/kusto/query/queryparametersstatement?pivots=azuredataexplorer
// for internals. This object is not thread-safe and passing it as an argument to a function will create a
// copy that will share the internal state with the original.
type Definitions struct {
	m ParamTypes
}

// NewDefinitions is the constructor for Definitions.
func NewDefinitions() Definitions {
	return Definitions{}
}

// IsZero indicates if the Definitions object is the zero type.
func (p Definitions) IsZero() bool {
	if p.m == nil || len(p.m) == 0 {
		return true
	}
	return false
}

// With returns a copy of the Definitions object with the parameters names and types defined in "types".
func (p Definitions) With(types ParamTypes) (Definitions, error) {
	for name, param := range types {
		if strings.Contains(name, " ") {
			return p, fmt.Errorf("name %q cannot contain spaces", name)
		}
		if err := param.validate(); err != nil {
			return p, fmt.Errorf("parameter %q could not be added: %s", name, err)
		}
	}
	p.m = types
	return p, nil
}

// Must is the same as With(), but it must succeed or it panics.
func (p Definitions) Must(types ParamTypes) Definitions {
	var err error
	p, err = p.With(types)
	if err != nil {
		panic(err)
	}
	return p
}

var buildPool = sync.Pool{
	New: func() interface{} {
		return new(strings.Builder)
	},
}

// String implements fmt.Stringer.
func (p Definitions) String() string {
	const (
		declare   = "declare query_parameters("
		closeStmt = ");"
	)

	if len(p.m) == 0 {
		return ""
	}

	params := make([]ParamType, 0, len(p.m))

	for k, v := range p.m {
		v.name = k
		params = append(params, v)
	}

	sort.Slice(params, func(i, j int) bool { return params[i].name < params[j].name })

	build := buildPool.Get().(*strings.Builder)
	build.Reset()
	defer buildPool.Put(build)

	build.WriteString(declare)
	/*
		declare query_parameters ( Name1 : Type1 [= DefaultValue1] [,...] );
		declare query_parameters(UserName:string, Password:string)
	*/
	for i, param := range params {
		build.WriteString(param.string())
		if i+1 < len(params) {
			build.WriteString(", ")
		}
	}
	build.WriteString(closeStmt)
	return build.String()
}

// clone returns a clone of Definitions.
func (p Definitions) clone() Definitions {
	p.m = p.m.clone()
	return p
}

// QueryValues represents a set of values that are substituted in Parameters. Every QueryValue key
// must have a corresponding Parameter name. All values must be compatible with the Kusto Column type
// it will go into (int64 for a long, int32 for int, time.Time for datetime, ...)
type QueryValues map[string]interface{}

func (v QueryValues) clone() QueryValues {
	c := make(QueryValues, len(v))
	for k, v := range v {
		c[k] = v
	}
	return c
}

// Parameters represents values that will be substituted for a Stmt's Parameter. Keys are the names
// of corresponding Parameters, values are the value to be used. Keys must exist in the Parameter
// and value must be a Go type that corresponds to the ParamType.
type Parameters struct {
	m    QueryValues
	outM map[string]string // This is string keys and Kusto string query parameter values
}

// NewParameters is the construtor for Parameters.
func NewParameters() Parameters {
	return Parameters{
		m:    map[string]interface{}{},
		outM: map[string]string{},
	}
}

// IsZero returns if Parameters is the zero value.
func (q Parameters) IsZero() bool {
	return len(q.m) == 0
}

// With returns a Parameters set to "values". values' keys represents Definitions names
// that will substituted for and the values to be subsituted.
func (q Parameters) With(values QueryValues) (Parameters, error) {
	q.m = values
	return q, nil
}

// Must is the same as With() except any error is a panic.
func (q Parameters) Must(values QueryValues) Parameters {
	var err error
	q, err = q.With(values)
	if err != nil {
		panic(err)
	}
	return q
}

func (q Parameters) clone() Parameters {
	c := Parameters{
		m:    q.m.clone(),
		outM: make(map[string]string, len(q.outM)),
	}

	for k, v := range q.outM {
		c.outM[k] = v
	}

	return c
}

// toParameters creates a map[string]interface{} that is ready for JSON encoding to a REST query
// requests properties.Parameters. While output is a map[string]interface{}, this is not the same as
// Parameters itself (the values are converted to the appropriate string output).
func (q Parameters) toParameters(p Definitions) (map[string]string, error) {
	if q.outM == nil {
		return q.outM, nil
	}

	var err error
	q, err = q.validate(p)
	if err != nil {
		return nil, err
	}
	return q.outM, nil
}

// validate validates that Parameters is valid and has associated keys/types in Definitions.
// It returns a copy of Parameters that has out output map created using the values.
func (q Parameters) validate(p Definitions) (Parameters, error) {
	out := make(map[string]string, len(q.m))

	for k, v := range q.m {
		str, err := convertTypeToKustoString(p.m[k].Type, v)
		if err != nil {
			return q, err
		}
		out[k] = str
	}

	q.outM = out
	return q, nil
}

// Stmt is a Kusto Query statement. A Stmt is thread-safe, but methods on the Stmt are not.
// All methods on a Stmt do not alter the statement, they return a new Stmt object with the changes.
// This includes a copy of the Definitions and Parameters objects, if provided.  This allows a
// root Stmt object that can be built upon. You should not pass *Stmt objects.
type Stmt struct {
	queryStr string
	defs     Definitions
	params   Parameters
	unsafe   unsafe.Stmt
}

// StmtOption is an optional argument to NewStmt().
type StmtOption func(s *Stmt)

// UnsafeStmt enables unsafe actions on a Stmt and all Stmts derived from that Stmt.
// This turns off safety features that could allow a service client to compromise your data store.
// USE AT YOUR OWN RISK!
func UnsafeStmt(options unsafe.Stmt) StmtOption {
	return func(s *Stmt) {
		ilog.UnsafeWarning(options.SuppressWarning)
		s.unsafe.Add = true
	}
}

// NewStmt creates a Stmt from a string constant.
func NewStmt(query stringConstant, options ...StmtOption) Stmt {
	s := Stmt{queryStr: query.String()}
	for _, option := range options {
		option(&s)
	}
	return s
}

// AddDatabase - Given a variable with a value 'MyDatabase', AddDatabase(arg) will produce the following string
// '["MyDatabase"]' and will append the following string to Stmt
func (s Stmt) AddDatabase(query string) Stmt {
	return s.NormalizeName(query, false)
}

// AddTable - Given a variable with a value 'MyTable', AddTable(arg) will produce the following string
// '["MyTable"]' and will append the following string to Stmt
func (s Stmt) AddTable(query string) Stmt {
	return s.NormalizeName(query, false)
}

// AddColumn - Given a variable with a value 'MyColumn', AddColumn(arg) will produce the following string
// '["MyColumn"]' and will append the following string to Stmt
func (s Stmt) AddColumn(query string) Stmt {
	return s.NormalizeName(query, false)
}

// AddFunction - Given a variable with a value 'MyFunction', AddFunction(arg) will produce the following string
// '["MyFunction"]' and will append the following string to Stmt
func (s Stmt) AddFunction(query string) Stmt {
	return s.NormalizeName(query, false)
}

// NormalizeName normalizes a string in order to be used safely in the engine - given "query" will produce [\"query\"].
func (s Stmt) NormalizeName(query string, forceNormalization bool) Stmt {
	if query == "" {
		return s
	}
	if !forceNormalization && !RequiresQuoting(query) {
		s.queryStr = s.queryStr + query
		return s
	}

	s.queryStr = s.queryStr + "[" + AddQuotedString(query, false) + "]"
	return s
}

// RequiresQuoting checks whether a given string is an identifier
func RequiresQuoting(query string) bool {
	if query == "" {
		return true
	}
	if !unicode.IsLetter(rune(query[0])) && rune(query[0]) != '_' {
		return true
	}
	for _, c := range query {
		if !(unicode.IsLetter(c) || unicode.IsDigit(c) || c == '_') {
			return true
		}
	}
	return false
}

// AddQuotedString escapes a string to be safely added to a stmt
func AddQuotedString(value string, hidden bool) string {
	if value == "" {
		return value
	}

	var literal strings.Builder

	if hidden {
		literal.WriteString("h")
	}
	literal.WriteString("\"")
	for _, c := range value {
		switch c {
		case '\'':
			literal.WriteString("\\'")

		case '"':
			literal.WriteString("\\\"")

		case '\\':
			literal.WriteString("\\\\")

		case '\x00':
			literal.WriteString("\\0")

		case '\a':
			literal.WriteString("\\a")

		case '\b':
			literal.WriteString("\\b")

		case '\f':
			literal.WriteString("\\f")

		case '\n':
			literal.WriteString("\\n")

		case '\r':
			literal.WriteString("\\r")

		case '\t':
			literal.WriteString("\\t")

		case '\v':
			literal.WriteString("\\v")

		default:
			if !ShouldBeEscaped(c) {
				literal.WriteString(string(c))
			} else {
				literal.WriteString(fmt.Sprintf("\\u%04x", c))
			}

		}
	}
	literal.WriteString("\"")

	return literal.String()
}

// ShouldBeEscaped Checks whether a rune should be escaped or not based on it's type.
func ShouldBeEscaped(c int32) bool {
	if c <= unicode.MaxLatin1 {
		return unicode.IsControl(c)
	}
	return true
}

// AddInt will add an int as a string to the Stmt.  This allows dynamically building of a query from a root Stmt.
func (s Stmt) AddInt(query int) Stmt {
	return s.addBase(types.Int, query)
}

// AddFloat32 will add a Float32 as a string to the Stmt.  This allows dynamically building of a query from a root Stmt.
func (s Stmt) AddFloat32(query float32) Stmt {
	return s.addBase(types.Decimal, query)
}

// AddFloat64 will add a Float64 as a string to the Stmt.  This allows dynamically building of a query from a root Stmt.
func (s Stmt) AddFloat64(query float64) Stmt {
	return s.addBase(types.Decimal, query)
}

// AddBool will add a bool as a string to the Stmt.  This allows dynamically building of a query from a root Stmt.
func (s Stmt) AddBool(query bool) Stmt {
	return s.addBase(types.Bool, query)
}

// AddByte will add a byte as a string to the Stmt.  This allows dynamically building of a query from a root Stmt.
func (s Stmt) AddByte(query byte) Stmt {
	return s.addBase(types.Int, query)
}

// AddDate will add a date as a string to the Stmt.  This allows dynamically building of a query from a root Stmt.
func (s Stmt) AddDate(query time.Time) Stmt {
	return s.addBase(types.DateTime, query)
}

// addBase will add a query of some kustoType as a string to the Stmt.  This allows dynamically building of a query from a root Stmt.
func (s Stmt) addBase(kustoType types.Column, query interface{}) Stmt {
	kustoString, _ := convertTypeToKustoString(kustoType, query)
	s.queryStr = s.queryStr + kustoString
	return s
}

// Add will add more text to the Stmt. This is similar to the + operator on two strings, except
// it only can be done with string constants. This allows dynamically building of a query from a root
// Stmt.
func (s Stmt) Add(query stringConstant) Stmt {
	s.queryStr = s.queryStr + query.String()
	return s
}

// UnsafeAdd provides a method to add strings that are not injection protected to the Stmt.
// To utilize this method, you must create the Stmt with the UnsafeStmt() option and pass
// the unsafe.Stmt with .Add set to true. If not set, THIS WILL PANIC!
func (s Stmt) UnsafeAdd(query string) Stmt {
	if !s.unsafe.Add {
		panic("Stmt.UnsafeAdd() called, but the unsafe.Stmt.Add ability has not been enabled")
	}

	s.queryStr = s.queryStr + query
	return s
}

// WithDefinitions will return a Stmt that can be used in a Query() with Kusto
// Parameters to protect against SQL-like injection attacks. These Parameters must align with
// the placeholders in the statement. The new Stmt object will have a copy of the Parameters passed,
// not the original.
func (s Stmt) WithDefinitions(defs Definitions) (Stmt, error) {
	if len(defs.m) == 0 {
		return s, fmt.Errorf("cannot pass Definitions that are empty")
	}
	s.defs = defs.clone()

	return s, nil
}

// MustDefinitions is the same as WithDefinitions with the exceptions that an error causes a panic.
func (s Stmt) MustDefinitions(defs Definitions) Stmt {
	s, err := s.WithDefinitions(defs)
	if err != nil {
		panic(err)
	}

	return s
}

// WithParameters returns a Stmt that has the Parameters that will be substituted for
// Definitions in the query.  Must have supplied the appropriate Definitions using WithQueryParamaters().
func (s Stmt) WithParameters(params Parameters) (Stmt, error) {
	if s.defs.IsZero() {
		return s, fmt.Errorf("cannot call WithParameters() if WithDefinitions hasn't been called")
	}
	params = params.clone()
	var err error

	params, err = params.validate(s.defs)
	if err != nil {
		return s, err
	}

	s.params = params
	return s, nil
}

// MustParameters is the same as WithParameters with the exceptions that an error causes a panic.
func (s Stmt) MustParameters(params Parameters) Stmt {
	stmt, err := s.WithParameters(params)
	if err != nil {
		panic(err)
	}
	return stmt
}

// String implements fmt.Stringer. This can be used to see what the query statement to the server will be
// for debugging purposes.
func (s Stmt) String() string {
	build := buildPool.Get().(*strings.Builder)
	build.Reset()
	defer buildPool.Put(build)

	if len(s.defs.m) > 0 {
		build.WriteString(s.defs.String() + "\n")
	}
	build.WriteString(s.queryStr)
	return build.String()
}

// ValuesJSON returns a string in JSON format representing the Kusto QueryOptions.Parameters value
// that will be passed to the server. These values are substitued for Definitions in the Stmt and
// are represented by the Parameters that was passed.
func (s Stmt) ValuesJSON() (string, error) {
	m, err := s.params.toParameters(s.defs)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
