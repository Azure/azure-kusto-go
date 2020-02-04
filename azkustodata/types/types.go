// Package types holds Kusto data type representations. All types provide a Value that
// stores the native value and Valid which indicates if the value was set or was null.
// TODO: This package is missing the Decimal an Timespan types.
package types

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// KustoValue represents a Kusto value.
type KustoValue interface {
	isKustoVal()
}

// KustoValues is a list of KustoValue, usually an ordered row.
type KustoValues []KustoValue

// Bool represents a Kusto boolean type. Bool implements KustoValue.
type Bool struct {
	// Value holds the value of the type.
	Value bool
	// Valid indicates if this value was set.
	Valid bool
}

func (Bool) isKustoVal() {}

// String implements fmt.Stringer.
func (b Bool) String() string {
	if !b.Valid {
		return "null"
	}
	if b.Value {
		return "true"
	}
	return "false"
}

// Unmarshal unmarshals i into Bool. i must be a bool or nil.
func (b *Bool) Unmarshal(i interface{}) error {
	if i == nil {
		return nil
	}
	v, ok := i.(bool)
	if !ok {
		return fmt.Errorf("Column with type 'bool' had value that was %T", i)
	}
	b.Value = v
	b.Valid = true
	return nil
}

// DateTime represents a Kusto datetime type.  DateTime implements KustoValue.
type DateTime struct {
	// Value holds the value of the type.
	Value time.Time
	// Valid indicates if this value was set.
	Valid bool
}

// String implements fmt.Stringer.
func (d DateTime) String() string {
	if !d.Valid {
		return "null"
	}
	return fmt.Sprint(d.Value)
}

func (DateTime) isKustoVal() {}

// Unmarshal unmarshals i into DateTime. i must be a string representing RFC3339Nano or nil.
func (d *DateTime) Unmarshal(i interface{}) error {
	if i == nil {
		return nil
	}

	str, ok := i.(string)
	if !ok {
		return fmt.Errorf("Column with type 'datetime' had value that was %T", i)
	}

	t, err := time.Parse(time.RFC3339Nano, str)
	if err != nil {
		return fmt.Errorf("Column with type 'datetime' had value %s which did not parse: %s", str, err)
	}
	d.Value = t
	d.Valid = true

	return nil
}

// Dynamic represents a Kusto dynamic type.  Dynamic implements KustoValue.
type Dynamic struct {
	// Value holds the value of the type.
	Value string
	// Valid indicates if this value was set.
	Valid bool
}

func (Dynamic) isKustoVal() {}

// String implements fmt.Stringer.
func (d Dynamic) String() string {
	if !d.Valid {
		return "null"
	}
	return d.Value
}

// Unmarshal unmarshal's i into Dynamic. i must be a string or nil.
func (d *Dynamic) Unmarshal(i interface{}) error {
	if i == nil {
		return nil
	}

	// TODO: temporary fix.
	//  Because unmarshalling happens
	if v, ok := i.(map[string]interface{}); ok {
		b, _ := json.Marshal(v)
		d.Value = string(b)
		d.Valid = true

		return nil
	}

	v, ok := i.(string)
	if !ok {
		return fmt.Errorf("Column with type 'dynamic' was not stored as a string, was %T", i)
	}

	m := map[string]interface{}{}
	if err := json.Unmarshal([]byte(v), &m); err != nil {
		return fmt.Errorf("Column with type 'dynamic' is storing string that cannot be JSON unmarshalled: %s", err)
	}
	d.Value = v
	d.Valid = true
	return nil
}

// GUID represents a Kusto GUID type.  GUID implements KustoValue.
type GUID struct {
	// Value holds the value of the type.
	Value uuid.UUID
	// Valid indicates if this value was set.
	Valid bool
}

func (GUID) isKustoVal() {}

// String implements fmt.Stringer.
func (g GUID) String() string {
	if !g.Valid {
		return "null"
	}
	return g.Value.String()
}

// Unmarshal unmarshals i into GUID. i must be a string representing a GUID or nil.
func (g *GUID) Unmarshal(i interface{}) error {
	if i == nil {
		return nil
	}
	str, ok := i.(string)
	if !ok {
		return fmt.Errorf("Column with type 'guid' was not stored as a string, was %T", i)
	}
	u, err := uuid.Parse(str)
	if err != nil {
		return fmt.Errorf("Column with type 'guid' did not store a valid uuid(%s): %s", str, err)
	}
	g.Value = u
	g.Valid = true
	return nil
}

// Int represents a Kuston int type. Kusto int type's are int32 values.  Int implements KustoValue.
type Int struct {
	// Value holds the value of the type.
	Value int32
	// Valid indicates if this value was set.
	Valid bool
}

func (Int) isKustoVal() {}

// String implements fmt.Stringer.
func (in Int) String() string {
	if !in.Valid {
		return "null"
	}
	return strconv.Itoa(int(in.Value))
}

// Unmarshal unmarshals i into Int. i must be an int32 or nil.
func (in *Int) Unmarshal(i interface{}) error {
	if i == nil {
		return nil
	}

	var myInt int64

	switch v := i.(type) {
	case json.Number:
		var err error
		myInt, err = v.Int64()
		if err != nil {
			return fmt.Errorf("Column with type 'int' had value json.Number that had error on .Int64(): %s", err)
		}
	case int:
		myInt = int64(v)
	default:
		return fmt.Errorf("Column with type 'int' had value that was not a json.Number or int, was %T", i)
	}

	if myInt > math.MaxInt32 {
		return fmt.Errorf("Column with type 'int' had value that was greater than an int32 can hold, was %d", myInt)
	}
	in.Value = int32(myInt)
	in.Valid = true
	return nil
}

// Long represents a Kuston long type, which is an int64.  Long implements KustoValue.
type Long struct {
	// Value holds the value of the type.
	Value int64
	// Valid indicates if this value was set.
	Valid bool
}

func (Long) isKustoVal() {}

// String implements fmt.Stringer.
func (l Long) String() string {
	if !l.Valid {
		return "null"
	}
	return strconv.Itoa(int(l.Value))
}

// Unmarshal unmarshals i into Long. i must be an int64 or nil.
func (l *Long) Unmarshal(i interface{}) error {
	if i == nil {
		return nil
	}

	var myInt int64

	switch v := i.(type) {
	case json.Number:
		var err error
		myInt, err = v.Int64()
		if err != nil {
			return fmt.Errorf("Column with type 'long' had value json.Number that had error on .Int64(): %s", err)
		}
	case int:
		myInt = int64(v)
	default:
		return fmt.Errorf("Column with type 'ong' had value that was not a json.Number or int, was %T", i)
	}

	l.Value = myInt
	l.Valid = true
	return nil
}

// Real represents a Kusto real type.  Real implements KustoValue.
type Real struct {
	// Value holds the value of the type.
	Value float64
	// Valid indicates if this value was set.
	Valid bool
}

func (Real) isKustoVal() {}

// String implements fmt.Stringer.
func (r Real) String() string {
	if !r.Valid {
		return "null"
	}
	return strconv.FormatFloat(r.Value, 'e', -1, 64)
}

// Unmarshal unmarshals i into Real. i must be a json.Number(that is a float64), float64 or nil.
func (r *Real) Unmarshal(i interface{}) error {
	if i == nil {
		return nil
	}

	var myFloat float64

	switch v := i.(type) {
	case json.Number:
		var err error
		myFloat, err = v.Float64()
		if err != nil {
			return fmt.Errorf("Column with type 'real' had value json.Number that had error on .Float64(): %s", err)
		}
	case float64:
		myFloat = v
	default:
		return fmt.Errorf("Column with type 'real' had value that was not a json.Number or float64, was %T", i)
	}

	r.Value = myFloat
	r.Valid = true
	return nil
}

// String represents a Kusto string type.  String implements KustoValue.
type String struct {
	// Value holds the value of the type.
	Value string
	// Valid indicates if this value was set.
	Valid bool
}

func (String) isKustoVal() {}

// String implements fmt.Stringer.
func (s String) String() string {
	if !s.Valid {
		return "null"
	}
	return s.Value
}

// Unmarshal unmarshals i into String. i must be a string or nil.
func (s *String) Unmarshal(i interface{}) error {
	if i == nil {
		return nil
	}

	v, ok := i.(string)
	if !ok {
		return fmt.Errorf("Column with type 'string' had type %T", i)
	}

	s.Value = v
	s.Valid = true
	return nil
}
