// Package types holds Kusto data type representations. All types provide a Value that
// stores the native value and Valid which indicates if the value was set or was null.
package types

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
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
// Decimal represents a Kusto decimal type.  Decimal implements KustoValue.
// Because Go does not have a dynamic decimal type that meets all needs, Decimal
// provides the string representation for you to unmarshal into.
type Decimal struct {
	// Value holds the value of the type.
	Value string
	// Valid indicates if this value was set.
	Valid bool
}

func (Decimal) isKustoVal() {}

// String implements fmt.Stringer.
func (d Decimal) String() string {
	if !d.Valid {
		return "null"
	}
	return d.Value
}

// ParseFloat provides builtin support for Go's *big.Float conversion where that type meets your needs.
func (d *Decimal) ParseFloat(base int, prec uint, mode big.RoundingMode) (f *big.Float, b int, err error) {
	return big.ParseFloat(d.Value, base, prec, mode)
}

var decRE = regexp.MustCompile(`^\d*\.\d+$`)

// Unmarshal unmarshals i into Decimal. i must be a string representing a decimal type or nil.
func (d *Decimal) Unmarshal(i interface{}) error {
	if i == nil {
		return nil
	}

	v, ok := i.(string)
	if !ok {
		return fmt.Errorf("Column with type 'decimal' had type %T", i)
	}

	if !decRE.MatchString(v) {
		return fmt.Errorf("column with type 'decimal' does not appear to be a decimal number, was %v", v)
	}

	d.Value = v
	d.Valid = true
	return nil
}

// Timespan represents a Kusto timespan type.  Timespan implements KustoValue.
type Timespan struct {
	// Value holds the value of the type.
	Value time.Duration
	// Valid indicates if this value was set.
	Valid bool
}

func (Timespan) isKustoVal() {}

// String implements fmt.Stringer.
func (t Timespan) String() string {
	if !t.Valid {
		return "null"
	}
	return t.Value.String()
}

// Unmarshal unmarshals i into Timespan. i must be a string representing a Kusto timespan or nil.
func (t *Timespan) Unmarshal(i interface{}) error {
	const (
		hoursIndex   = 0
		minutesIndex = 1
		secondsIndex = 2
	)

	if i == nil {
		return nil
	}

	v, ok := i.(string)
	if !ok {
		return fmt.Errorf("Column with type 'timespan' had type %T", i)
	}

	negative := false
	if len(v) > 1 {
		if string(v[0]) == "-" {
			negative = true
			v = v[1:]
		}
	}

	sp := strings.Split(v, ":")
	if len(sp) != 3 {
		return fmt.Errorf("value to unmarshal into Timespan does not seem to fit format '00:00:00', where values are decimal(%s)", v)
	}

	var sum time.Duration

	d, err := t.unmarshalDaysHours(sp[hoursIndex])
	if err != nil {
		return err
	}
	sum += d

	d, err = t.unmarshalMinutes(sp[minutesIndex])
	if err != nil {
		return err
	}
	sum += d

	d, err = t.unmarshalSeconds(sp[secondsIndex])
	if err != nil {
		return err
	}

	sum += d

	if negative {
		sum = sum * time.Duration(-1)
	}

	t.Value = sum
	t.Valid = true
	return nil
}

var day = 24 * time.Hour

func (t *Timespan) unmarshalDaysHours(s string) (time.Duration, error) {
	sp := strings.Split(s, ".")
	switch len(sp) {
	case 1:
		hours, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("timespan's hours/day field was incorrect, was %s: %s", s, err)
		}
		return time.Duration(hours) * time.Hour, nil
	case 2:
		days, err := strconv.Atoi(sp[0])
		if err != nil {
			return 0, fmt.Errorf("timespan's hours/day field was incorrect, was %s", s)
		}
		hours, err := strconv.Atoi(sp[1])
		if err != nil {
			return 0, fmt.Errorf("timespan's hours/day field was incorrect, was %s", s)
		}
		return time.Duration(days)*day + time.Duration(hours)*time.Hour, nil
	}
	return 0, fmt.Errorf("timespan's hours/days field did not have the requisite '.'s, was %s", s)
}

func (t *Timespan) unmarshalMinutes(s string) (time.Duration, error) {
	s = strings.Split(s, ".")[0] // We can have 01 or 01.00 or 59, but nothing comes behind the .

	minutes, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("timespan's minutes field was incorrect, was %s", s)
	}
	if minutes < 0 || minutes > 59 {
		return 0, fmt.Errorf("timespan's minutes field was incorrect, was %s", s)
	}
	return time.Duration(minutes) * time.Minute, nil
}

const tick = 100 * time.Nanosecond

// unmarshalSeconds deals with this crazy output format. Instead of having some multiplier, the number
// of precision characters behind the decimal indicates your multiplier. This can be between 0 and 7, but
// really only has 3, 4 and 7. There is something called a tick, which is 100 Nanoseconds and the precision
// at len 4 is 100 * Microsecond (don't know if that has a name).
func (t *Timespan) unmarshalSeconds(s string) (time.Duration, error) {
	// "03" = 3 * time.Second
	// "00.099" = 99 * time.Millisecond
	// "03.0123" == 3 * time.Second + 12300 * time.Microsecond
	sp := strings.Split(s, ".")
	switch len(sp) {
	case 1:
		seconds, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("timespan's seconds field was incorrect, was %s", s)
		}
		return time.Duration(seconds) * time.Second, nil
	case 2:
		seconds, err := strconv.Atoi(sp[0])
		if err != nil {
			return 0, fmt.Errorf("timespan's seconds field was incorrect, was %s", s)
		}
		n, err := strconv.Atoi(sp[1])
		if err != nil {
			return 0, fmt.Errorf("timespan's seconds field was incorrect, was %s", s)
		}
		var prec time.Duration
		switch len(sp[1]) {
		case 1:
			prec = time.Duration(n) * (100 * time.Millisecond)
		case 2:
			prec = time.Duration(n) * (10 * time.Millisecond)
		case 3:
			prec = time.Duration(n) * time.Millisecond
		case 4:
			prec = time.Duration(n) * 100 * time.Microsecond
		case 5:
			prec = time.Duration(n) * 10 * time.Microsecond
		case 6:
			prec = time.Duration(n) * time.Microsecond
		case 7:
			prec = time.Duration(n) * tick
		case 8:
			prec = time.Duration(n) * (10 * time.Nanosecond)
		case 9:
			prec = time.Duration(n) * time.Nanosecond
		default:
			return 0, fmt.Errorf("timespan's seconds field did not have 1-9 numbers after the decimal, had %v", s)
		}

		return time.Duration(seconds)*time.Second + prec, nil
	}
	return 0, fmt.Errorf("timespan's seconds field did not have the requisite '.'s, was %s", s)
}
