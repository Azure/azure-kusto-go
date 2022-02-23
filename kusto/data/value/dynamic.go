package value

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// Dynamic represents a Kusto dynamic type.  Dynamic implements Kusto.
type Dynamic struct {
	// Value holds the value of the type.
	Value []byte
	// Valid indicates if this value was set.
	Valid bool
}

func (Dynamic) isKustoVal() {}

// String implements fmt.Stringer.
func (d Dynamic) String() string {
	if !d.Valid {
		return ""
	}

	return string(d.Value)
}

// Unmarshal unmarshal's i into Dynamic. i must be a string, []byte, map[string]interface{}, []interface{}, other JSON serializable value or nil.
// If []byte or string, must be a JSON representation of a value.
func (d *Dynamic) Unmarshal(i interface{}) error {
	if i == nil {
		d.Value = nil
		d.Valid = false
		return nil
	}

	switch v := i.(type) {
	case []byte:
		d.Value = v
		d.Valid = true
		return nil
	case string:
		d.Value = []byte(v)
		d.Valid = true
		return nil
	}

	b, err := json.Marshal(i)
	if err != nil {
		return fmt.Errorf("Column with type 'dynamic' was a %T that could not be JSON encoded: %s", i, err)
	}

	d.Value = b
	d.Valid = true
	return nil
}

// Convert Dynamic into reflect value.
func (d Dynamic) Convert(v reflect.Value) error {
	t := v.Type()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	var valueToSet reflect.Value
	switch {
	case t.ConvertibleTo(reflect.TypeOf(Dynamic{})):
		valueToSet = reflect.ValueOf(d)
	case t.ConvertibleTo(reflect.TypeOf([]byte{})):
		if t.Kind() == reflect.String {
			s := string(d.Value)
			valueToSet = reflect.ValueOf(s)
		} else {
			valueToSet = reflect.ValueOf(d.Value)
		}
	case t.Kind() == reflect.Slice:
		if !d.Valid {
			return nil
		}
		if t.Elem().Kind() != reflect.Interface {
			return fmt.Errorf("The slice for storing Dynamic must be of type []interface{}")
		}

		s := []interface{}{}
		if err := json.Unmarshal([]byte(d.Value), &s); err != nil {
			return fmt.Errorf("Error occurred while trying to unmarshal Dynamic into a []interface{}: %s", err)
		}

		valueToSet = reflect.ValueOf(s)
	case t.Kind() == reflect.Map:
		if !d.Valid {
			return nil
		}
		if t.Key().Kind() != reflect.String {
			return fmt.Errorf("Type dymanic and can only be stored in a string, *string, map[string]interface{}, *map[string]interface{}, struct or *struct")
		}
		if t.Elem().Kind() != reflect.Interface {
			return fmt.Errorf("Type dymanic and can only be stored in a string, *string, map[string]interface{}, *map[string]interface{}, struct or *struct")
		}

		m := map[string]interface{}{}
		if err := json.Unmarshal([]byte(d.Value), &m); err != nil {
			return fmt.Errorf("Error occurred while trying to marshal type dynamic into a map[string]interface{}: %s", err)
		}

		valueToSet = reflect.ValueOf(m)
	case t.Kind() == reflect.Struct:
		structPtr := reflect.New(t)

		if err := json.Unmarshal([]byte(d.Value), structPtr.Interface()); err != nil {
			return fmt.Errorf("Could not unmarshal type dynamic into receiver: %s", err)
		}

		valueToSet = structPtr.Elem()
	default:
		return fmt.Errorf("Column was type Kusto.Dynamic, receiver had base Kind %s ", t.Kind())
	}

	if v.Type().Kind() != reflect.Ptr {
		v.Set(valueToSet)
	} else {
		ptr := reflect.New(valueToSet.Type())
		ptr.Elem().Set(valueToSet)
		v.Set(ptr)
	}
	return nil
}
