package value

import (
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"reflect"
)

// Dynamic represents a Kusto dynamic type.  Dynamic implements Kusto.
type Dynamic struct {
	pointerValue[[]byte]
}

// NewDynamic creates a new Dynamic.
func NewDynamic(v []byte) *Dynamic { return &Dynamic{newPointerValue[[]byte](&v)} }

// NewNullDynamic creates a new null Dynamic.
func NewNullDynamic() *Dynamic { return &Dynamic{newPointerValue[[]byte](nil)} }

func DynamicFromInterface(v interface{}) *Dynamic {
	marshal, err := json.Marshal(v)
	if err != nil {
		return NewNullDynamic()
	}

	return NewDynamic(marshal)
}

func (*Dynamic) isKustoVal() {}

// Unmarshal unmarshal's i into Dynamic. i must be a string, []byte, map[string]interface{}, []interface{}, other JSON serializable value or nil.
// If []byte or string, must be a JSON representation of a value.
func (d *Dynamic) Unmarshal(i interface{}) error {
	if i == nil {
		d.value = nil
		return nil
	}

	switch v := i.(type) {
	case []byte:
		d.value = &v
		return nil
	case string:
		bytes := []byte(v)
		d.value = &bytes
		return nil
	}

	b, err := json.Marshal(i)
	if err != nil {
		return parseError(d, i, err)
	}

	d.value = &b
	return nil
}

// Convert Dynamic into reflect value.
func (d *Dynamic) Convert(v reflect.Value) error {
	t := v.Type()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if d.value == nil {
		return nil
	}

	var valueToSet reflect.Value
	switch {
	case t.ConvertibleTo(reflect.TypeOf(Dynamic{})):
		valueToSet = reflect.ValueOf(*d)
	case t.ConvertibleTo(reflect.TypeOf([]byte{})):
		if t.Kind() == reflect.String {
			s := string(*d.value)
			valueToSet = reflect.ValueOf(s)
		} else {
			valueToSet = reflect.ValueOf(*d.value)
		}
	case t.Kind() == reflect.Slice || t.Kind() == reflect.Map:

		ptr := reflect.New(t)
		if err := json.Unmarshal([]byte(*d.value), ptr.Interface()); err != nil {
			return fmt.Errorf("Error occurred while trying to unmarshal Dynamic into a %s: %s", t.Kind(), err)
		}

		valueToSet = ptr.Elem()
	case t.Kind() == reflect.Struct:
		structPtr := reflect.New(t)

		if err := json.Unmarshal([]byte(*d.value), structPtr.Interface()); err != nil {
			return fmt.Errorf("Could not unmarshal type dynamic into receiver: %s", err)
		}

		valueToSet = structPtr.Elem()
	default:
		return fmt.Errorf("Column was type Kusto.Dynamic, receiver had base Kind %s ", t.Kind())
	}

	if v.Type().Kind() != reflect.Ptr {
		v.Set(valueToSet)
	} else {
		if v.IsZero() {
			v.Set(reflect.New(valueToSet.Type()))
		}
		v.Elem().Set(valueToSet)
	}
	return nil
}

// GetType returns the type of the value.
func (d *Dynamic) GetType() types.Column {
	return types.Dynamic
}
