package kusto

// value.go provides methods for converting a row to a *struct and for converting KustoValue into Go types
// or in the reverse.

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/types"

	"github.com/google/uuid"
)

// decodeToStruct takes a list of columns and a row to decode into "p" which will be a pointer
// to a struct (enforce in the decoder).
func decodeToStruct(cols Columns, row types.KustoValues, p interface{}) error {
	t := reflect.TypeOf(p)
	v := reflect.ValueOf(p)
	fields := newFields(cols, t)

	for i, col := range cols {
		if err := fields.convert(col, row[i], t, v); err != nil {
			return err
		}
	}
	return nil
}

// fields represents the fields inside a struct.
type fields struct {
	colNameToFieldName map[string]string
}

// newFields takes in the Columns from our row and the reflect.Type of our *struct.
func newFields(cols Columns, ptr reflect.Type) fields {
	nFields := fields{colNameToFieldName: map[string]string{}}
	for i := 0; i < ptr.Elem().NumField(); i++ {
		field := ptr.Elem().Field(i)
		if tag := field.Tag.Get("kusto"); strings.TrimSpace(tag) != "" {
			nFields.colNameToFieldName[tag] = field.Name
		} else {
			nFields.colNameToFieldName[field.Name] = field.Name
		}
	}

	return nFields
}

// match returns the name of the field in the struct that matches col. Empty string indicates there
// is no match.
func (f fields) match(col Column) string {
	return f.colNameToFieldName[col.Name]
}

// convert converts a KustoValue that is for Column col into "v" reflect.Value with reflect.Type "t".
func (f fields) convert(col Column, k types.KustoValue, t reflect.Type, v reflect.Value) error {
	fieldName, ok := f.colNameToFieldName[col.Name]
	if !ok {
		return nil
	}

	if fieldName == "-" {
		return nil
	}

	switch col.Type {
	case CTBool:
		return boolConvert(fieldName, col, k, t, v)
	case CTDateTime:
		return dateTimeConvert(fieldName, col, k, t, v)
	case CTDynamic:
		return dynamicConvert(fieldName, col, k, t, v)
	case CTGUID:
		return guidConvert(fieldName, col, k, t, v)
	case CTInt:
		return intConvert(fieldName, col, k, t, v)
	case CTLong:
		return longConvert(fieldName, col, k, t, v)
	case CTReal:
		return realConvert(fieldName, col, k, t, v)
	case CTString:
		return stringConvert(fieldName, col, k, t, v)
	case CTTimespan:
		return timespanConvert(fieldName, col, k, t, v)
	case CTDecimal:
		return decimalConvert(fieldName, col, k, t, v)
	}
	return fmt.Errorf("received a field type %q we don't recognize", col.Type)
}

/*
The next section has conversion types that allow us to change our types.KustoValue into the underlying types or
Go representations of them, and vice versus. Conversion from a types.KustoValue will be <type>Convert and
conversion to a types.KustoValue will be convert<Type>.  We support conversion to/from types.KustoValue to
mulitple Go types.  For example, our types.Bool could be converted to types.Bool, *types.Bool,
bool or *bool.  Others work similar to this.
*/

func boolConvert(fieldName string, col Column, k types.KustoValue, t reflect.Type, v reflect.Value) error {
	val, ok := k.(types.Bool)
	if !ok {
		return fmt.Errorf("Column %s is type %s was trying to store a KustoValue type of %T", col.Name, col.Type, k)
	}
	sf, _ := t.Elem().FieldByName(fieldName)
	switch {
	case sf.Type.Kind() == reflect.Bool:
		if val.Valid {
			v.Elem().FieldByName(fieldName).SetBool(val.Value)
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(new(bool))):
		if val.Valid {
			b := new(bool)
			if val.Value {
				*b = true
			}
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(b))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(types.Bool{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val))
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(&types.Bool{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(&val))
		return nil
	}
	return fmt.Errorf("column %s could not store in struct.%s: column was type Kusto.Bool, struct had base Kind %s ", col.Name, fieldName, sf.Type.Kind())
}

func dateTimeConvert(fieldName string, col Column, k types.KustoValue, t reflect.Type, v reflect.Value) error {
	val, ok := k.(types.DateTime)
	if !ok {
		return fmt.Errorf("Column %s is type %s was trying to store a KustoValue type of %T", col.Name, col.Type, k)
	}
	sf, _ := t.Elem().FieldByName(fieldName)
	switch {
	case sf.Type.AssignableTo(reflect.TypeOf(time.Time{})):
		if val.Valid {
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val.Value))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(new(time.Time))):
		if val.Valid {
			t := &val.Value
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(t))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(types.DateTime{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val))
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(&types.DateTime{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(&val))
		return nil
	}
	return fmt.Errorf("column %s could not store in struct.%s: column was type Kusto.DateTime, struct had type %s ", col.Name, fieldName, sf.Type.Name())
}

func timespanConvert(fieldName string, col Column, k types.KustoValue, t reflect.Type, v reflect.Value) error {
	val, ok := k.(types.Timespan)
	if !ok {
		return fmt.Errorf("Column %s is type %s was trying to store a KustoValue type of %T", col.Name, col.Type, k)
	}
	sf, _ := t.Elem().FieldByName(fieldName)
	switch {
	case sf.Type.AssignableTo(reflect.TypeOf(time.Duration(0))):
		if val.Valid {
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val.Value))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(new(time.Duration))):
		if val.Valid {
			t := &val.Value
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(t))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(types.Timespan{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val))
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(&types.Timespan{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(&val))
		return nil
	}
	return fmt.Errorf("column %s could not store in struct.%s: column was type Kusto.Timespan, struct had type %s ", col.Name, fieldName, sf.Type.Name())
}

func dynamicConvert(fieldName string, col Column, k types.KustoValue, t reflect.Type, v reflect.Value) error {
	val, ok := k.(types.Dynamic)
	if !ok {
		return fmt.Errorf("Column %s is type %s was tryihng to store a KustoValue type of %T", col.Name, col.Type, k)
	}
	sf, _ := t.Elem().FieldByName(fieldName)
	switch {
	case sf.Type.ConvertibleTo(reflect.TypeOf(types.Dynamic{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val))
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(&types.Dynamic{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(&val))
		return nil
	case sf.Type.Kind() == reflect.Ptr && sf.Type.Elem().Kind() == reflect.Struct:
		if !val.Valid {
			return nil
		}
		store := reflect.New(sf.Type.Elem())
		if err := json.Unmarshal([]byte(val.Value), store.Interface()); err != nil {
			return fmt.Errorf("Column %s of type dynamic could not unmarshal into the passed *struct: %s", col.Name, err)
		}
		v.Elem().FieldByName(fieldName).Set(store)
		return nil
	case sf.Type.Kind() == reflect.Ptr && sf.Type.Elem().Kind() == reflect.Map:
		if !val.Valid {
			return nil
		}
		if sf.Type.Elem().Key().Kind() != reflect.String {
			return fmt.Errorf("Column %s is type dymanic and can only be stored in a *map[string]interface{} or *struct", col.Name)
		}
		if sf.Type.Elem().Elem().Kind() != reflect.Interface {
			return fmt.Errorf("Column %s is type dymanic and can only be stored in a *map[string]interface{} or *struct", col.Name)
		}
		m := map[string]interface{}{}
		if err := json.Unmarshal([]byte(val.Value), &m); err != nil {
			return fmt.Errorf("Column %s of type dynamic could not unmarshal into the passed *map: %s", col.Name, err)
		}
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(&m))
		return nil
	case sf.Type.Kind() == reflect.Map:
		if !val.Valid {
			return nil
		}
		if sf.Type.Key().Kind() != reflect.String {
			return fmt.Errorf("Column %s is type dymanic and can only be stored in a *map[string]interface{} or *struct", col.Name)
		}
		if sf.Type.Elem().Kind() != reflect.Interface {
			return fmt.Errorf("Column %s is type dymanic and can only be stored in a *map[string]interface{} or *struct", col.Name)
		}

		m := map[string]interface{}{}
		if err := json.Unmarshal([]byte(val.Value), &m); err != nil {
			return fmt.Errorf("Column %s is type dynamic and the value could not be stored in a *map[string]interface{}: %s", col.Name, err)
		}
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(m))
		return nil
	case sf.Type.Kind() == reflect.Struct:
		structPtr := reflect.New(sf.Type)
		if err := json.Unmarshal([]byte(val.Value), structPtr.Interface()); err != nil {
			return fmt.Errorf("Column %s is type dynamic and the value could not be stored in passed struct: %s", col.Name, err)
		}
		v.Elem().FieldByName(fieldName).Set(structPtr.Elem())
		return nil
	}
	return fmt.Errorf("column %s could not store in struct.%s: column was type Kusto.Dynamic, struct had base Kind %s ", col.Name, fieldName, sf.Type.Kind())
}

func guidConvert(fieldName string, col Column, k types.KustoValue, t reflect.Type, v reflect.Value) error {
	val, ok := k.(types.GUID)
	if !ok {
		return fmt.Errorf("Column %s is type %s was trying to store a KustoValue type of %T", col.Name, col.Type, k)
	}
	sf, _ := t.Elem().FieldByName(fieldName)
	switch {
	case sf.Type.AssignableTo(reflect.TypeOf(uuid.UUID{})):
		if val.Valid {
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val.Value))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(new(uuid.UUID))):
		if val.Valid {
			t := &val.Value
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(t))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(types.GUID{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val))
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(&types.GUID{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(&val))
		return nil
	}
	return fmt.Errorf("column %s could not store in struct.%s: column was type Kusto.GUID, struct had type %s ", col.Name, fieldName, sf.Type.Name())
}

func intConvert(fieldName string, col Column, k types.KustoValue, t reflect.Type, v reflect.Value) error {
	val, ok := k.(types.Int)
	if !ok {
		return fmt.Errorf("Column %s is type %s was tryihng to store a KustoValue type of %T", col.Name, col.Type, k)
	}
	sf, _ := t.Elem().FieldByName(fieldName)
	switch {
	case sf.Type.Kind() == reflect.Int32:
		if val.Valid {
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val.Value))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(new(int32))):
		if val.Valid {
			i := &val.Value
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(i))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(types.Int{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val))
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(&types.Int{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(&val))
		return nil
	}
	return fmt.Errorf("column %s could not store in struct.%s: column was type Kusto.Int, struct had base Kind %s ", col.Name, fieldName, sf.Type.Kind())
}

func longConvert(fieldName string, col Column, k types.KustoValue, t reflect.Type, v reflect.Value) error {
	val, ok := k.(types.Long)
	if !ok {
		return fmt.Errorf("Column %s is type %s was tryihng to store a KustoValue type of %T", col.Name, col.Type, k)
	}
	sf, _ := t.Elem().FieldByName(fieldName)
	switch {
	case sf.Type.Kind() == reflect.Int64:
		if val.Valid {
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val.Value))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(new(int64))):
		if val.Valid {
			i := &val.Value
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(i))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(types.Long{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val))
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(&types.Long{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(&val))
		return nil
	}
	return fmt.Errorf("column %s could not store in struct.%s: column was type Kusto.Long, struct had base Kind %s ", col.Name, fieldName, sf.Type.Kind())
}

func realConvert(fieldName string, col Column, k types.KustoValue, t reflect.Type, v reflect.Value) error {
	val, ok := k.(types.Real)
	if !ok {
		return fmt.Errorf("Column %s is type %s was tryihng to store a KustoValue type of %T", col.Name, col.Type, k)
	}
	sf, _ := t.Elem().FieldByName(fieldName)
	switch {
	case sf.Type.Kind() == reflect.Float64:
		if val.Valid {
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val.Value))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(new(float64))):
		if val.Valid {
			i := &val.Value
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(i))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(types.Real{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val))
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(&types.Real{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(&val))
		return nil
	}
	return fmt.Errorf("column %s could not store in struct.%s: column was type Kusto.Real, struct had base Kind %s ", col.Name, fieldName, sf.Type.Kind())
}

func stringConvert(fieldName string, col Column, k types.KustoValue, t reflect.Type, v reflect.Value) error {
	val, ok := k.(types.String)
	if !ok {
		return fmt.Errorf("Column %s is type %s was trying to store a KustoValue type of %T", col.Name, col.Type, k)
	}
	sf, _ := t.Elem().FieldByName(fieldName)
	switch {
	case sf.Type.Kind() == reflect.String:
		if val.Valid {
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val.Value))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(new(string))):
		if val.Valid {
			i := &val.Value
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(i))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(types.String{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val))
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(&types.String{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(&val))
		return nil
	}
	return fmt.Errorf("column %s could not store in struct.%s: column was type Kusto.String, struct had base Kind %s ", col.Name, fieldName, sf.Type.Kind())
}

func decimalConvert(fieldName string, col Column, k types.KustoValue, t reflect.Type, v reflect.Value) error {
	val, ok := k.(types.Decimal)
	if !ok {
		return fmt.Errorf("Column %s is type %s was trying to store a KustoValue type of %T", col.Name, col.Type, k)
	}
	sf, _ := t.Elem().FieldByName(fieldName)
	switch {
	case sf.Type.Kind() == reflect.String:
		if val.Valid {
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val.Value))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(new(string))):
		if val.Valid {
			i := &val.Value
			v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(i))
		}
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(types.Decimal{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(val))
		return nil
	case sf.Type.ConvertibleTo(reflect.TypeOf(&types.Decimal{})):
		v.Elem().FieldByName(fieldName).Set(reflect.ValueOf(&val))
		return nil
	}
	return fmt.Errorf("column %s could not store in struct.%s: column was type Kusto.Decimal, struct had base Kind %s ", col.Name, fieldName, sf.Type.Kind())
}
