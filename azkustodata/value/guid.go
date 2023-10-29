package value

import (
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"reflect"

	"github.com/google/uuid"
)

// GUID represents a Kusto GUID type.  GUID implements Kusto.
type GUID struct {
	// Value holds the value of the type.
	Value uuid.NullUUID
}

func (*GUID) isKustoVal() {}

// String implements fmt.Stringer.
func (g *GUID) String() string {
	if !g.Value.Valid {
		return ""
	}
	return g.Value.UUID.String()
}

// Unmarshal unmarshals i into GUID. i must be a string representing a GUID or nil.
func (g *GUID) Unmarshal(i interface{}) error {
	if i == nil {
		g.Value = uuid.NullUUID{}
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
	g.Value = uuid.NullUUID{
		UUID:  u,
		Valid: true,
	}
	return nil
}

// Convert GUID into reflect value.
func (g *GUID) Convert(v reflect.Value) error {
	t := v.Type()
	switch {
	case t.AssignableTo(reflect.TypeOf(uuid.UUID{})):
		if g.Value.Valid {
			v.Set(reflect.ValueOf(g.Value.UUID))
		}
		return nil
	case t.ConvertibleTo(reflect.TypeOf(new(uuid.UUID))):
		if g.Value.Valid {
			t := &g.Value.UUID
			v.Set(reflect.ValueOf(t))
		}
		return nil
	case t.ConvertibleTo(reflect.TypeOf(GUID{})):
		v.Set(reflect.ValueOf(*g))
		return nil
	case t.ConvertibleTo(reflect.TypeOf(&GUID{})):
		v.Set(reflect.ValueOf(g))
		return nil
	}
	return fmt.Errorf("Column was type Kusto.GUID, receiver had base Kind %s ", t.Kind())
}

// GetValue returns the value of the type.
func (g *GUID) GetValue() interface{} {
	if !g.Value.Valid {
		return nil
	}
	return g.Value.UUID
}

// GetType returns the type of the value.
func (g *GUID) GetType() types.Column {
	return types.GUID
}
