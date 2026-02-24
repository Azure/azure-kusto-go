package value_test

import (
	"reflect"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/stretchr/testify/assert"
)

func TestStringConvert(t *testing.T) {
	t.Parallel()

	wantStr := "hello"

	testCases := []struct {
		desc string
		val  value.String
		want interface{}
	}{
		{desc: "convert to string", val: *value.NewString("hello"), want: &wantStr},
		{desc: "convert to value.String (struct)", val: *value.NewString("hello"), want: value.NewString("hello")},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			switch want := tc.want.(type) {
			case *string:
				// element (string)
				var targetElem string
				err := tc.val.Convert(reflect.ValueOf(&targetElem).Elem())
				assert.NoError(t, err)
				assert.EqualValues(t, *want, targetElem)

				// pointer to string
				var pstr *string
				err = tc.val.Convert(reflect.ValueOf(&pstr).Elem())
				assert.NoError(t, err)
				if assert.NotNil(t, pstr) {
					assert.EqualValues(t, *want, *pstr)
				}

			case *value.String:
				var target value.String
				err := tc.val.Convert(reflect.ValueOf(&target).Elem())
				assert.NoError(t, err)
				assert.EqualValues(t, want, &target)

				var pval *value.String
				err = tc.val.Convert(reflect.ValueOf(&pval).Elem())
				assert.NoError(t, err)
				if assert.NotNil(t, pval) {
					assert.EqualValues(t, *want, *pval)
				}
			default:
				t.Fatalf("unsupported want type %T", want)
			}
		})
	}

	t.Run("convert to named string type", func(t *testing.T) {
		t.Parallel()
		type namedString string
		var target namedString
		err := value.NewString("hello").Convert(reflect.ValueOf(&target).Elem())
		assert.NoError(t, err)
		assert.EqualValues(t, namedString("hello"), target)
	})

	t.Run("convert to incompatible type", func(t *testing.T) {
		i := 0
		err := value.NewString("x").Convert(reflect.ValueOf(&i).Elem())
		assert.Error(t, err)
	})
}
