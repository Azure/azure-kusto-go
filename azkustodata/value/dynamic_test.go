package value_test

import (
	"reflect"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustodata/value"
	"github.com/stretchr/testify/assert"
)

type DynamicConverterTestCase struct {
	Desc   string
	Value  value.Dynamic
	Target reflect.Value
	Want   interface{}
}

type DynamicConverterNegativeTestCase struct {
	Desc   string
	Value  value.Dynamic
	Target reflect.Value
	Error  string
}

type TestStruct struct {
	Name string `json:"name"`
	ID   int64  `json:"id"`
}

func TestDynamicConverter(t *testing.T) {
	t.Parallel()

	wantByteArray := []byte(`hello`)
	emptyStr := ""
	wantStr := "hello"

	testCases := []DynamicConverterTestCase{
		{
			Desc:   "convert to dynamic",
			Value:  *value.NewDynamic([]byte(`hello`)),
			Target: reflect.ValueOf(&value.Dynamic{}),
			Want:   value.NewDynamic([]byte(`hello`)),
		},
		{
			Desc:   "convert to []byte",
			Value:  *value.NewDynamic([]byte(`hello`)),
			Target: reflect.ValueOf(&[]byte{}),
			Want:   &wantByteArray,
		},
		{
			Desc:   "convert to string",
			Value:  *value.NewDynamic([]byte(`hello`)),
			Target: reflect.ValueOf(&emptyStr),
			Want:   &wantStr,
		},
		{
			Desc:   "convert to []string",
			Value:  *value.NewDynamic([]byte(`["hello", "world"]`)),
			Target: reflect.ValueOf(&[]string{}),
			Want:   &[]string{"hello", "world"},
		},
		{
			Desc:   "convert to []int64",
			Value:  *value.NewDynamic([]byte(`[1,2,3]`)),
			Target: reflect.ValueOf(&[]int64{}),
			Want:   &[]int64{1, 2, 3},
		},
		{
			Desc:   "convert to []struct",
			Value:  *value.NewDynamic([]byte(`[{"name":"A","id":1},{"name":"B","id":2}]`)),
			Target: reflect.ValueOf(&[]TestStruct{}),
			Want: &[]TestStruct{
				{Name: "A", ID: 1},
				{Name: "B", ID: 2},
			},
		},
		{
			Desc:   "convert to []map[string]interface{}",
			Value:  *value.NewDynamic([]byte(`[{"name":"A","id":1},{"name":"B","id":2}]`)),
			Target: reflect.ValueOf(&[]map[string]interface{}{}),
			Want: &[]map[string]interface{}{
				{"name": "A", "id": float64(1)},
				{"name": "B", "id": float64(2)},
			},
		},
		{
			Desc:   "convert to []map[string]struct",
			Value:  *value.NewDynamic([]byte(`[{"group1":{"name":"A","id":1}},{"group2":{"name":"B","id":2}}]`)),
			Target: reflect.ValueOf(&[]map[string]TestStruct{}),
			Want: &[]map[string]TestStruct{
				{"group1": {Name: "A", ID: 1}},
				{"group2": {Name: "B", ID: 2}},
			},
		},
		{
			Desc:   "convert to struct",
			Value:  *value.NewDynamic([]byte(`{"name":"A","id":1}`)),
			Target: reflect.ValueOf(&TestStruct{}),
			Want: &TestStruct{
				Name: "A",
				ID:   1,
			},
		},
		{
			Desc:   "convert to map[string]interface{}",
			Value:  *value.NewDynamic([]byte(`{"name":"A","id":1}`)),
			Target: reflect.ValueOf(&map[string]interface{}{}),
			Want: &map[string]interface{}{
				"name": "A",
				"id":   float64(1),
			},
		},
		{
			Desc:   "convert to map[string]struct",
			Value:  *value.NewDynamic([]byte(`{"group1": {"name":"A","id":1}}`)),
			Target: reflect.ValueOf(&map[string]TestStruct{}),
			Want: &map[string]TestStruct{
				"group1": {
					Name: "A",
					ID:   1,
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc // capture range variable
		t.Run(tc.Desc, func(t *testing.T) {
			t.Parallel()

			err := tc.Value.Convert(tc.Target.Elem())
			assert.NoError(t, err)

			assert.EqualValues(t, tc.Want, tc.Target.Interface())

			err = tc.Value.Convert(tc.Target)
			assert.NoError(t, err)

			assert.EqualValues(t, tc.Want, tc.Target.Interface())
		})

	}
}

func TestDynamicConverterNegative(t *testing.T) {
	t.Parallel()

	testCases := []DynamicConverterNegativeTestCase{
		{
			Desc:   "fail to convert to []string",
			Value:  *value.NewDynamic([]byte(`["hello", "world`)),
			Target: reflect.ValueOf(&[]string{}),
			Error:  "Error occurred while trying to unmarshal Dynamic into a slice: unexpected end of JSON input",
		},
		{
			Desc:   "fail to convert to []int64",
			Value:  *value.NewDynamic([]byte(`[1,2,"3"]`)),
			Target: reflect.ValueOf(&[]int64{}),
			Error:  "Error occurred while trying to unmarshal Dynamic into a slice: json: cannot unmarshal string into Go value of type int64",
		},
		{
			Desc:   "fail to convert to []struct",
			Value:  *value.NewDynamic([]byte(`[{"name":"A","id":1},{"name":"B","id":2}`)),
			Target: reflect.ValueOf(&[]TestStruct{}),
			Error:  "Error occurred while trying to unmarshal Dynamic into a slice: unexpected end of JSON input",
		},
		{
			Desc:   "convert to []map[string]interface{}",
			Value:  *value.NewDynamic([]byte(`[{"name":"A","id":1},{"name":"B","id":2}`)),
			Target: reflect.ValueOf(&[]map[string]interface{}{}),
			Error:  "Error occurred while trying to unmarshal Dynamic into a slice: unexpected end of JSON input",
		},
		{
			Desc:   "convert to []map[string]struct",
			Value:  *value.NewDynamic([]byte(`[{"group1":{"name":"A","id":1}},{"group2":{"name":"B","id":2}}`)),
			Target: reflect.ValueOf(&[]map[string]TestStruct{}),
			Error:  "Error occurred while trying to unmarshal Dynamic into a slice: unexpected end of JSON input",
		},
		{
			Desc:   "convert to struct",
			Value:  *value.NewDynamic([]byte(`{"name":"A","id":1`)),
			Target: reflect.ValueOf(&TestStruct{}),
			Error:  "Could not unmarshal type dynamic into receiver: unexpected end of JSON input",
		},
		{
			Desc:   "convert to map[string]interface{}",
			Value:  *value.NewDynamic([]byte(`{"named":"A","id":1`)),
			Target: reflect.ValueOf(&map[string]interface{}{}),
			Error:  "Error occurred while trying to unmarshal Dynamic into a map: unexpected end of JSON input",
		},
		{
			Desc:   "convert to map[string]struct",
			Value:  *value.NewDynamic([]byte(`{"group1":{"named":"A","id":1}`)),
			Target: reflect.ValueOf(&map[string]TestStruct{}),
			Error:  "Error occurred while trying to unmarshal Dynamic into a map: unexpected end of JSON input",
		},
	}

	for _, tc := range testCases {
		tc := tc // capture range variable
		t.Run(tc.Desc, func(t *testing.T) {
			t.Parallel()

			err := tc.Value.Convert(tc.Target.Elem())
			assert.EqualError(t, err, tc.Error)

			err = tc.Value.Convert(tc.Target)
			assert.EqualError(t, err, tc.Error)
		})

	}
}
