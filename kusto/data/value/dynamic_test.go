package value_test

import (
	"reflect"
	"testing"

	"github.com/Azure/azure-kusto-go/kusto/data/value"
	"github.com/kylelemons/godebug/pretty"
)

type DynamicConverterTestCase struct {
	Desc   string
	Value  value.Dynamic
	Target reflect.Value
	Want   interface{}
}

type TestStruct struct {
	Name string `json:"name"`
	ID   int64  `json:"id"`
}

func TestDynamicConverter(t *testing.T) {
	t.Parallel()

	testCases := []DynamicConverterTestCase{
		{
			Desc:   "convert to dynamic",
			Value:  value.Dynamic{Value: []byte(`hello`), Valid: true},
			Target: reflect.ValueOf(&value.Dynamic{}),
			Want:   value.Dynamic{Value: []byte(`hello`), Valid: true},
		},
		{
			Desc:   "convert to []byte",
			Value:  value.Dynamic{Value: []byte(`hello`), Valid: true},
			Target: reflect.ValueOf(&[]byte{}),
			Want:   []byte(`hello`),
		},
		{
			Desc:   "convert to []string",
			Value:  value.Dynamic{Value: []byte(`["hello", "world"]`), Valid: true},
			Target: reflect.ValueOf(&[]string{}),
			Want:   []string{"hello", "world"},
		},
		{
			Desc:   "convert to []int64",
			Value:  value.Dynamic{Value: []byte(`[1,2,3]`), Valid: true},
			Target: reflect.ValueOf(&[]int64{}),
			Want:   []int64{1, 2, 3},
		},
		{
			Desc:   "convert to []struct",
			Value:  value.Dynamic{Value: []byte(`[{"name":"A","id":1},{"name":"B","id":2}]`), Valid: true},
			Target: reflect.ValueOf(&[]TestStruct{}),
			Want: []TestStruct{
				{Name: "A", ID: 1},
				{Name: "B", ID: 2},
			},
		},
		{
			Desc:   "convert to []map[string]interface{}",
			Value:  value.Dynamic{Value: []byte(`[{"name":"A","id":1},{"name":"B","id":2}]`), Valid: true},
			Target: reflect.ValueOf(&[]map[string]interface{}{}),
			Want: []map[string]interface{}{
				{"name": "A", "id": 1},
				{"name": "B", "id": 2},
			},
		},
		{
			Desc:   "convert to []map[string]struct",
			Value:  value.Dynamic{Value: []byte(`[{"group1":{"name":"A","id":1}},{"group2":{"name":"B","id":2}}]`), Valid: true},
			Target: reflect.ValueOf(&[]map[string]TestStruct{}),
			Want: []map[string]TestStruct{
				{"group1": {Name: "A", ID: 1}},
				{"group2": {Name: "B", ID: 2}},
			},
		},
		{
			Desc:   "convert to struct",
			Value:  value.Dynamic{Value: []byte(`{"name":"A","id":1}`), Valid: true},
			Target: reflect.ValueOf(&TestStruct{}),
			Want: TestStruct{
				Name: "A",
				ID:   1,
			},
		},
		{
			Desc:   "convert to map[string]interface{}",
			Value:  value.Dynamic{Value: []byte(`{"name":"A","id":1}`), Valid: true},
			Target: reflect.ValueOf(&map[string]interface{}{}),
			Want: map[string]interface{}{
				"name": "A",
				"id":   1,
			},
		},
		{
			Desc:   "convert to map[string]struct",
			Value:  value.Dynamic{Value: []byte(`{"group1": {"name":"A","id":1}}`), Valid: true},
			Target: reflect.ValueOf(&map[string]TestStruct{}),
			Want: map[string]TestStruct{
				"group1": {
					Name: "A",
					ID:   1,
				},
			},
		},
	}

	for _, tc := range testCases {
		if err := tc.Value.Convert(tc.Target.Elem()); err != nil {
			t.Errorf("TestDynamicConvert(%s): %s", tc.Desc, err)
		}
		if diff := pretty.Compare(tc.Want, tc.Target.Interface()); diff != "" {
			t.Errorf("TestDynamicConvert(%s): -want/+got:\n%s", tc.Desc, diff)
		}

		if err := tc.Value.Convert(tc.Target); err != nil {
			t.Errorf("TestDynamicConvert(%s): %s", tc.Desc, err)
		}
		if diff := pretty.Compare(tc.Want, tc.Target.Interface()); diff != "" {
			t.Errorf("TestDynamicConvert(%s): -want/+got:\n%s", tc.Desc, diff)
		}
	}
}
