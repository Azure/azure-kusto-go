package table

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/data/value"
	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"
)

var now = time.Now()
var guid = uuid.New()

type SomeJSON struct {
	Name string
	ID   int
}

func TestFieldsConvert(t *testing.T) {
	t.Parallel()

	myStruct := SomeJSON{
		Name: "Adam",
		ID:   1,
	}

	myArrayOfStruct := []SomeJSON{
		{
			Name: "Adam",
			ID:   1,
		},
		{
			Name: "Bob",
			ID:   2,
		},
	}

	myJSON, err := json.Marshal(myStruct)
	if err != nil {
		panic(err)
	}

	myJSONArray, err := json.Marshal(myArrayOfStruct)
	if err != nil {
		panic(err)
	}

	myJSONStr := string(myJSON)
	myJSONStrPtr := &myJSONStr

	myJSONArrayStr := string(myJSONArray)
	myJSONArrayStrPtr := &myJSONArrayStr

	jsonMap := map[string]interface{}{}
	if err := json.Unmarshal(myJSON, &jsonMap); err != nil {
		panic(err)
	}

	jsonList := []interface{}{}
	if err := json.Unmarshal(myJSONArray, &jsonList); err != nil {
		panic(err)
	}

	tests := []struct {
		desc      string
		columns   Columns
		k         value.Kusto
		ptrStruct interface{}
		err       bool
		want      interface{}
	}{
		{
			desc: "valid Bool",
			columns: Columns{
				{Type: types.Bool, Name: "bool"},
				{Type: types.Bool, Name: "ptrbool"},
				{Type: types.Bool, Name: "kBool"},
				{Type: types.Bool, Name: "PtrkBool"},
			},
			k: value.Bool{Value: true, Valid: true},
			ptrStruct: &struct {
				Bool     bool       `kusto:"bool"`
				PtrBool  *bool      `kusto:"ptrbool"`
				KBool    value.Bool `kusto:"kBool"`
				PtrkBool *value.Bool
			}{},
			err: false,
			want: &struct {
				Bool     bool       `kusto:"bool"`
				PtrBool  *bool      `kusto:"ptrbool"`
				KBool    value.Bool `kusto:"kBool"`
				PtrkBool *value.Bool
			}{true, boolPtr(true), value.Bool{Value: true, Valid: true}, &value.Bool{Value: true, Valid: true}},
		},
		{
			desc: "non-valid Bool",
			columns: Columns{
				{Type: types.Bool, Name: "bool"},
				{Type: types.Bool, Name: "ptrbool"},
				{Type: types.Bool, Name: "kBool"},
				{Type: types.Bool, Name: "PtrkBool"},
			},
			k: value.Bool{Value: false, Valid: false},
			ptrStruct: &struct {
				Bool     bool       `kusto:"bool"`
				PtrBool  *bool      `kusto:"ptrbool"`
				KBool    value.Bool `kusto:"kBool"`
				PtrkBool *value.Bool
			}{},
			err: false,
			want: &struct {
				Bool     bool       `kusto:"bool"`
				PtrBool  *bool      `kusto:"ptrbool"`
				KBool    value.Bool `kusto:"kBool"`
				PtrkBool *value.Bool
			}{false, nil, value.Bool{Value: false, Valid: false}, &value.Bool{Value: false, Valid: false}},
		},
		{
			desc: "valid DateTime",
			columns: Columns{
				{Type: types.DateTime, Name: "time"},
				{Type: types.DateTime, Name: "ptrtime"},
				{Type: types.DateTime, Name: "dateTime"},
				{Type: types.DateTime, Name: "PtrDateTime"},
			},
			k: value.DateTime{Value: now, Valid: true},
			ptrStruct: &struct {
				Time        time.Time      `kusto:"time"`
				PtrTime     *time.Time     `kusto:"ptrtime"`
				DateTime    value.DateTime `kusto:"dateTime"`
				PtrDateTime *value.DateTime
			}{},
			err: false,
			want: &struct {
				Time        time.Time      `kusto:"time"`
				PtrTime     *time.Time     `kusto:"ptrtime"`
				DateTime    value.DateTime `kusto:"dateTime"`
				PtrDateTime *value.DateTime
			}{now, &now, value.DateTime{Value: now, Valid: true}, &value.DateTime{Value: now, Valid: true}},
		},
		{
			desc: "non-valid DateTime",
			columns: Columns{
				{Type: types.DateTime, Name: "time"},
				{Type: types.DateTime, Name: "ptrtime"},
				{Type: types.DateTime, Name: "dateTime"},
				{Type: types.DateTime, Name: "PtrDateTime"},
			},
			k: value.DateTime{Value: time.Time{}, Valid: false},
			ptrStruct: &struct {
				Time        time.Time      `kusto:"time"`
				PtrTime     *time.Time     `kusto:"ptrtime"`
				DateTime    value.DateTime `kusto:"dateTime"`
				PtrDateTime *value.DateTime
			}{},
			err: false,
			want: &struct {
				Time        time.Time      `kusto:"time"`
				PtrTime     *time.Time     `kusto:"ptrtime"`
				DateTime    value.DateTime `kusto:"dateTime"`
				PtrDateTime *value.DateTime
			}{time.Time{}, nil, value.DateTime{Value: time.Time{}, Valid: false}, &value.DateTime{Value: time.Time{}, Valid: false}},
		},
		{
			desc: "valid Dynamic",
			columns: Columns{
				{Type: types.Dynamic, Name: "Struct"},
				{Type: types.Dynamic, Name: "PtrStruct"},
				{Type: types.Dynamic, Name: "String"},
				{Type: types.Dynamic, Name: "PtrString"},
				{Type: types.Dynamic, Name: "Map"},
				{Type: types.Dynamic, Name: "PtrMap"},
				{Type: types.Dynamic, Name: "Dynamic"},
				{Type: types.Dynamic, Name: "PtrDynamic"},
			},
			k: value.Dynamic{Value: myJSON, Valid: true},
			ptrStruct: &struct {
				Struct     SomeJSON
				PtrStruct  *SomeJSON
				String     string
				PtrString  *string
				Map        map[string]interface{}
				PtrMap     *map[string]interface{}
				Dynamic    value.Dynamic
				PtrDynamic *value.Dynamic
			}{},
			err: false,
			want: &struct {
				Struct     SomeJSON
				PtrStruct  *SomeJSON
				String     string
				PtrString  *string
				Map        map[string]interface{}
				PtrMap     *map[string]interface{}
				Dynamic    value.Dynamic
				PtrDynamic *value.Dynamic
			}{
				myStruct,
				&myStruct,
				myJSONStr,
				myJSONStrPtr,
				map[string]interface{}{
					"Name": "Adam",
					"ID":   float64(1),
				},
				&map[string]interface{}{
					"Name": "Adam",
					"ID":   float64(1),
				},
				value.Dynamic{Value: myJSON, Valid: true},
				&value.Dynamic{Value: myJSON, Valid: true},
			},
		},
		{
			desc: "valid Dynamic list",
			columns: Columns{
				{Type: types.Dynamic, Name: "Struct"},
				{Type: types.Dynamic, Name: "PtrStruct"},
				{Type: types.Dynamic, Name: "String"},
				{Type: types.Dynamic, Name: "PtrString"},
				{Type: types.Dynamic, Name: "Slice"},
				{Type: types.Dynamic, Name: "PtrSlice"},
				{Type: types.Dynamic, Name: "Dynamic"},
				{Type: types.Dynamic, Name: "PtrDynamic"},
			},
			k: value.Dynamic{Value: myJSONArray, Valid: true},
			ptrStruct: &struct {
				Struct     []SomeJSON
				PtrStruct  *[]SomeJSON
				String     string
				PtrString  *string
				Slice      []map[string]interface{}
				PtrSlice   *[]map[string]interface{}
				Dynamic    value.Dynamic
				PtrDynamic *value.Dynamic
			}{},
			err: false,
			want: &struct {
				Struct     []SomeJSON
				PtrStruct  *[]SomeJSON
				String     string
				PtrString  *string
				Slice      []map[string]interface{}
				PtrSlice   *[]map[string]interface{}
				Dynamic    value.Dynamic
				PtrDynamic *value.Dynamic
			}{
				myArrayOfStruct,
				&myArrayOfStruct,
				myJSONArrayStr,
				myJSONArrayStrPtr,
				[]map[string]interface{}{
					{
						"Name": "Adam",
						"ID":   float64(1),
					},
					{
						"Name": "Bob",
						"ID":   float64(2),
					},
				},
				&[]map[string]interface{}{
					{
						"Name": "Adam",
						"ID":   float64(1),
					},
					{
						"Name": "Bob",
						"ID":   float64(2),
					},
				},
				value.Dynamic{Value: myJSONArray, Valid: true},
				&value.Dynamic{Value: myJSONArray, Valid: true},
			},
		},
		{
			desc: "non-valid Dynamic",
			columns: Columns{
				{Type: types.Dynamic, Name: "Struct"},
				{Type: types.Dynamic, Name: "PtrStruct"},
				{Type: types.Dynamic, Name: "String"},
				{Type: types.Dynamic, Name: "PtrString"},
				{Type: types.Dynamic, Name: "Map"},
				{Type: types.Dynamic, Name: "PtrMap"},
				{Type: types.Dynamic, Name: "Dynamic"},
				{Type: types.Dynamic, Name: "PtrDynamic"},
			},
			k: value.Dynamic{Value: myJSON, Valid: false},
			ptrStruct: &struct {
				Struct     SomeJSON
				PtrStruct  *SomeJSON
				String     string
				PtrString  *string
				Map        map[string]interface{}
				PtrMap     *map[string]interface{}
				Dynamic    value.Dynamic
				PtrDynamic *value.Dynamic
			}{},
			err: false,
			want: &struct {
				Struct     SomeJSON
				PtrStruct  *SomeJSON
				String     string
				PtrString  *string
				Map        map[string]interface{}
				PtrMap     *map[string]interface{}
				Dynamic    value.Dynamic
				PtrDynamic *value.Dynamic
			}{
				myStruct,
				&myStruct,
				myJSONStr,
				myJSONStrPtr,
				nil,
				nil,
				value.Dynamic{Value: myJSON, Valid: false},
				&value.Dynamic{Value: myJSON, Valid: false},
			},
		},
		{
			desc: "non-valid Dynamic list",
			columns: Columns{
				{Type: types.Dynamic, Name: "Struct"},
				{Type: types.Dynamic, Name: "PtrStruct"},
				{Type: types.Dynamic, Name: "String"},
				{Type: types.Dynamic, Name: "PtrString"},
				{Type: types.Dynamic, Name: "Slice"},
				{Type: types.Dynamic, Name: "PtrSlice"},
				{Type: types.Dynamic, Name: "Dynamic"},
				{Type: types.Dynamic, Name: "PtrDynamic"},
			},
			k: value.Dynamic{Value: myJSONArray, Valid: false},
			ptrStruct: &struct {
				Struct     []SomeJSON
				PtrStruct  *[]SomeJSON
				String     string
				PtrString  *string
				Slice      []map[string]interface{}
				PtrSlice   *[]map[string]interface{}
				Dynamic    value.Dynamic
				PtrDynamic *value.Dynamic
			}{},
			err: false,
			want: &struct {
				Struct     []SomeJSON
				PtrStruct  *[]SomeJSON
				String     string
				PtrString  *string
				Slice      []map[string]interface{}
				PtrSlice   *[]map[string]interface{}
				Dynamic    value.Dynamic
				PtrDynamic *value.Dynamic
			}{
				nil,
				nil,
				myJSONArrayStr,
				myJSONArrayStrPtr,
				nil,
				nil,
				value.Dynamic{Value: myJSONArray, Valid: false},
				&value.Dynamic{Value: myJSONArray, Valid: false},
			},
		},
		{
			desc: "valid GUID",
			columns: Columns{
				{Type: types.GUID, Name: "guid"},
				{Type: types.GUID, Name: "ptrguid"},
				{Type: types.GUID, Name: "kGUID"},
				{Type: types.GUID, Name: "PtrKGUID"},
			},
			k: value.GUID{Value: guid, Valid: true},
			ptrStruct: &struct {
				GUID     uuid.UUID  `kusto:"guid"`
				PtrGUID  *uuid.UUID `kusto:"ptrguid"`
				KGUID    value.GUID `kusto:"kGUID"`
				PtrKGUID *value.GUID
			}{},
			err: false,
			want: &struct {
				GUID     uuid.UUID  `kusto:"guid"`
				PtrGUID  *uuid.UUID `kusto:"ptrguid"`
				KGUID    value.GUID `kusto:"kGUID"`
				PtrKGUID *value.GUID
			}{guid, &guid, value.GUID{Value: guid, Valid: true}, &value.GUID{Value: guid, Valid: true}},
		},
		{
			desc: "non-valid GUID",
			columns: Columns{
				{Type: types.GUID, Name: "guid"},
				{Type: types.GUID, Name: "ptrguid"},
				{Type: types.GUID, Name: "kGUID"},
				{Type: types.GUID, Name: "PtrKGUID"},
			},
			k: value.GUID{Value: uuid.UUID{}, Valid: false},
			ptrStruct: &struct {
				GUID     uuid.UUID  `kusto:"guid"`
				PtrGUID  *uuid.UUID `kusto:"ptrguid"`
				KGUID    value.GUID `kusto:"kGUID"`
				PtrKGUID *value.GUID
			}{},
			err: false,
			want: &struct {
				GUID     uuid.UUID  `kusto:"guid"`
				PtrGUID  *uuid.UUID `kusto:"ptrguid"`
				KGUID    value.GUID `kusto:"kGUID"`
				PtrKGUID *value.GUID
			}{uuid.UUID{}, nil, value.GUID{Value: uuid.UUID{}, Valid: false}, &value.GUID{Value: uuid.UUID{}, Valid: false}},
		},
		{
			desc: "valid Int",
			columns: Columns{
				{Type: types.Int, Name: "int"},
				{Type: types.Int, Name: "ptrint"},
				{Type: types.Int, Name: "kInt"},
				{Type: types.Int, Name: "PtrkInt"},
			},
			k: value.Int{Value: 1, Valid: true},
			ptrStruct: &struct {
				Int     int32     `kusto:"int"`
				PtrInt  *int32    `kusto:"ptrint"`
				KInt    value.Int `kusto:"kInt"`
				PtrkInt *value.Int
			}{},
			err: false,
			want: &struct {
				Int     int32     `kusto:"int"`
				PtrInt  *int32    `kusto:"ptrint"`
				KInt    value.Int `kusto:"kInt"`
				PtrkInt *value.Int
			}{1, int32Ptr(1), value.Int{Value: 1, Valid: true}, &value.Int{Value: 1, Valid: true}},
		},
		{
			desc: "non-valid Int",
			columns: Columns{
				{Type: types.Int, Name: "int"},
				{Type: types.Int, Name: "ptrint"},
				{Type: types.Int, Name: "kInt"},
				{Type: types.Int, Name: "PtrkInt"},
			},
			k: value.Int{Value: 0, Valid: false},
			ptrStruct: &struct {
				Int     int32     `kusto:"int"`
				PtrInt  *int32    `kusto:"ptrint"`
				KInt    value.Int `kusto:"kInt"`
				PtrkInt *value.Int
			}{},
			err: false,
			want: &struct {
				Int     int32     `kusto:"int"`
				PtrInt  *int32    `kusto:"ptrint"`
				KInt    value.Int `kusto:"kInt"`
				PtrkInt *value.Int
			}{0, nil, value.Int{Value: 0, Valid: false}, &value.Int{Value: 0, Valid: false}},
		},
		{
			desc: "valid Long",
			columns: Columns{
				{Type: types.Long, Name: "long"},
				{Type: types.Long, Name: "ptrLong"},
				{Type: types.Long, Name: "kLong"},
				{Type: types.Long, Name: "PtrkLong"},
			},
			k: value.Long{Value: 1, Valid: true},
			ptrStruct: &struct {
				Long     int64      `kusto:"long"`
				PtrLong  *int64     `kusto:"ptrLong"`
				KLong    value.Long `kusto:"kLong"`
				PtrkLong *value.Long
			}{},
			err: false,
			want: &struct {
				Long     int64      `kusto:"long"`
				PtrLong  *int64     `kusto:"ptrLong"`
				KLong    value.Long `kusto:"kLong"`
				PtrkLong *value.Long
			}{1, int64Ptr(1), value.Long{Value: 1, Valid: true}, &value.Long{Value: 1, Valid: true}},
		},
		{
			desc: "non-valid Long",
			columns: Columns{
				{Type: types.Long, Name: "long"},
				{Type: types.Long, Name: "ptrLong"},
				{Type: types.Long, Name: "kLong"},
				{Type: types.Long, Name: "PtrkLong"},
			},
			k: value.Long{Value: 0, Valid: false},
			ptrStruct: &struct {
				Long     int64      `kusto:"long"`
				PtrLong  *int64     `kusto:"ptrLong"`
				KLong    value.Long `kusto:"kLong"`
				PtrkLong *value.Long
			}{},
			err: false,
			want: &struct {
				Long     int64      `kusto:"long"`
				PtrLong  *int64     `kusto:"ptrLong"`
				KLong    value.Long `kusto:"kLong"`
				PtrkLong *value.Long
			}{0, nil, value.Long{Value: 0, Valid: false}, &value.Long{Value: 0, Valid: false}},
		},
		{
			desc: "valid real",
			columns: Columns{
				{Type: types.Real, Name: "real"},
				{Type: types.Real, Name: "ptrReal"},
				{Type: types.Real, Name: "kReal"},
				{Type: types.Real, Name: "PtrkReal"},
			},
			k: value.Real{Value: 3.2, Valid: true},
			ptrStruct: &struct {
				Real     float64    `kusto:"real"`
				PtrReal  *float64   `kusto:"ptrReal"`
				KReal    value.Real `kusto:"kReal"`
				PtrkReal *value.Real
			}{},
			err: false,
			want: &struct {
				Real     float64    `kusto:"real"`
				PtrReal  *float64   `kusto:"ptrReal"`
				KReal    value.Real `kusto:"kReal"`
				PtrkReal *value.Real
			}{3.2, float64Ptr(3.2), value.Real{Value: 3.2, Valid: true}, &value.Real{Value: 3.2, Valid: true}},
		},
		{
			desc: "non-valid real",
			columns: Columns{
				{Type: types.Real, Name: "real"},
				{Type: types.Real, Name: "ptrReal"},
				{Type: types.Real, Name: "kReal"},
				{Type: types.Real, Name: "PtrkReal"},
			},
			k: value.Real{Value: 0.0, Valid: false},
			ptrStruct: &struct {
				Real     float64    `kusto:"real"`
				PtrReal  *float64   `kusto:"ptrReal"`
				KReal    value.Real `kusto:"kReal"`
				PtrkReal *value.Real
			}{},
			err: false,
			want: &struct {
				Real     float64    `kusto:"real"`
				PtrReal  *float64   `kusto:"ptrReal"`
				KReal    value.Real `kusto:"kReal"`
				PtrkReal *value.Real
			}{0.0, nil, value.Real{Value: 0.0, Valid: false}, &value.Real{Value: 0.0, Valid: false}},
		},
		{
			desc: "valid String",
			columns: Columns{
				{Type: types.String, Name: "string"},
				{Type: types.String, Name: "ptrString"},
				{Type: types.String, Name: "kString"},
				{Type: types.String, Name: "PtrkString"},
			},
			k: value.String{Value: "hello", Valid: true},
			ptrStruct: &struct {
				String     string       `kusto:"string"`
				PtrString  *string      `kusto:"ptrString"`
				KString    value.String `kusto:"kString"`
				PtrkString *value.String
			}{},
			err: false,
			want: &struct {
				String     string       `kusto:"string"`
				PtrString  *string      `kusto:"ptrString"`
				KString    value.String `kusto:"kString"`
				PtrkString *value.String
			}{"hello", stringPtr("hello"), value.String{Value: "hello", Valid: true}, &value.String{Value: "hello", Valid: true}},
		},
		{
			desc: "non-valid String",
			columns: Columns{
				{Type: types.String, Name: "string"},
				{Type: types.String, Name: "ptrString"},
				{Type: types.String, Name: "kString"},
				{Type: types.String, Name: "PtrkString"},
			},
			k: value.String{Value: "", Valid: false},
			ptrStruct: &struct {
				String     string       `kusto:"string"`
				PtrString  *string      `kusto:"ptrString"`
				KString    value.String `kusto:"kString"`
				PtrkString *value.String
			}{},
			err: false,
			want: &struct {
				String     string       `kusto:"string"`
				PtrString  *string      `kusto:"ptrString"`
				KString    value.String `kusto:"kString"`
				PtrkString *value.String
			}{"", nil, value.String{Value: "", Valid: false}, &value.String{Value: "", Valid: false}},
		},
		{
			desc: "valid Timespan",
			columns: Columns{
				{Type: types.Timespan, Name: "timespan"},
				{Type: types.Timespan, Name: "ptrTimespan"},
				{Type: types.Timespan, Name: "kTimespan"},
				{Type: types.Timespan, Name: "PtrkTimespan"},
			},
			k: value.Timespan{Value: 2 * time.Minute, Valid: true},
			ptrStruct: &struct {
				Timespan     time.Duration  `kusto:"timespan"`
				PtrTimespan  *time.Duration `kusto:"ptrTimespan"`
				KTimespan    value.Timespan `kusto:"kTimespan"`
				PtrkTimespan *value.Timespan
			}{},
			err: false,
			want: &struct {
				Timespan     time.Duration  `kusto:"timespan"`
				PtrTimespan  *time.Duration `kusto:"ptrTimespan"`
				KTimespan    value.Timespan `kusto:"kTimespan"`
				PtrkTimespan *value.Timespan
			}{2 * time.Minute, durationPtr(2 * time.Minute), value.Timespan{Value: 2 * time.Minute, Valid: true}, &value.Timespan{Value: 2 * time.Minute, Valid: true}},
		},
		{
			desc: "non-valid Timespan",
			columns: Columns{
				{Type: types.Timespan, Name: "timespan"},
				{Type: types.Timespan, Name: "ptrTimespan"},
				{Type: types.Timespan, Name: "kTimespan"},
				{Type: types.Timespan, Name: "PtrkTimespan"},
			},
			k: value.Timespan{Value: 0, Valid: false},
			ptrStruct: &struct {
				Timespan     time.Duration  `kusto:"timespan"`
				PtrTimespan  *time.Duration `kusto:"ptrTimespan"`
				KTimespan    value.Timespan `kusto:"kTimespan"`
				PtrkTimespan *value.Timespan
			}{},
			err: false,
			want: &struct {
				Timespan     time.Duration  `kusto:"timespan"`
				PtrTimespan  *time.Duration `kusto:"ptrTimespan"`
				KTimespan    value.Timespan `kusto:"kTimespan"`
				PtrkTimespan *value.Timespan
			}{0, nil, value.Timespan{Value: 0, Valid: false}, &value.Timespan{Value: 0, Valid: false}},
		},
		{
			desc: "valid Decimal",
			columns: Columns{
				{Type: types.Decimal, Name: "decimal"},
				{Type: types.Decimal, Name: "ptrDecimal"},
				{Type: types.Decimal, Name: "kDecimal"},
				{Type: types.Decimal, Name: "PtrkDecimal"},
			},
			k: value.Decimal{Value: "0.1", Valid: true},
			ptrStruct: &struct {
				Decimal     string        `kusto:"decimal"`
				PtrDecimal  *string       `kusto:"ptrDecimal"`
				KDecimal    value.Decimal `kusto:"kDecimal"`
				PtrkDecimal *value.Decimal
			}{},
			err: false,
			want: &struct {
				Decimal     string        `kusto:"decimal"`
				PtrDecimal  *string       `kusto:"ptrDecimal"`
				KDecimal    value.Decimal `kusto:"kDecimal"`
				PtrkDecimal *value.Decimal
			}{"0.1", stringPtr("0.1"), value.Decimal{Value: "0.1", Valid: true}, &value.Decimal{Value: "0.1", Valid: true}},
		},
	}

	for _, test := range tests {
		test := test // Capture
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			fields := newFields(test.columns, reflect.TypeOf(test.ptrStruct))

			ty := reflect.TypeOf(test.ptrStruct)
			v := reflect.ValueOf(test.ptrStruct)
			for _, column := range test.columns {
				err := fields.convert(column, test.k, ty, v)
				if test.err {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			}

			assert.EqualValues(t, test.want, test.ptrStruct)
		})

	}
}

func boolPtr(b bool) *bool {
	return &b
}

func int32Ptr(i int32) *int32 {
	return &i
}
func int64Ptr(i int64) *int64 {
	return &i
}

func float64Ptr(f float64) *float64 {
	return &f
}

func stringPtr(s string) *string {
	return &s
}

func durationPtr(d time.Duration) *time.Duration {
	return &d
}
