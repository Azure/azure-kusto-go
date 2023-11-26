package table

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
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

	emptyStruct := SomeJSON{
		Name: "",
		ID:   0,
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
			k: value.NewBool(true),
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
			}{true, boolPtr(true), *value.NewBool(true), value.NewBool(true)},
		},
		{
			desc: "non-valid Bool",
			columns: Columns{
				{Type: types.Bool, Name: "bool"},
				{Type: types.Bool, Name: "ptrbool"},
				{Type: types.Bool, Name: "kBool"},
				{Type: types.Bool, Name: "PtrkBool"},
			},
			k: value.NewNullBool(),
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
			}{false, nil, *value.NewNullBool(), value.NewNullBool()},
		},
		{
			desc: "valid DateTime",
			columns: Columns{
				{Type: types.DateTime, Name: "time"},
				{Type: types.DateTime, Name: "ptrtime"},
				{Type: types.DateTime, Name: "dateTime"},
				{Type: types.DateTime, Name: "PtrDateTime"},
			},
			k: value.NewDateTime(now),
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
			}{now, &now, *value.NewDateTime(now), value.NewDateTime(now)},
		},
		{
			desc: "non-valid DateTime",
			columns: Columns{
				{Type: types.DateTime, Name: "time"},
				{Type: types.DateTime, Name: "ptrtime"},
				{Type: types.DateTime, Name: "dateTime"},
				{Type: types.DateTime, Name: "PtrDateTime"},
			},
			k: value.NewNullDateTime(),
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
			}{time.Time{}, nil, *value.NewNullDateTime(), value.NewNullDateTime()},
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
			k: value.NewDynamic(myJSON),
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
				*value.NewDynamic(myJSON),
				value.NewDynamic(myJSON),
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
			k: value.NewDynamic(myJSONArray),
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
				*value.NewDynamic(myJSONArray),
				value.NewDynamic(myJSONArray),
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
			k: value.NewNullDynamic(),
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
				emptyStruct,
				nil,
				"",
				nil,
				nil,
				nil,
				*value.NewNullDynamic(),
				nil,
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
			k: value.NewNullDynamic(),
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
				"",
				nil,
				nil,
				nil,
				*value.NewNullDynamic(),
				nil,
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
			k: value.NewGUID(guid),
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
			}{guid, &guid, *value.NewGUID(guid), value.NewGUID(guid)},
		},
		{
			desc: "non-valid GUID",
			columns: Columns{
				{Type: types.GUID, Name: "guid"},
				{Type: types.GUID, Name: "ptrguid"},
				{Type: types.GUID, Name: "kGUID"},
				{Type: types.GUID, Name: "PtrKGUID"},
			},
			k: value.NewNullGUID(),
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
			}{uuid.UUID{}, nil, *value.NewNullGUID(), value.NewNullGUID()},
		},
		{
			desc: "valid Int",
			columns: Columns{
				{Type: types.Int, Name: "int"},
				{Type: types.Int, Name: "ptrint"},
				{Type: types.Int, Name: "kInt"},
				{Type: types.Int, Name: "PtrkInt"},
			},
			k: value.NewInt(1),
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
			}{1, int32Ptr(1), *value.NewInt(1), value.NewInt(1)},
		},
		{
			desc: "non-valid Int",
			columns: Columns{
				{Type: types.Int, Name: "int"},
				{Type: types.Int, Name: "ptrint"},
				{Type: types.Int, Name: "kInt"},
				{Type: types.Int, Name: "PtrkInt"},
			},
			k: value.NewNullInt(),
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
			}{0, nil, *value.NewNullInt(), value.NewNullInt()},
		},
		{
			desc: "valid Long",
			columns: Columns{
				{Type: types.Long, Name: "long"},
				{Type: types.Long, Name: "ptrLong"},
				{Type: types.Long, Name: "kLong"},
				{Type: types.Long, Name: "PtrkLong"},
			},
			k: value.NewLong(1),
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
			}{1, int64Ptr(1), *value.NewLong(1), value.NewLong(1)},
		},
		{
			desc: "non-valid Long",
			columns: Columns{
				{Type: types.Long, Name: "long"},
				{Type: types.Long, Name: "ptrLong"},
				{Type: types.Long, Name: "kLong"},
				{Type: types.Long, Name: "PtrkLong"},
			},
			k: value.NewNullLong(),
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
			}{0, nil, *value.NewNullLong(), value.NewNullLong()},
		},
		{
			desc: "valid real",
			columns: Columns{
				{Type: types.Real, Name: "real"},
				{Type: types.Real, Name: "ptrReal"},
				{Type: types.Real, Name: "kReal"},
				{Type: types.Real, Name: "PtrkReal"},
			},
			k: value.NewReal(3.2),
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
			}{3.2, float64Ptr(3.2), *value.NewReal(3.2), value.NewReal(3.2)},
		},
		{
			desc: "non-valid real",
			columns: Columns{
				{Type: types.Real, Name: "real"},
				{Type: types.Real, Name: "ptrReal"},
				{Type: types.Real, Name: "kReal"},
				{Type: types.Real, Name: "PtrkReal"},
			},
			k: value.NewNullReal(),
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
			}{0.0, nil, *value.NewNullReal(), value.NewNullReal()},
		},
		{
			desc: "valid String",
			columns: Columns{
				{Type: types.String, Name: "string"},
				{Type: types.String, Name: "ptrString"},
				{Type: types.String, Name: "kString"},
				{Type: types.String, Name: "PtrkString"},
			},
			k: value.NewString("hello"),
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
			}{"hello", stringPtr("hello"), *value.NewString("hello"), value.NewString("hello")},
		},
		{
			desc: "non-valid String",
			columns: Columns{
				{Type: types.String, Name: "string"},
				{Type: types.String, Name: "ptrString"},
				{Type: types.String, Name: "kString"},
				{Type: types.String, Name: "PtrkString"},
			},
			k: value.NewNullString(),
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
			}{"", nil, *value.NewNullString(), value.NewNullString()},
		},
		{
			desc: "valid Timespan",
			columns: Columns{
				{Type: types.Timespan, Name: "timespan"},
				{Type: types.Timespan, Name: "ptrTimespan"},
				{Type: types.Timespan, Name: "kTimespan"},
				{Type: types.Timespan, Name: "PtrkTimespan"},
			},
			k: value.NewTimespan(2 * time.Minute),
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
			}{2 * time.Minute, durationPtr(2 * time.Minute), *value.NewTimespan(2 * time.Minute), value.NewTimespan(2 * time.Minute)},
		},
		{
			desc: "non-valid Timespan",
			columns: Columns{
				{Type: types.Timespan, Name: "timespan"},
				{Type: types.Timespan, Name: "ptrTimespan"},
				{Type: types.Timespan, Name: "kTimespan"},
				{Type: types.Timespan, Name: "PtrkTimespan"},
			},
			k: value.NewNullTimespan(),
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
			}{0, nil, *value.NewNullTimespan(), value.NewNullTimespan()},
		},
		{
			desc: "valid Decimal",
			columns: Columns{
				{Type: types.Decimal, Name: "decimal"},
				{Type: types.Decimal, Name: "ptrDecimal"},
				{Type: types.Decimal, Name: "kDecimal"},
				{Type: types.Decimal, Name: "PtrkDecimal"},
			},
			k: value.DecimalFromString("0.1"),
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
			}{"0.1", stringPtr("0.1"), *value.DecimalFromString("0.1"), value.DecimalFromString("0.1")},
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
				err := fields.convert(column, test.k, v)
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
