package azkustodata

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata/types"

	"github.com/google/uuid"
	"github.com/kylelemons/godebug/pretty"
)

var now = time.Now()
var guid = uuid.New()

type SomeJSON struct {
	Name string
	ID   int
}

func TestFieldsConvert(t *testing.T) {
	myStruct := SomeJSON{
		Name: "Adam",
		ID:   1,
	}
	myJSON, err := json.Marshal(myStruct)
	if err != nil {
		panic(err)
	}

	tests := []struct {
		desc      string
		columns   Columns
		k         types.KustoValue
		ptrStruct interface{}
		err       bool
		want      interface{}
	}{
		{
			desc: "valid Bool",
			columns: Columns{
				{ColumnType: CTBool, ColumnName: "bool"},
				{ColumnType: CTBool, ColumnName: "ptrbool"},
				{ColumnType: CTBool, ColumnName: "kBool"},
				{ColumnType: CTBool, ColumnName: "PtrkBool"},
			},
			k: types.Bool{Value: true, Valid: true},
			ptrStruct: &struct {
				Bool     bool       `kusto:"bool"`
				PtrBool  *bool      `kusto:"ptrbool"`
				KBool    types.Bool `kusto:"kBool"`
				PtrkBool *types.Bool
			}{},
			err: false,
			want: &struct {
				Bool     bool       `kusto:"bool"`
				PtrBool  *bool      `kusto:"ptrbool"`
				KBool    types.Bool `kusto:"kBool"`
				PtrkBool *types.Bool
			}{true, boolPtr(true), types.Bool{Value: true, Valid: true}, &types.Bool{Value: true, Valid: true}},
		},
		{
			desc: "non-valid Bool",
			columns: Columns{
				{ColumnType: CTBool, ColumnName: "bool"},
				{ColumnType: CTBool, ColumnName: "ptrbool"},
				{ColumnType: CTBool, ColumnName: "kBool"},
				{ColumnType: CTBool, ColumnName: "PtrkBool"},
			},
			k: types.Bool{Value: false, Valid: false},
			ptrStruct: &struct {
				Bool     bool       `kusto:"bool"`
				PtrBool  *bool      `kusto:"ptrbool"`
				KBool    types.Bool `kusto:"kBool"`
				PtrkBool *types.Bool
			}{},
			err: false,
			want: &struct {
				Bool     bool       `kusto:"bool"`
				PtrBool  *bool      `kusto:"ptrbool"`
				KBool    types.Bool `kusto:"kBool"`
				PtrkBool *types.Bool
			}{false, nil, types.Bool{Value: false, Valid: false}, &types.Bool{Value: false, Valid: false}},
		},
		{
			desc: "valid DateTime",
			columns: Columns{
				{ColumnType: CTDateTime, ColumnName: "time"},
				{ColumnType: CTDateTime, ColumnName: "ptrtime"},
				{ColumnType: CTDateTime, ColumnName: "dateTime"},
				{ColumnType: CTDateTime, ColumnName: "PtrDateTime"},
			},
			k: types.DateTime{Value: now, Valid: true},
			ptrStruct: &struct {
				Time        time.Time      `kusto:"time"`
				PtrTime     *time.Time     `kusto:"ptrtime"`
				DateTime    types.DateTime `kusto:"dateTime"`
				PtrDateTime *types.DateTime
			}{},
			err: false,
			want: &struct {
				Time        time.Time      `kusto:"time"`
				PtrTime     *time.Time     `kusto:"ptrtime"`
				DateTime    types.DateTime `kusto:"dateTime"`
				PtrDateTime *types.DateTime
			}{now, &now, types.DateTime{Value: now, Valid: true}, &types.DateTime{Value: now, Valid: true}},
		},
		{
			desc: "non-valid DateTime",
			columns: Columns{
				{ColumnType: CTDateTime, ColumnName: "time"},
				{ColumnType: CTDateTime, ColumnName: "ptrtime"},
				{ColumnType: CTDateTime, ColumnName: "dateTime"},
				{ColumnType: CTDateTime, ColumnName: "PtrDateTime"},
			},
			k: types.DateTime{Value: time.Time{}, Valid: false},
			ptrStruct: &struct {
				Time        time.Time      `kusto:"time"`
				PtrTime     *time.Time     `kusto:"ptrtime"`
				DateTime    types.DateTime `kusto:"dateTime"`
				PtrDateTime *types.DateTime
			}{},
			err: false,
			want: &struct {
				Time        time.Time      `kusto:"time"`
				PtrTime     *time.Time     `kusto:"ptrtime"`
				DateTime    types.DateTime `kusto:"dateTime"`
				PtrDateTime *types.DateTime
			}{time.Time{}, nil, types.DateTime{Value: time.Time{}, Valid: false}, &types.DateTime{Value: time.Time{}, Valid: false}},
		},
		{
			desc: "valid Dynamic",
			columns: Columns{
				{ColumnType: CTDynamic, ColumnName: "Struct"},
				{ColumnType: CTDynamic, ColumnName: "PtrStruct"},
				{ColumnType: CTDynamic, ColumnName: "Map"},
				{ColumnType: CTDynamic, ColumnName: "PtrMap"},
				{ColumnType: CTDynamic, ColumnName: "Dynamic"},
				{ColumnType: CTDynamic, ColumnName: "PtrDynamic"},
			},
			k: types.Dynamic{Value: string(myJSON), Valid: true},
			ptrStruct: &struct {
				Struct     SomeJSON
				PtrStruct  *SomeJSON
				Map        map[string]interface{}
				PtrMap     *map[string]interface{}
				Dynamic    types.Dynamic
				PtrDynamic *types.Dynamic
			}{},
			err: false,
			want: &struct {
				Struct     SomeJSON
				PtrStruct  *SomeJSON
				Map        map[string]interface{}
				PtrMap     *map[string]interface{}
				Dynamic    types.Dynamic
				PtrDynamic *types.Dynamic
			}{
				myStruct,
				&myStruct,
				map[string]interface{}{
					"Name": "Adam",
					"ID":   1,
				},
				&map[string]interface{}{
					"Name": "Adam",
					"ID":   1,
				},
				types.Dynamic{Value: string(myJSON), Valid: true},
				&types.Dynamic{Value: string(myJSON), Valid: true},
			},
		},
		{
			desc: "valid GUID",
			columns: Columns{
				{ColumnType: CTGUID, ColumnName: "guid"},
				{ColumnType: CTGUID, ColumnName: "ptrguid"},
				{ColumnType: CTGUID, ColumnName: "kGUID"},
				{ColumnType: CTGUID, ColumnName: "PtrKGUID"},
			},
			k: types.GUID{Value: guid, Valid: true},
			ptrStruct: &struct {
				GUID     uuid.UUID  `kusto:"guid"`
				PtrGUID  *uuid.UUID `kusto:"ptrguid"`
				KGUID    types.GUID `kusto:"kGUID"`
				PtrKGUID *types.GUID
			}{},
			err: false,
			want: &struct {
				GUID     uuid.UUID  `kusto:"guid"`
				PtrGUID  *uuid.UUID `kusto:"ptrguid"`
				KGUID    types.GUID `kusto:"kGUID"`
				PtrKGUID *types.GUID
			}{guid, &guid, types.GUID{Value: guid, Valid: true}, &types.GUID{Value: guid, Valid: true}},
		},
		{
			desc: "non-valid GUID",
			columns: Columns{
				{ColumnType: CTGUID, ColumnName: "guid"},
				{ColumnType: CTGUID, ColumnName: "ptrguid"},
				{ColumnType: CTGUID, ColumnName: "kGUID"},
				{ColumnType: CTGUID, ColumnName: "PtrKGUID"},
			},
			k: types.GUID{Value: uuid.UUID{}, Valid: false},
			ptrStruct: &struct {
				GUID     uuid.UUID  `kusto:"guid"`
				PtrGUID  *uuid.UUID `kusto:"ptrguid"`
				KGUID    types.GUID `kusto:"kGUID"`
				PtrKGUID *types.GUID
			}{},
			err: false,
			want: &struct {
				GUID     uuid.UUID  `kusto:"guid"`
				PtrGUID  *uuid.UUID `kusto:"ptrguid"`
				KGUID    types.GUID `kusto:"kGUID"`
				PtrKGUID *types.GUID
			}{uuid.UUID{}, nil, types.GUID{Value: uuid.UUID{}, Valid: false}, &types.GUID{Value: uuid.UUID{}, Valid: false}},
		},
		{
			desc: "valid Int",
			columns: Columns{
				{ColumnType: CTInt, ColumnName: "int"},
				{ColumnType: CTInt, ColumnName: "ptrint"},
				{ColumnType: CTInt, ColumnName: "kInt"},
				{ColumnType: CTInt, ColumnName: "PtrkInt"},
			},
			k: types.Int{Value: 1, Valid: true},
			ptrStruct: &struct {
				Int     int32     `kusto:"int"`
				PtrInt  *int32    `kusto:"ptrint"`
				KInt    types.Int `kusto:"kInt"`
				PtrkInt *types.Int
			}{},
			err: false,
			want: &struct {
				Int     int32     `kusto:"int"`
				PtrInt  *int32    `kusto:"ptrint"`
				KInt    types.Int `kusto:"kInt"`
				PtrkInt *types.Int
			}{1, int32Ptr(1), types.Int{Value: 1, Valid: true}, &types.Int{Value: 1, Valid: true}},
		},
		{
			desc: "non-valid Int",
			columns: Columns{
				{ColumnType: CTInt, ColumnName: "int"},
				{ColumnType: CTInt, ColumnName: "ptrint"},
				{ColumnType: CTInt, ColumnName: "kInt"},
				{ColumnType: CTInt, ColumnName: "PtrkInt"},
			},
			k: types.Int{Value: 0, Valid: false},
			ptrStruct: &struct {
				Int     int32     `kusto:"int"`
				PtrInt  *int32    `kusto:"ptrint"`
				KInt    types.Int `kusto:"kInt"`
				PtrkInt *types.Int
			}{},
			err: false,
			want: &struct {
				Int     int32     `kusto:"int"`
				PtrInt  *int32    `kusto:"ptrint"`
				KInt    types.Int `kusto:"kInt"`
				PtrkInt *types.Int
			}{0, nil, types.Int{Value: 0, Valid: false}, &types.Int{Value: 0, Valid: false}},
		},
		{
			desc: "valid Long",
			columns: Columns{
				{ColumnType: CTLong, ColumnName: "long"},
				{ColumnType: CTLong, ColumnName: "ptrLong"},
				{ColumnType: CTLong, ColumnName: "kLong"},
				{ColumnType: CTLong, ColumnName: "PtrkLong"},
			},
			k: types.Long{Value: 1, Valid: true},
			ptrStruct: &struct {
				Long     int64      `kusto:"long"`
				PtrLong  *int64     `kusto:"ptrLong"`
				KLong    types.Long `kusto:"kLong"`
				PtrkLong *types.Long
			}{},
			err: false,
			want: &struct {
				Long     int64      `kusto:"long"`
				PtrLong  *int64     `kusto:"ptrLong"`
				KLong    types.Long `kusto:"kLong"`
				PtrkLong *types.Long
			}{1, int64Ptr(1), types.Long{Value: 1, Valid: true}, &types.Long{Value: 1, Valid: true}},
		},
		{
			desc: "non-valid Long",
			columns: Columns{
				{ColumnType: CTLong, ColumnName: "long"},
				{ColumnType: CTLong, ColumnName: "ptrLong"},
				{ColumnType: CTLong, ColumnName: "kLong"},
				{ColumnType: CTLong, ColumnName: "PtrkLong"},
			},
			k: types.Long{Value: 0, Valid: false},
			ptrStruct: &struct {
				Long     int64      `kusto:"long"`
				PtrLong  *int64     `kusto:"ptrLong"`
				KLong    types.Long `kusto:"kLong"`
				PtrkLong *types.Long
			}{},
			err: false,
			want: &struct {
				Long     int64      `kusto:"long"`
				PtrLong  *int64     `kusto:"ptrLong"`
				KLong    types.Long `kusto:"kLong"`
				PtrkLong *types.Long
			}{0, nil, types.Long{Value: 0, Valid: false}, &types.Long{Value: 0, Valid: false}},
		},
		{
			desc: "valid real",
			columns: Columns{
				{ColumnType: CTReal, ColumnName: "real"},
				{ColumnType: CTReal, ColumnName: "ptrReal"},
				{ColumnType: CTReal, ColumnName: "kReal"},
				{ColumnType: CTReal, ColumnName: "PtrkReal"},
			},
			k: types.Real{Value: 3.2, Valid: true},
			ptrStruct: &struct {
				Real     float64    `kusto:"real"`
				PtrReal  *float64   `kusto:"ptrReal"`
				KReal    types.Real `kusto:"kReal"`
				PtrkReal *types.Real
			}{},
			err: false,
			want: &struct {
				Real     float64    `kusto:"real"`
				PtrReal  *float64   `kusto:"ptrReal"`
				KReal    types.Real `kusto:"kReal"`
				PtrkReal *types.Real
			}{3.2, float64Ptr(3.2), types.Real{Value: 3.2, Valid: true}, &types.Real{Value: 3.2, Valid: true}},
		},
		{
			desc: "non-valid real",
			columns: Columns{
				{ColumnType: CTReal, ColumnName: "real"},
				{ColumnType: CTReal, ColumnName: "ptrReal"},
				{ColumnType: CTReal, ColumnName: "kReal"},
				{ColumnType: CTReal, ColumnName: "PtrkReal"},
			},
			k: types.Real{Value: 0.0, Valid: false},
			ptrStruct: &struct {
				Real     float64    `kusto:"real"`
				PtrReal  *float64   `kusto:"ptrReal"`
				KReal    types.Real `kusto:"kReal"`
				PtrkReal *types.Real
			}{},
			err: false,
			want: &struct {
				Real     float64    `kusto:"real"`
				PtrReal  *float64   `kusto:"ptrReal"`
				KReal    types.Real `kusto:"kReal"`
				PtrkReal *types.Real
			}{0.0, nil, types.Real{Value: 0.0, Valid: false}, &types.Real{Value: 0.0, Valid: false}},
		},
		{
			desc: "valid String",
			columns: Columns{
				{ColumnType: CTString, ColumnName: "string"},
				{ColumnType: CTString, ColumnName: "ptrString"},
				{ColumnType: CTString, ColumnName: "kString"},
				{ColumnType: CTString, ColumnName: "PtrkString"},
			},
			k: types.String{Value: "hello", Valid: true},
			ptrStruct: &struct {
				String     string       `kusto:"string"`
				PtrString  *string      `kusto:"ptrString"`
				KString    types.String `kusto:"kString"`
				PtrkString *types.String
			}{},
			err: false,
			want: &struct {
				String     string       `kusto:"string"`
				PtrString  *string      `kusto:"ptrString"`
				KString    types.String `kusto:"kString"`
				PtrkString *types.String
			}{"hello", stringPtr("hello"), types.String{Value: "hello", Valid: true}, &types.String{Value: "hello", Valid: true}},
		},
		{
			desc: "non-valid String",
			columns: Columns{
				{ColumnType: CTString, ColumnName: "string"},
				{ColumnType: CTString, ColumnName: "ptrString"},
				{ColumnType: CTString, ColumnName: "kString"},
				{ColumnType: CTString, ColumnName: "PtrkString"},
			},
			k: types.String{Value: "", Valid: false},
			ptrStruct: &struct {
				String     string       `kusto:"string"`
				PtrString  *string      `kusto:"ptrString"`
				KString    types.String `kusto:"kString"`
				PtrkString *types.String
			}{},
			err: false,
			want: &struct {
				String     string       `kusto:"string"`
				PtrString  *string      `kusto:"ptrString"`
				KString    types.String `kusto:"kString"`
				PtrkString *types.String
			}{"", nil, types.String{Value: "", Valid: false}, &types.String{Value: "", Valid: false}},
		},
	}

	for _, test := range tests {
		fields := newFields(test.columns, reflect.TypeOf(test.ptrStruct))

		ty := reflect.TypeOf(test.ptrStruct)
		v := reflect.ValueOf(test.ptrStruct)
		for _, column := range test.columns {
			err = fields.convert(column, test.k, ty, v)
			switch {
			case err == nil && test.err:
				t.Errorf("TestFieldsConvert(%s): got err == nil, want err != nil", test.desc)
				continue
			case err != nil && !test.err:
				t.Errorf("TestFieldsConvert(%s): got err == %s, want err == nil", test.desc, err)
				continue
			case err != nil:
				continue
			}
		}

		if diff := pretty.Compare(test.want, test.ptrStruct); diff != "" {
			t.Errorf("TestFieldsConvert(%s): -want/+got:\n%s", test.desc, diff)
		}
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
