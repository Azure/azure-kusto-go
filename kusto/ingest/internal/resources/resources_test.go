package resources

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/kylelemons/godebug/pretty"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/data/value"
)

func TestParse(t *testing.T) {
	tests := []struct {
		desc           string
		url            string
		err            bool
		wantAccount    string
		wantObjectType string
		wantObjectName string
	}{
		{
			desc: "account is missing, but has leading dot",
			url:  "https://.queue.core.windows.net/objectname",
			err:  true,
		},
		{
			desc: "account is missing",
			url:  "https://queue.core.windows.net/objectname",
			err:  true,
		},
		{
			desc: "invalid object type",
			url:  "https://account.invalid.core.windows.net/objectname",
			err:  true,
		},
		{
			desc: "invalid domain",
			url:  "https://account.blob.core.invalid.net/objectname",
			err:  true,
		},
		{
			desc: "no object name provided",
			url:  "https://account.invalid.core.windows.net/",
			err:  true,
		},
		{
			desc: "bad scheme",
			url:  "http://account.table.core.windows.net/objectname",
			err:  true,
		},
		{
			desc:           "success",
			url:            "https://account.table.core.windows.net/objectname",
			wantAccount:    "account",
			wantObjectType: "table",
			wantObjectName: "objectname",
		},
	}

	for _, test := range tests {
		got, err := parse(test.url)
		switch {
		case err == nil && test.err:
			t.Errorf("TestParse(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestParse(%s): got err == %s, want err != nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if got.Account() != test.wantAccount {
			t.Errorf("TestParse(%s): URI.Account(): got %s, want %s", test.desc, got.Account(), test.wantAccount)
		}
		if got.ObjectType() != test.wantObjectType {
			t.Errorf("TestParse(%s): URI.ObjectType(): got %s, want %s", test.desc, got.ObjectType(), test.wantObjectType)
		}
		if got.ObjectName() != test.wantObjectName {
			t.Errorf("TestParse(%s): URI.ObjectName(): got %s, want %s", test.desc, got.ObjectName(), test.wantObjectName)
		}
		if got.String() != test.url {
			t.Errorf("TestParse(%s): String(): got %s, want %s", test.desc, got.String(), test.url)
		}
	}
}

type FakeMgmt struct {
	mock       *kusto.MockRows
	DBEqual    string
	QueryEqual string
	mgmtErr    bool
}

func NewFakeMgmt(columns table.Columns, rows []value.Values, setErr bool) *FakeMgmt {
	mock, err := kusto.NewMockRows(columns)
	if err != nil {
		panic(err)
	}

	for _, row := range rows {
		mock.Row(row)
	}

	if setErr {
		mock.Error(errors.New("some error"))
	}

	return &FakeMgmt{
		mock: mock,
	}
}

func (f *FakeMgmt) SetDBEquals(s string) *FakeMgmt {
	f.DBEqual = s
	return f
}

func (f *FakeMgmt) SetQueryEquals(s string) *FakeMgmt {
	f.DBEqual = s
	return f
}

func (f *FakeMgmt) SetMgmtErr() *FakeMgmt {
	f.mgmtErr = true
	return f
}

func (f *FakeMgmt) Mgmt(ctx context.Context, db string, query kusto.Stmt, options ...kusto.QueryOption) (*kusto.RowIterator, error) {
	if f.DBEqual != "" {
		if db != f.DBEqual {
			panic(fmt.Sprintf("expected db to be %q, was %q", f.DBEqual, db))
		}
	}
	if f.QueryEqual != "" {
		if query.String() != f.QueryEqual {
			panic(fmt.Sprintf("expected query to be %q, was %q", f.QueryEqual, db))
		}
	}
	if f.mgmtErr {
		return nil, fmt.Errorf("some mgmt error")
	}
	iter := &kusto.RowIterator{}
	if err := iter.Mock(f.mock); err != nil {
		panic(err)
	}
	return iter, nil
}

func FakeAuthContext(rows []value.Values, setErr bool) *FakeMgmt {
	cols := table.Columns{
		{
			Name: "AuthorizationContext",
			Type: types.String,
		},
	}

	fm := NewFakeMgmt(cols, rows, setErr)
	return fm
}

func TestAuthContext(t *testing.T) {
	tests := []struct {
		desc     string
		fakeMgmt *FakeMgmt
		err      bool
		want     string
	}{
		{
			desc: "Mgmt returns an error",
			fakeMgmt: FakeAuthContext(
				[]value.Values{
					{
						value.String{
							Valid: true,
							Value: "authtoken",
						},
					},
				},
				false,
			).SetMgmtErr(),
			err: true,
		},
		{
			desc: "Returned two rows, only allowed one",
			fakeMgmt: FakeAuthContext(
				[]value.Values{
					{
						value.String{
							Valid: true,
							Value: "authtoken",
						},
					},
					{
						value.String{
							Valid: true,
							Value: "authtoken2",
						},
					},
				},
				false,
			),
			err: true,
		},
		{
			desc: "Success",
			fakeMgmt: FakeAuthContext(
				[]value.Values{
					{
						value.String{
							Valid: true,
							Value: "authtoken",
						},
					},
				},
				false,
			),
			want: "authtoken",
		},
	}

	for _, test := range tests {
		manager := &Manager{client: test.fakeMgmt}

		got, err := manager.AuthContext(context.Background())
		switch {
		case err == nil && test.err:
			t.Errorf("TestAuthContext(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestAuthContext(%s): got err == %s, want err != nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		if got != test.want {
			t.Errorf("TestAuthContext(%s): got %s, want %s", test.desc, got, test.want)
		}
	}
}

func FakeResources(rows []value.Values, setErr bool) *FakeMgmt {
	cols := table.Columns{
		{
			Name: "ResourceTypeName",
			Type: types.String,
		},
		{
			Name: "StorageRoot",
			Type: types.String,
		},
	}

	fm := NewFakeMgmt(cols, rows, setErr)
	return fm
}

func mustParse(s string) *URI {
	u, err := parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

func TestResources(t *testing.T) {
	tests := []struct {
		desc     string
		fakeMgmt *FakeMgmt
		err      bool
		want     Ingestion
	}{
		{
			desc: "Mgmt returns an error",
			fakeMgmt: FakeResources(
				[]value.Values{},
				false,
			).SetMgmtErr(),
			err: true,
		},
		{
			desc: "Bad StorageRoot value",
			fakeMgmt: FakeResources(
				[]value.Values{
					{
						value.String{
							Valid: true,
							Value: "TempStorage",
						},
						value.String{
							Valid: true,
							Value: "https://.blob.core.windows.net/storageroot",
						},
					},
				},
				false,
			),
			err: true,
		},
		{
			desc: "Success",
			fakeMgmt: FakeResources(
				[]value.Values{
					{
						value.String{
							Valid: true,
							Value: "TempStorage",
						},
						value.String{
							Valid: true,
							Value: "https://account.blob.core.windows.net/storageroot0",
						},
					},
					{
						value.String{
							Valid: true,
							Value: "SecuredReadyForAggregationQueue",
						},
						value.String{
							Valid: true,
							Value: "https://account.blob.core.windows.net/storageroot1",
						},
					},
				},
				false,
			),
			want: Ingestion{
				Queues:     []*URI{mustParse("https://account.blob.core.windows.net/storageroot1")},
				Containers: []*URI{mustParse("https://account.blob.core.windows.net/storageroot0")},
			},
		},
	}

	for _, test := range tests {
		manager := &Manager{client: test.fakeMgmt}

		err := manager.fetch(context.Background())

		switch {
		case err == nil && test.err:
			t.Errorf("TestResources(%s): got err == nil, want err != nil", test.desc)
			continue
		case err != nil && !test.err:
			t.Errorf("TestResources(%s): got err == %s, want err != nil", test.desc, err)
			continue
		case err != nil:
			continue
		}

		got, err := manager.Resources()
		if err != nil {
			panic(err)
		}

		if diff := pretty.Compare(test.want, got); diff != "" {
			t.Errorf("TestResources(%s): -want/+got:\n%s", test.desc, diff)
		}
	}
}
