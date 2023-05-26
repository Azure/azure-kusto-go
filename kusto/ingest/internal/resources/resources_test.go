package resources

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/data/value"
)

func TestParse(t *testing.T) {
	t.Parallel()

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
		{
			desc:           "success non-public",
			url:            "https://account.table.kusto.chinacloudapi.cn/objectname",
			wantAccount:    "account",
			wantObjectType: "table",
			wantObjectName: "objectname",
		},
		{
			desc:           "success dns zone",
			url:            "https://account.zone1.blob.storage.azure.net/objectname",
			wantAccount:    "account",
			wantObjectType: "blob",
			wantObjectName: "objectname",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got, err := parse(test.url)

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.Equal(t, test.wantAccount, got.Account())
			assert.Equal(t, test.wantObjectType, got.ObjectType())
			assert.Equal(t, test.wantObjectName, got.ObjectName())
			assert.Equal(t, test.url, got.String())
		})
	}
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
	t.Parallel()

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
		test := test
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			manager := &Manager{client: test.fakeMgmt}

			got, err := manager.AuthContext(context.Background())

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.Equal(t, test.want, got)
		})
	}
}

func mustParse(s string) *URI {
	u, err := parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

func TestResources(t *testing.T) {
	t.Parallel()

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
			desc:     "Success",
			fakeMgmt: SuccessfulFakeResources(),
			want: Ingestion{
				Queues:     []*URI{mustParse("https://account.blob.core.windows.net/storageroot1")},
				Containers: []*URI{mustParse("https://account.blob.core.windows.net/storageroot0")},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			manager := &Manager{client: test.fakeMgmt}

			err := manager.fetch(context.Background())

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			got, err := manager.Resources()
			assert.NoError(t, err)

			assert.Equal(t, test.want, got)
		})
	}
}
