package ingest

import (
	"context"
	"testing"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/data/types"
	"github.com/Azure/azure-kusto-go/kusto/ingest/internal/resources"
	"github.com/stretchr/testify/assert"
)

type mockClient struct {
	endpoint string
	auth     kusto.Authorization
	onMgmt   func(ctx context.Context, db string, query kusto.Stmt, options ...kusto.MgmtOption) (*kusto.RowIterator, error)
}

func (m mockClient) Auth() kusto.Authorization {
	return m.auth
}

func (m mockClient) Endpoint() string {
	return m.endpoint
}

func (m mockClient) Query(context.Context, string, kusto.Stmt, ...kusto.QueryOption) (*kusto.RowIterator, error) {
	panic("not implemented")
}

func (m mockClient) Mgmt(ctx context.Context, db string, query kusto.Stmt, options ...kusto.MgmtOption) (*kusto.RowIterator, error) {
	if m.onMgmt != nil {
		rows, err := m.onMgmt(ctx, db, query, options...)
		if err != nil || rows != nil {
			return rows, err
		}
	}

	rows, err := kusto.NewMockRows(table.Columns{
		{
			Name: "ResourceTypeName",
			Type: types.String,
		},
		{
			Name: "StorageRoot",
			Type: types.String,
		},
	})
	if err != nil {
		return nil, err
	}
	iter := &kusto.RowIterator{}
	err = iter.Mock(rows)
	if err != nil {
		return nil, err
	}

	return iter, nil
}

func TestIngestion(t *testing.T) {

	firstMockClient := mockClient{
		endpoint: "https://test.kusto.windows.net",
		auth:     kusto.Authorization{},
	}
	mockClientSame := mockClient{
		endpoint: "https://test.kusto.windows.net",
		auth:     kusto.Authorization{},
	}
	secondMockClient := mockClient{
		endpoint: "https://test2.kusto.windows.net",
		auth:     kusto.Authorization{},
	}

	tests := []struct {
		name    string
		clients []func() *Ingestion
	}{
		{
			name: "TestSameClient",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := New(firstMockClient, "test", "test")
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := New(firstMockClient, "test2", "test2")
					return ingestion
				},
			},
		},
		{
			name: "TestSameEndpoint",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := New(firstMockClient, "test", "test")
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := New(mockClientSame, "test2", "test2")
					return ingestion
				},
			},
		},
		{
			name: "TestDifferentEndpoint",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := New(firstMockClient, "test", "test")
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := New(secondMockClient, "test2", "test2")
					return ingestion
				},
			},
		},
		{
			name: "TestDifferentAndSameEndpoints",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := New(firstMockClient, "test", "test")
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := New(mockClientSame, "test", "test")
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := New(secondMockClient, "test2", "test2")
					return ingestion
				},
			},
		},
	}

	for _, test := range tests {
		test := test // Capture
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mgrMap := make(map[*resources.Manager]bool)
			for _, client := range test.clients {
				mgr := client().mgr
				mgrMap[mgr] = true
			}

			assert.Equalf(t, len(mgrMap), len(test.clients), "Got duplicated managers, want %d managers, got %d", len(test.clients), len(mgrMap))
		})
	}
}
