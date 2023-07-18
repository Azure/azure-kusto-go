package azkustoingest

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"net/http"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustodata/table"
	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustoingest/internal/resources"
	"github.com/stretchr/testify/assert"
)

type mockClient struct {
	endpoint string
	auth     azkustodata.Authorization
	onMgmt   func(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.MgmtOption) (*azkustodata.RowIterator, error)
}

func (m mockClient) ClientDetails() *azkustodata.ClientDetails {
	return azkustodata.NewClientDetails("test", "test")
}
func (m mockClient) HttpClient() *http.Client {
	return &http.Client{}
}

func (m mockClient) Close() error {
	return nil
}

func (m mockClient) Auth() azkustodata.Authorization {
	return m.auth
}

func (m mockClient) Endpoint() string {
	return m.endpoint
}

func (m mockClient) Query(context.Context, string, azkustodata.Statement, ...azkustodata.QueryOption) (*azkustodata.RowIterator, error) {
	panic("not implemented")
}

func (m mockClient) Mgmt(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.MgmtOption) (*azkustodata.RowIterator, error) {
	if m.onMgmt != nil {
		rows, err := m.onMgmt(ctx, db, query, options...)
		if err != nil || rows != nil {
			return rows, err
		}
	}

	rows, err := azkustodata.NewMockRows(table.Columns{
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
	iter := &azkustodata.RowIterator{}
	err = iter.Mock(rows)
	if err != nil {
		return nil, err
	}

	return iter, nil
}

func TestIngestion(t *testing.T) {

	firstMockClient := mockClient{
		endpoint: "https://test.kusto.windows.net",
		auth:     azkustodata.Authorization{},
	}
	mockClientSame := mockClient{
		endpoint: "https://test.kusto.windows.net",
		auth:     azkustodata.Authorization{},
	}
	secondMockClient := mockClient{
		endpoint: "https://test2.kusto.windows.net",
		auth:     azkustodata.Authorization{},
	}

	tests := []struct {
		name    string
		clients []func() *Ingestion
	}{
		{
			name: "TestSameClient",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{ db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{ db: "test2", table: "test2"})
					return ingestion
				},
			},
		},
		{
			name: "TestSameEndpoint",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{ db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(mockClientSame, &Ingestion{ db: "test2", table: "test2"})
					return ingestion
				},
			},
		},
		{
			name: "TestDifferentEndpoint",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{ db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(secondMockClient, &Ingestion{ db: "test2", table: "test2"})
					return ingestion
				},
			},
		},
		{
			name: "TestDifferentAndSameEndpoints",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{ db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(mockClientSame, &Ingestion{ db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(secondMockClient, &Ingestion{ db: "test2", table: "test2"})
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
