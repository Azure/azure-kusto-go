package azkustoingest

import (
	"github.com/Azure/azure-kusto-go/azkustodata"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingest/internal/resources"
	"github.com/stretchr/testify/assert"
)

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
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{db: "test2", table: "test2"})
					return ingestion
				},
			},
		},
		{
			name: "TestSameEndpoint",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(mockClientSame, &Ingestion{db: "test2", table: "test2"})
					return ingestion
				},
			},
		},
		{
			name: "TestDifferentEndpoint",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(secondMockClient, &Ingestion{db: "test2", table: "test2"})
					return ingestion
				},
			},
		},
		{
			name: "TestDifferentAndSameEndpoints",
			clients: []func() *Ingestion{
				func() *Ingestion {
					ingestion, _ := newFromClient(firstMockClient, &Ingestion{db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(mockClientSame, &Ingestion{db: "test", table: "test"})
					return ingestion
				},
				func() *Ingestion {
					ingestion, _ := newFromClient(secondMockClient, &Ingestion{db: "test2", table: "test2"})
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
