package azkustodata

type mockClient struct {
}

func NewMockClient() *mockClient {
	return &mockClient{}
}
