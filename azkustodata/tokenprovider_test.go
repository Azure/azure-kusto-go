package azkustodata

import (
	"context"
	"github.com/stretchr/testify/require"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAcquireTokenErr(t *testing.T) {
	s := newTestServ()
	os.Unsetenv("AZURE_TENANT_ID")
	payload := `{"AzureAD": {"LoginEndpoint": "https://login.microsofdummy.com","LoginMfaRequired": false,"KustoClientAppId": "db662dc1-0cfe-4e1c-a843-19a68e65xxxx","KustoClientRedirectUri": "https://microsoft/dummykustoclient","KustoServiceResourceId": "https://kusto.windows.net","FirstPartyAuthorityUrl": "https://login.microsofdummy.com/f8cdef31-a31e-4b4a-93e4-5f571e9xxxxx"  },  "dSTS": {"CloudEndpointSuffix": "windows.net","DstsRealm": "realm://xxx.windows.net","DstsInstance": "xxx.core.windows.net","KustoDnsHostName": "kusto.windows.net","ServiceName": "kusto"}}`
	provider, err := NewConnectionStringBuilder("https://help.kusto.windows.net").newTokenProvider()
	if err != nil {
		require.NoError(t, err)
	}

	tests := []struct {
		name    string
		wantErr string
		tkp     TokenProvider
	}{
		{
			name: "test_acquiretoken_cred",
			tkp: TokenProvider{
				tokenCred: provider.tokenCred,
			},
			wantErr: "",
		},
		{
			name: "test_acquiretoken_invalid_datasource",
			tkp: TokenProvider{
				tokenCred: provider.tokenCred,
			},
		},
	}
	for _, test := range tests {
		tkp := test.tkp
		s.code = 200
		s.payload = []byte(payload)

		got, token_type, err := tkp.AcquireToken(context.Background())
		assert.NotNil(t, err)
		assert.EqualValues(t, "", got)
		assert.EqualValues(t, "", token_type)
	}

}
