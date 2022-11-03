package kusto

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAcquireTokenErr(t *testing.T) {
	s := newTestServ()
	os.Unsetenv("AZURE_TENANT_ID")
	payload := `{"AzureAD": {"LoginEndpoint": "https://login.microsofdummy.com","LoginMfaRequired": false,"KustoClientAppId": "db662dc1-0cfe-4e1c-a843-19a68e65xxxx","KustoClientRedirectUri": "https://microsoft/dummykustoclient","KustoServiceResourceId": "https://kusto.windows.net","FirstPartyAuthorityUrl": "https://login.microsofdummy.com/f8cdef31-a31e-4b4a-93e4-5f571e9xxxxx"  },  "dSTS": {"CloudEndpointSuffix": "windows.net","DstsRealm": "realm://xxx.windows.net","DstsInstance": "xxx.core.windows.net","KustoDnsHostName": "kusto.windows.net","ServiceName": "kusto"}}`
	tests := []struct {
		name    string
		wantErr string
		tkp     tokenProvider
	}{
		{
			name: "test_acquiretoken_cred",
			tkp: tokenProvider{
				tokenCred:  NewMockClient().auth.tokenProvider.tokenCred,
				dataSource: s.urlStr() + "/test_acquiretoken_cred",
			},
			wantErr: "",
		},
		{
			name: "test_acquiretoken_invalid_datasource",
			tkp: tokenProvider{
				tokenCred:  NewMockClient().auth.tokenProvider.tokenCred,
				dataSource: "endpoint",
			},
		},
	}
	for _, test := range tests {
		tkp := test.tkp
		s.code = 200
		s.payload = []byte(payload)

		got, token_type, err := tkp.acquireToken(context.Background())
		assert.NotNil(t, err)
		assert.EqualValues(t, "", got)
		assert.EqualValues(t, "", token_type)
	}

}
