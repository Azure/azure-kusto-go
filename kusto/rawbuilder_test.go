package kusto

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildHappy(t *testing.T) {
	s := newTestServ()
	tests := []struct {
		name    string
		payload string
		want    *ConnectionStringBuilder
		bparams builderParams
	}{
		{
			name:    "build_basic",
			payload: `{"AzureAD": {"LoginEndpoint": "https://login.microsofdummy.com","LoginMfaRequired": false,"KustoClientAppId": "db662dc1-0cfe-4e1c-a843-19a68e65xxxx","KustoClientRedirectUri": "https://microsoft/dummykustoclient","KustoServiceResourceId": "https://kusto.windows.net","FirstPartyAuthorityUrl": "https://login.microsofdummy.com/f8cdef31-a31e-4b4a-93e4-5f571e9xxxxx"  },  "dSTS": {"CloudEndpointSuffix": "windows.net","DstsRealm": "realm://xxx.windows.net","DstsInstance": "xxx.core.windows.net","KustoDnsHostName": "kusto.windows.net","ServiceName": "kusto"}}`,
			want: &ConnectionStringBuilder{
				clusterURI:  s.urlStr() + "/build_basic",
				authType:    "",
				resourceURI: "https://kusto.windows.net",
				authParams:  map[string]interface{}{},
				cloudInfo: CloudInfo{
					LoginEndpoint:          "https://login.microsofdummy.com",
					LoginMfaRequired:       false,
					KustoClientAppID:       "db662dc1-0cfe-4e1c-a843-19a68e65xxxx",
					KustoClientRedirectURI: "https://microsoft/dummykustoclient",
					KustoServiceResourceID: "https://kusto.windows.net",
					FirstPartyAuthorityURL: "https://login.microsofdummy.com/f8cdef31-a31e-4b4a-93e4-5f571e9xxxxx",
				},
			},
			bparams: builderParams{
				clusterURI: "endpoint",
			},
		}, {
			name:    "build_clientid",
			payload: `{"AzureAD": {"LoginEndpoint": "https://login.microsofdummy.com","LoginMfaRequired": false,"KustoClientAppId": "db662dc1-0cfe-4e1c-a843-19a68e65xxxx","KustoClientRedirectUri": "https://microsoft/dummykustoclient","KustoServiceResourceId": "https://kusto.windows.net","FirstPartyAuthorityUrl": "https://login.microsofdummy.com/f8cdef31-a31e-4b4a-93e4-5f571e9xxxxx"  },  "dSTS": {"CloudEndpointSuffix": "windows.net","DstsRealm": "realm://xxx.windows.net","DstsInstance": "xxx.core.windows.net","KustoDnsHostName": "kusto.windows.net","ServiceName": "kusto"}}`,
			want: &ConnectionStringBuilder{
				clusterURI:  s.urlStr() + "/build_clientid",
				authType:    "",
				resourceURI: "https://kusto.windows.net",
				authParams: map[string]interface{}{
					tenantIDStr: "tenantId",
					clientIDStr: "clientID",
				},
				cloudInfo: CloudInfo{
					LoginEndpoint:          "https://login.microsofdummy.com",
					LoginMfaRequired:       false,
					KustoClientAppID:       "db662dc1-0cfe-4e1c-a843-19a68e65xxxx",
					KustoClientRedirectURI: "https://microsoft/dummykustoclient",
					KustoServiceResourceID: "https://kusto.windows.net",
					FirstPartyAuthorityURL: "https://login.microsofdummy.com/f8cdef31-a31e-4b4a-93e4-5f571e9xxxxx",
				},
			},
			bparams: builderParams{
				clusterURI:          "endpoint",
				applicationClientID: "clientID",
				tenantID:            "tenantId",
			},
		},
	}

	for _, test := range tests {
		bp := test.bparams
		s.code = 200
		s.payload = []byte(test.payload)
		bp.clusterURI = fmt.Sprintf("%s/%s", s.urlStr(), test.name)
		resp, err := bp.Build()

		assert.Nil(t, err)
		assert.EqualValues(t, test.want, resp)
	}
}
