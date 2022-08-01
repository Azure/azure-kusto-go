package kusto

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

type server struct {
	code    int
	payload []byte
	http    *httptest.Server
}

func newTestServ() *server {
	s := &server{}
	s.http = httptest.NewServer(s)
	return s
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer log.Println("server exited")
	w.WriteHeader(s.code)
	if s.code == 200 {
		w.Write(s.payload)
	}
}

func (s *server) urlStr() string {
	return s.http.URL
}

func (s *server) close() {
	s.http.Close()
}

func TestGetMetadata(t *testing.T) {
	s := newTestServ()
	defer s.close()
	var tests = []struct {
		name    string
		payload string
		code    int
		err     bool
		desc    string
		want    CloudInfo
		errwant string
	}{
		{
			name:    "test_cloud_info_success_1",
			code:    200,
			err:     false,
			desc:    "Success login endpoint for url-1",
			payload: `{"AzureAD": {"LoginEndpoint": "https://login.microsoftonline.com","LoginMfaRequired": false,"KustoClientAppId": "db662dc1-0cfe-4e1c-a843-19a68e65be58","KustoClientRedirectUri": "https://microsoft/kustoclient","KustoServiceResourceId": "https://kusto.dev.kusto.windows.net","FirstPartyAuthorityUrl": "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a"  },  "dSTS": {"CloudEndpointSuffix": "windows.net","DstsRealm": "realm://dsts.core.windows.net","DstsInstance": "prod-dsts.dsts.core.windows.net","KustoDnsHostName": "kusto.windows.net","ServiceName": "kusto"}}`,
			want: CloudInfo{
				LoginEndpoint:          "https://login.microsoftonline.com",
				LoginMfaRequired:       false,
				KustoClientAppID:       "db662dc1-0cfe-4e1c-a843-19a68e65be58",
				KustoClientRedirectURI: "https://microsoft/kustoclient",
				KustoServiceResourceID: "https://kusto.dev.kusto.windows.net",
				FirstPartyAuthorityURL: "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a",
			},
		},
		{
			name:    "test_cloud_info_success_2",
			code:    200,
			err:     false,
			desc:    "Success login endpoint for url-2",
			payload: `{"AzureAD": {"LoginEndpoint": "https://login2.microsoftonline.com","LoginMfaRequired": true,"KustoClientAppId": "db662dc1-0cfe-4e1c-a843-19a68e65bxxx","KustoClientRedirectUri": "https://microsoft/kustoclient","KustoServiceResourceId": "https://kusto.dev.kusto.windows.net","FirstPartyAuthorityUrl": "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e912xxx"  },  "dSTS": {"CloudEndpointSuffix": "windows.net","DstsRealm": "realm://dsts.core.windows.net","DstsInstance": "prod-dsts.dsts.core.windows.net","KustoDnsHostName": "kusto.windows.net","ServiceName": "kusto"}}`,
			want: CloudInfo{
				LoginEndpoint:          "https://login2.microsoftonline.com",
				LoginMfaRequired:       true,
				KustoClientAppID:       "db662dc1-0cfe-4e1c-a843-19a68e65bxxx",
				KustoClientRedirectURI: "https://microsoft/kustoclient",
				KustoServiceResourceID: "https://kusto.dev.kusto.windows.net",
				FirstPartyAuthorityURL: "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e912xxx",
			},
		},
		{
			name:    "test_cloud_info_not_found",
			code:    404,
			err:     false,
			desc:    "Not found",
			payload: "",
			want:    defaultCloudInfo,
		},
		{
			name:    "test_cloud_info_internal_error",
			code:    500,
			err:     true,
			desc:    "Internal server error",
			payload: "",
			want:    CloudInfo{},
			errwant: fmt.Sprintf("error 500 Internal Server Error when querying endpoint %s/%s", s.urlStr(), metadataPath),
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		s.code = test.code
		s.payload = []byte(test.payload)
		res, err := GetMetadata(ctx, s.urlStr()+"/"+test.name) // Adding test name to the path make sure multiple URL's can be cached
		switch {
		case err != nil && !test.err:
			t.Errorf("TestGetMetadata(%s): got err == %s, want err == nil", test.desc, err)
			continue
		case err == nil && test.err:
			t.Errorf("TestGetMetadata(%s): got err == nil, want err != nil", test.desc)
			continue
		}
		if test.err {
			assert.NotNil(t, err)
			assert.Equal(t, test.errwant, err.Error())
		}
		assert.Equal(t, test.want, res)
	}
}
