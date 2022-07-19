package kusto

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRetrieveCloudInfoMetadataSuccess(t *testing.T) {
	responsePayloadOne := `{"AzureAD": {"LoginEndpoint": "https://login.microsoftonline.com","LoginMfaRequired": false,"KustoClientAppId": "db662dc1-0cfe-4e1c-a843-19a68e65be58","KustoClientRedirectUri": "https://microsoft/kustoclient","KustoServiceResourceId": "https://kusto.dev.kusto.windows.net","FirstPartyAuthorityUrl": "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a"  },  "dSTS": {"CloudEndpointSuffix": "windows.net","DstsRealm": "realm://dsts.core.windows.net","DstsInstance": "prod-dsts.dsts.core.windows.net","KustoDnsHostName": "kusto.windows.net","ServiceName": "kusto"}}`
	expectedCloudInfo := &cloudInfo{
		loginEndpoint:          "https://login.microsoftonline.com",
		loginMfaRequired:       false,
		kustoClientAppId:       "db662dc1-0cfe-4e1c-a843-19a68e65be58",
		kustoClientRedirectUri: "https://microsoft/kustoclient",
		kustoServiceResourceId: "https://kusto.dev.kusto.windows.net",
		firstPartyAuthorityUrl: "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a",
	}
	server := runServer(http.StatusOK, responsePayloadOne, true)
	actualCloudInfo, err := RetrieveCloudInfoMetadata(server.URL)
	assert.Nil(t, err)
	assert.Equal(t, expectedCloudInfo, actualCloudInfo)
	server.Close()
}

func TestRetrieveCloudInfoMetadataError(t *testing.T) {
	responsePayload := ``
	server := runServer(http.StatusInternalServerError, responsePayload, false)
	actualCloudInfo, err := RetrieveCloudInfoMetadata(server.URL)
	server.Close()
	assert.NotNil(t, err)
	assert.Nil(t, actualCloudInfo)
	errorMessage := err.Error()
	assert.EqualValues(t, fmt.Sprintf("retrieved error code %d when querying endpoint %s", http.StatusInternalServerError, server.URL), errorMessage)
}

func TestRetrieveCloudInfoMetadataNotFound(t *testing.T) {
	server := runServer(http.StatusNotFound, ``, false)
	actualCloudInfo, err := RetrieveCloudInfoMetadata(server.URL)
	server.Close()
	assert.Nil(t, err)
	assert.Equal(t, defaultCloudInfo, actualCloudInfo)
}

func TestRetrieveCloudInfoMetadataSuccessNoBody(t *testing.T) {
	server := runServer(http.StatusOK, ``, true)
	actualCloudInfo, err := RetrieveCloudInfoMetadata(server.URL)
	server.Close()
	assert.Nil(t, err)
	assert.Equal(t, defaultCloudInfo, actualCloudInfo)
}

func runServer(statusCode int, payload string, isSuccess bool) *httptest.Server {
	// Start a local HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Test request parameters
		// Send response to be tested
		rw.WriteHeader(statusCode)
		if isSuccess {
			rw.Write([]byte(payload))
		}
	}))
	return server
	// Close the server when test finishes
}
