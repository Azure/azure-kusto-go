package kusto

import (
	"context"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/stretchr/testify/assert"
)

func TestGetTokenProviderHappy(t *testing.T) {
	tests := []struct {
		name string
		kcsb ConnectionStringBuilder
	}{
		{
			name: "test_tokenprovider_cred",
			kcsb: ConnectionStringBuilder{
				authType:    clientCredAuth,
				clusterURI:  "endpoint",
				resourceURI: "resId",
				authParams: map[string]interface{}{
					tenantIDStr:      "tenantID",
					clientIDStr:      "clientID",
					clientSecretStr:  "clientSec",
					clientOptionsStr: azcore.ClientOptions{},
				},
			},
		}, {
			name: "test_tokenprovider_usernamepasswordauth",
			kcsb: ConnectionStringBuilder{
				authType:    unamePassAuth,
				clusterURI:  "endpoint",
				resourceURI: "resId",
				authParams: map[string]interface{}{
					tenantIDStr:     "tenantID",
					clientIDStr:     "clientID",
					usernameStr:     "ussername",
					userPasswordStr: "userpass",
				},
			},
		}, {
			name: "test_tokenprovider_usernamepasswordauth",
			kcsb: ConnectionStringBuilder{
				authType:    appTokenAuth,
				clusterURI:  "endpoint",
				resourceURI: "resId",
				authParams: map[string]interface{}{
					appTokenStr:      "dummytokenstring",
					clientOptionsStr: azcore.ClientOptions{},
				},
			},
		}, {
			name: "test_tokenprovider_managedidauth",
			kcsb: ConnectionStringBuilder{
				authType:    managedIDAuth,
				clusterURI:  "endpoint",
				resourceURI: "resId",
				authParams: map[string]interface{}{
					managedIDStr:     "managedid",
					clientOptionsStr: azcore.ClientOptions{},
				},
			},
		}, {
			name: "test_tokenprovider_managedidauth2",
			kcsb: ConnectionStringBuilder{
				authType:    managedIDAuth,
				clusterURI:  "endpoint",
				resourceURI: "resId",
				authParams:  map[string]interface{}{},
			},
		}, {
			name: "test_tokenprovider_default",
			kcsb: ConnectionStringBuilder{
				authType:    "random",
				clusterURI:  "endpoint",
				resourceURI: "resId",
				authParams: map[string]interface{}{
					tenantIDStr: "",
					clientIDStr: "clientID",
				},
			},
		},
	}
	for _, test := range tests {
		kscb := test.kcsb
		got, err := kscb.getTokenProvider(context.Background())
		assert.Nil(t, err)
		assert.NotNil(t, got)
	}

}

func TestGetTokenProviderErr(t *testing.T) {
	tests := []struct {
		name string
		kcsb ConnectionStringBuilder
	}{
		{
			name: "test_tokenprovider_cred_err",
			kcsb: ConnectionStringBuilder{
				authType:    clientCredAuth,
				clusterURI:  "endpoint",
				resourceURI: "resId",
				authParams: map[string]interface{}{
					tenantIDStr: "tenantID",
				},
			},
		}, {
			name: "test_tokenprovider_usernamepasswordauth_err",
			kcsb: ConnectionStringBuilder{
				authType:    unamePassAuth,
				clusterURI:  "endpoint",
				resourceURI: "resId",
				authParams: map[string]interface{}{
					tenantIDStr: "tenentId",
					clientIDStr: "clientID",
					usernameStr: "ussername",
				},
			},
		}, {
			name: "test_tokenprovider_usernamepasswordauth_err",
			kcsb: ConnectionStringBuilder{
				authType:    appTokenAuth,
				clusterURI:  "endpoint",
				resourceURI: "resId",
				authParams: map[string]interface{}{
					clientOptionsStr: azcore.ClientOptions{},
				},
			},
		},
	}
	for _, test := range tests {
		kscb := test.kcsb
		got, err := kscb.getTokenProvider(context.Background())
		assert.Nil(t, got)
		assert.NotNil(t, err)
		assert.EqualValues(t, "Error : Couldn't get token provider due to insufficient parameters", err.Error())
	}

}
