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
		kcsb connectionStringBuilder
	}{
		{
			name: "test_tokenprovider_cred",
			kcsb: connectionStringBuilder{
				authType:   clientCredAuth,
				clusterURI: "endpoint",
				authParams: map[string]interface{}{
					tenantIDStr:      "tenantID",
					clientIDStr:      "clientID",
					clientSecretStr:  "clientSec",
					clientOptionsStr: azcore.ClientOptions{},
				},
			},
		}, {
			name: "test_tokenprovider_usernamepasswordauth",
			kcsb: connectionStringBuilder{
				authType:   unamePassAuth,
				clusterURI: "endpoint",
				authParams: map[string]interface{}{
					tenantIDStr:     "tenantID",
					clientIDStr:     "clientID",
					usernameStr:     "ussername",
					userPasswordStr: "userpass",
				},
			},
		}, {
			name: "test_tokenprovider_usernamepasswordauth",
			kcsb: connectionStringBuilder{
				authType:   appTokenAuth,
				clusterURI: "endpoint",
				authParams: map[string]interface{}{
					appTokenStr:      "dummytokenstring",
					clientOptionsStr: azcore.ClientOptions{},
				},
			},
		}, {
			name: "test_tokenprovider_managedidauth",
			kcsb: connectionStringBuilder{
				authType:   managedIDAuth,
				clusterURI: "endpoint",
				authParams: map[string]interface{}{
					managedIDStr:     "managedid",
					clientOptionsStr: azcore.ClientOptions{},
				},
			},
		}, {
			name: "test_tokenprovider_managedidauth2",
			kcsb: connectionStringBuilder{
				authType:   managedIDAuth,
				clusterURI: "endpoint",
				authParams: map[string]interface{}{},
			},
		}, {
			name: "test_tokenprovider_default",
			kcsb: connectionStringBuilder{
				authType:   "random",
				clusterURI: "endpoint",
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
		kcsb connectionStringBuilder
	}{
		{
			name: "test_tokenprovider_cred_err",
			kcsb: connectionStringBuilder{
				authType:   clientCredAuth,
				clusterURI: "endpoint",
				authParams: map[string]interface{}{
					tenantIDStr: "tenantID",
				},
			},
		}, {
			name: "test_tokenprovider_usernamepasswordauth_err",
			kcsb: connectionStringBuilder{
				authType:   unamePassAuth,
				clusterURI: "endpoint",
				authParams: map[string]interface{}{
					tenantIDStr: "tenentId",
					clientIDStr: "clientID",
					usernameStr: "ussername",
				},
			},
		}, {
			name: "test_tokenprovider_usernamepasswordauth_err",
			kcsb: connectionStringBuilder{
				authType:   appTokenAuth,
				clusterURI: "endpoint",
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
