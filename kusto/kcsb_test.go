package kusto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildConnectionStringWithEnv(t *testing.T) {
	ecsb := &ConnectionStringBuilder{}
	ecsb.clusterURI = "endpoint"
	ecsb.envAuth = true
	csb, err := BuildConnectionStringWithEnv("endpoint")
	assert.Nil(t, err)
	assert.NotNil(t, csb)
	assert.Equal(t, ecsb, csb)
}

func TestBuildConnectionStringWithAccessToken(t *testing.T) {
	ecsb := &ConnectionStringBuilder{}
	ecsb.clusterURI = "endpoint"
	ecsb.applicationToken = "dummytoken"
	csb, err := BuildConnectionStringWithAccessToken("endpoint", "dummytoken")
	assert.Nil(t, err)
	assert.NotNil(t, csb)
	assert.Equal(t, ecsb, csb)
}

func TestBuildConnectionStringWithMangedIdentity(t *testing.T) {
	ecsb := &ConnectionStringBuilder{}
	ecsb.clusterURI = "endpoint"
	ecsb.manageIdentityAuth = true
	ecsb.managedID = "resourceID"
	csb, err := BuildConnectionStringWithManagedIdentity("endpoint", "resourceID")
	assert.Nil(t, err)
	assert.NotNil(t, csb)
	assert.Equal(t, ecsb, csb)
}

func TestBuildConnectionStringWithAzCli(t *testing.T) {
	ecsb := &ConnectionStringBuilder{}
	ecsb.clusterURI = "endpoint"
	ecsb.tenentID = "tenentID"
	ecsb.azCliAuth = true
	csb, err := BuildConnectionStringWithAzCli("endpoint", "tenentID")
	assert.Nil(t, err)
	assert.NotNil(t, csb)
	assert.Equal(t, ecsb, csb)
}

func TestBuildConnectionStringWithAadApplicationCredentials(t *testing.T) {
	ecsb := &ConnectionStringBuilder{}
	ecsb.clusterURI = "endpoint"
	ecsb.tenentID = "tenentID"
	ecsb.applicationClientID = "appClientID"
	ecsb.clientSecret = "clientSecret"
	csb, err := BuildConnectionStringWithAadApplicationCredentials("endpoint", "tenentID", "appClientID", "clientSecret")
	assert.Nil(t, err)
	assert.NotNil(t, csb)
	assert.Equal(t, ecsb, csb)
}

func TestBuildConnectionStringWithUsernamePassword(t *testing.T) {
	ecsb := &ConnectionStringBuilder{}
	ecsb.clusterURI = "endpoint"
	ecsb.tenentID = "tenentID"
	ecsb.applicationClientID = "clientID"
	ecsb.aadUserID = "username"
	ecsb.password = "password"
	csb, err := BuildConnectionStringWithUsernamePassword("endpoint", "tenentID", "clientID", "username", "password")
	assert.Nil(t, err)
	assert.NotNil(t, csb)
	assert.Equal(t, ecsb, csb)
}
