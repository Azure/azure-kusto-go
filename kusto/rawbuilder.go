package kusto

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

type builderParams struct {
	clusterURI              string
	tenantID                string
	clientSecret            string
	aadUserID               string
	username                string
	userPassword            string
	applicationClientID     string
	applicationCertificates []byte
	sendCertificateChain    bool
	privateKey              string
	applicationToken        string
	managedID               string
	clientOptions           azcore.ClientOptions
}

func GetBuilder() *builderParams {
	return &builderParams{}
}

func (bp *builderParams) WithClusterURI(clusterUri string) *builderParams {
	bp.clusterURI = clusterUri
	return bp
}

func (bp *builderParams) WithClientId(clientID string) *builderParams {
	bp.applicationClientID = clientID
	return bp
}

func (bp *builderParams) WithTenantId(tenantID string) *builderParams {
	bp.tenantID = tenantID
	return bp
}

func (bp *builderParams) WithClientSec(clientSec string) *builderParams {
	bp.clientSecret = clientSec
	return bp
}

func (bp *builderParams) WithUserName(username string) *builderParams {
	bp.username = username
	return bp
}

func (bp *builderParams) WithUserPassword(password string) *builderParams {
	bp.userPassword = password
	return bp
}

// sendCertChain controls whether the credential sends the public certificate chain in the x5c
// header of each token request's JWT. This is required for Subject Name/Issuer (SNI) authentication.
// Defaults to False.
func (bp *builderParams) WithAppCertificates(certificates []byte, privateKey string, sendCertChain bool) *builderParams {
	bp.applicationCertificates = certificates
	bp.privateKey = privateKey
	bp.sendCertificateChain = sendCertChain
	return bp
}

func (bp *builderParams) WithApplicationToken(token string) *builderParams {
	bp.applicationToken = token
	return bp
}

func (bp *builderParams) WithManagedID(managedID string) *builderParams {
	bp.managedID = managedID
	return bp
}

func (bp *builderParams) WithClientOptions(options azcore.ClientOptions) *builderParams {
	bp.clientOptions = options
	return bp
}

func (bp *builderParams) Build() (*ConnectionStringBuilder, error) {
	kcsb := &ConnectionStringBuilder{}
	if isEmpty(bp.clusterURI) {
		return nil, fmt.Errorf("Error : Cluster URL not set")
	}

	//Fetches cloud meta data
	fetchedCI, cierr := GetMetadata(context.Background(), bp.clusterURI)
	if cierr != nil {
		return nil, cierr
	}
	kcsb.clusterURI = bp.clusterURI
	kcsb.cloudInfo = fetchedCI

	kcsb.authParams = map[string]interface{}{}
	if !reflect.DeepEqual(bp.clientOptions, azcore.ClientOptions{}) {
		kcsb.authParams[clientOptionsStr] = bp.clientOptions
	}

	//Update resource URI if MFA enabled
	resourceURI := fetchedCI.KustoServiceResourceID
	if fetchedCI.LoginMfaRequired {
		resourceURI = strings.Replace(resourceURI, ".kusto.", ".kustomfa.", 1)
	}
	kcsb.resourceURI = resourceURI

	if !isEmpty(bp.applicationClientID) {
		clientSecret := os.Getenv(clientSecretEnvVariable)

		if isEmpty(bp.tenantID) {
			return nil, fmt.Errorf("invalid parameters: tenantID")
		}

		kcsb.authParams[tenantIDStr] = bp.tenantID
		kcsb.authParams[clientIDStr] = bp.applicationClientID

		//Could be done this way to, this will eleminate the clientId to be set by user
		//kcsb.authParams[clientIDStr] = fetchedCI.KustoClientAppID

		if isEmpty(clientSecret) {
			clientSecret = bp.clientSecret
		}
		if !isEmpty(clientSecret) {

			kcsb.authParams[clientSecretStr] = clientSecret
			kcsb.authType = clientCredAuth
			return kcsb, nil
		} else if len(bp.applicationCertificates) != 0 {
			kcsb.authParams[appCertStr] = bp.applicationCertificates
			kcsb.authParams[appCertKeyStr] = bp.privateKey
			kcsb.authParams[sendCertChainStr] = bp.sendCertificateChain
			kcsb.authType = appCertAuth
			return kcsb, nil
		} else if !isEmpty(bp.aadUserID) && !isEmpty(bp.userPassword) {
			kcsb.authParams[usernameStr] = bp.username
			kcsb.authParams[userPasswordStr] = bp.userPassword
			kcsb.authType = unamePassAuth
			return kcsb, nil
		}
	} else if !isEmpty(bp.applicationToken) {
		kcsb.authParams[appTokenStr] = bp.applicationToken
		kcsb.authType = appTokenAuth
		return kcsb, nil
	} else if !isEmpty(bp.managedID) {
		kcsb.authParams[managedIDStr] = bp.managedID
		kcsb.authType = managedIDAuth
	} else if !isEmpty(bp.tenantID) {
		kcsb.authParams[tenantIDStr] = bp.tenantID
	}

	return kcsb, nil
}
