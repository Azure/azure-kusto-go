package kusto

import (
	"crypto"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

type ConnectionStringBuilder struct {
	clusterUri                    string
	envAuth                       bool
	tenentId                      string
	clientId                      string
	aadUserId                     string
	password                      string
	applicationClientId           string
	applicationKey                string
	applicationCertificates       []*x509.Certificate
	applicationCertThumbprint     crypto.PrivateKey
	publicApplicationCeritificate string
	aadAuthorityId                string
	applicationToken              string
	aadFederatedSecurity          string
	userToken                     string
	manageIdentityAuth            bool
	managedId                     string
	azCliAuth                     bool
	azCliTenentId                 string
	interactiveLogin              string
	loginHint                     string
	domainHint                    string
	cloudInfo                     cloudInfo
}

const (
	tenentIdEnvVariable     = "AZURE_TENANT_ID"
	clientIdEnvVariable     = "AZURE_CLIENT_ID"
	clientSecretEnvVariable = "AZURE_CLIENT_SECRET"
)

/*Build connection string builder to authenticate using the environment variables.
See more: https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azidentity#EnvironmentCredential */
func BuildConnectionStringWithEnv(clusterUri string) (*ConnectionStringBuilder, error) {
	kcsb := &ConnectionStringBuilder{}
	fetchedCI, _ := RetrieveCloudInfoMetadata(clusterUri)
	kcsb.cloudInfo = *fetchedCI
	kcsb.clusterUri = clusterUri
	kcsb.envAuth = true
	return kcsb, nil
}

//TODO: Need a thorough implementation check.
func BuildConnectionStringWithCert(clusterUri string, tenentId string, certPath string, password string) (*ConnectionStringBuilder, error) {
	kcsb := &ConnectionStringBuilder{}
	//Reads certificate and assign
	certificate, err := os.ReadFile(certPath)
	if err != nil {
		fmt.Println("Error : couldn't read the certificate path", err)
	}
	certs, key, err := azidentity.ParseCertificates(certificate, []byte(password))
	if err != nil {
		fmt.Println("Error : x509 certificate parsing error ", err)
	}
	kcsb.applicationCertificates = certs
	kcsb.applicationCertThumbprint = key
	fetchedCI, _ := RetrieveCloudInfoMetadata(clusterUri)
	kcsb.cloudInfo = *fetchedCI
	return kcsb, nil
}

/*Build connection string builder to authenticate with the provided access token*/
func BuildConnectionStringWithAccessToken(clusterUri string, at string) (*ConnectionStringBuilder, error) {
	kcsb := &ConnectionStringBuilder{}
	fetchedCI, _ := RetrieveCloudInfoMetadata(clusterUri)
	kcsb.cloudInfo = *fetchedCI
	kcsb.clusterUri = clusterUri
	kcsb.applicationToken = at
	return kcsb, nil
}

/* Build connection string builder to authenticate an Azure managed identity in any hosting environment supporting managed identities..
The value may be the identity's client ID or resource ID */
func BuildConnectionStringWithManagedIdentity(clusterUri string, managedId string) (*ConnectionStringBuilder, error) {
	kcsb := &ConnectionStringBuilder{}
	fetchedCI, _ := RetrieveCloudInfoMetadata(clusterUri)
	kcsb.cloudInfo = *fetchedCI
	kcsb.manageIdentityAuth = true
	kcsb.managedId = managedId
	return kcsb, nil
}

//Build connection string builder for AZ Cli authentication type, takes tenentId as input, Defaults to the CLI's default tenant
func BuildConnectionStringWithAzCli(clusterUri string, tenentId string) (*ConnectionStringBuilder, error) {
	kcsb := &ConnectionStringBuilder{}
	fetchedCI, _ := RetrieveCloudInfoMetadata(clusterUri)
	kcsb.cloudInfo = *fetchedCI
	kcsb.clusterUri = clusterUri
	kcsb.azCliAuth = true
	kcsb.azCliTenentId = tenentId
	return kcsb, nil
}

//Build connection string builder for AAD Application Credentials
func BuildConnectionStringWithAadApplicationCredentials(clusterUri string, tenentId string, appClientId string) (*ConnectionStringBuilder, error) {
	kcsb := &ConnectionStringBuilder{}
	fetchedCI, _ := RetrieveCloudInfoMetadata(clusterUri)
	kcsb.cloudInfo = *fetchedCI
	kcsb.clusterUri = clusterUri
	kcsb.applicationClientId = appClientId
	kcsb.tenentId = tenentId
	return kcsb, nil
}

func BuildConnectionStringWithUsernamePassword(clusterUri string, clientId string, tenentId string, username string, password string) (*ConnectionStringBuilder, nil) {
	kcsb := &ConnectionStringBuilder{}
	fetchedCI, _ := RetrieveCloudInfoMetadata(clusterUri)
	kcsb.cloudInfo = *fetchedCI
	kcsb.tenentId = tenentId
	kcsb.clientId = clientId
	kcsb.aadUserId = username
	kcsb.password = password
	return kcsb, nil
}

/****
	Method to be used for generating TokenCredential
****/
func (kcsb *ConnectionStringBuilder) getTokenProvider() (*tokenProvider, error) {
	if isEmpty(kcsb.clusterUri) {
		errors.New("Error : Cluster URL not set.")
	}
	tkp := &tokenProvider{}

	resourceUri := kcsb.cloudInfo.kustoServiceResourceId

	//Update resource URI if MFA enabled
	if kcsb.cloudInfo.loginMfaRequired {
		resourceUri = strings.Replace(resourceUri, ".kusto.", ".kustomfa.", 1)
	}
	tkp.scopes = []string{resourceUri + "/.default"}

	if kcsb.envAuth {
		envCred, err := azidentity.NewEnvironmentCredential(nil)
		if err != nil {
			return nil, errors.New("Error : Could not able to retrieve client credentiels using Azure CLI")
		}
		tkp.tokenCredential = envCred
		return tkp, nil

	} else if !isEmpty(kcsb.applicationClientId) {

		if clientSecret := os.Getenv(clientSecretEnvVariable); !isEmpty(clientSecret) {
			ccred, err := azidentity.NewClientSecretCredential(kcsb.tenentId, kcsb.applicationClientId, clientSecret, nil)
			if err != nil {
				return nil, errors.New("Error : Could not able to retrieve client credentiels")
			}
			tkp.tokenCredential = ccred
			return tkp, nil
		} else if len(kcsb.applicationCertificates) != 0 {
			certCred, err := azidentity.NewClientCertificateCredential(kcsb.tenentId, kcsb.applicationClientId, kcsb.applicationCertificates, kcsb.applicationCertThumbprint, nil)
			if err != nil {
				return nil, errors.New("Error : Could not able to retrieve client credentiels using Certificate")
			}
			tkp.tokenCredential = certCred
			return tkp, nil
		}
	} else if !isEmpty(kcsb.applicationToken) {
		tkp.accessToken = kcsb.applicationToken
		return tkp, nil
	} else if kcsb.manageIdentityAuth {
		miOptions := &azidentity.ManagedIdentityCredentialOptions{}
		if !isEmpty(kcsb.managedId) {
			miOptions.ID = azidentity.ClientID(kcsb.managedId)
		}
		miC, err := azidentity.NewManagedIdentityCredential(miOptions)
		if err != nil {
			return nil, errors.New("Error : Could not able to retrieve client credentiels using Managed Identity")
		}
		tkp.tokenCredential = miC
		return tkp, nil
	} else if kcsb.azCliAuth {
		azCliOptions := &azidentity.AzureCLICredentialOptions{}
		if !isEmpty(kcsb.azCliTenentId) {
			azCliOptions.TenantID = kcsb.azCliTenentId
		}
		azCliCred, err := azidentity.NewAzureCLICredential(azCliOptions)
		if err != nil {
			return nil, errors.New("Error : Could not able to retrieve client credentiels using Azure CLI")
		}
		tkp.tokenCredential = azCliCred
		return tkp, nil

	} else if !isEmpty(kcsb.aadUserId) && !isEmpty(kcsb.password) {
		uspCred, err := azidentity.NewUsernamePasswordCredential(kcsb.tenentId, kcsb.clientId, kcsb.aadUserId, kcsb.password, nil)
		if err != nil {
			return nil, errors.New("Error : Could not able to retrieve client credentiels using Username and password")
		}
		tkp.tokenCredential = uspCred
		return tkp, nil
	}

	return nil, errors.New("Error : type Not supported")
}

func isEmpty(str string) bool {
	return strings.TrimSpace(str) == ""
}
