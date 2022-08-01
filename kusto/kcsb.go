package kusto

import (
	"context"
	"crypto"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

type ConnectionStringBuilder struct {
	clusterURI                    string
	envAuth                       bool
	tenentID                      string
	clientID                      string
	clientSecret                  string
	aadUserID                     string
	password                      string
	applicationClientID           string
	applicationCertificates       []*x509.Certificate
	applicationCertThumbprint     crypto.PrivateKey
	publicApplicationCeritificate string
	applicationToken              string
	manageIdentityAuth            bool
	managedID                     string
	azCliAuth                     bool
	azCliTenentID                 string
	cloudInfo                     CloudInfo
}

const (
	tenentIdEnvVariable     = "AZURE_TENANT_ID"
	clientIdEnvVariable     = "AZURE_CLIENT_ID"
	clientSecretEnvVariable = "AZURE_CLIENT_SECRET"
)

/*Build connection string builder to authenticate using the environment variables.
See more: https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azidentity#EnvironmentCredential */
func BuildConnectionStringWithEnv(clusterURI string) (*ConnectionStringBuilder, error) {
	kcsb := &ConnectionStringBuilder{}
	fetchedCI, _ := GetMetadata(context.Background(), clusterURI)
	kcsb.cloudInfo = fetchedCI
	kcsb.clusterURI = clusterURI
	kcsb.envAuth = true
	return kcsb, nil
}

//TODO: Need a thorough implementation check.
/*Build connection string to authenticate a service principal with a certificate. Take clusterURI, tenentID, certificate as byte array and password to
decrypt the certificate. Pass nil for password if the private key isn't encrypted.*/
func BuildConnectionStringWithCert(clusterURI string, tenentID string, certificate []byte, password string) (*ConnectionStringBuilder, error) {
	kcsb := &ConnectionStringBuilder{}

	certs, key, err := azidentity.ParseCertificates(certificate, []byte(password))
	if err != nil {
		fmt.Println("Error : x509 certificate parsing error ", err)
	}
	kcsb.applicationCertificates = certs
	kcsb.applicationCertThumbprint = key
	fetchedCI, _ := GetMetadata(context.Background(), clusterURI)
	kcsb.tenentID = tenentID
	kcsb.cloudInfo = fetchedCI
	return kcsb, nil
}

/*Build connection string builder to authenticate with the provided access token*/
func BuildConnectionStringWithAccessToken(clusterURI string, at string) (*ConnectionStringBuilder, error) {
	kcsb := &ConnectionStringBuilder{}
	fetchedCI, _ := GetMetadata(context.Background(), clusterURI)
	kcsb.cloudInfo = fetchedCI
	kcsb.clusterURI = clusterURI
	kcsb.applicationToken = at
	return kcsb, nil
}

/* Build connection string builder to authenticate an Azure managed identity in any hosting environment supporting managed identities..
The value may be the identity's client ID or resource ID */
func BuildConnectionStringWithManagedIdentity(clusterURI string, managedID string) (*ConnectionStringBuilder, error) {
	kcsb := &ConnectionStringBuilder{}
	fetchedCI, _ := GetMetadata(context.Background(), clusterURI)
	kcsb.cloudInfo = fetchedCI
	kcsb.manageIdentityAuth = true
	kcsb.managedID = managedID
	return kcsb, nil
}

//Build connection string builder for AZ Cli authentication type, takes tenentID as input, Defaults to the CLI's default tenant
func BuildConnectionStringWithAzCli(clusterURI string, tenentID string) (*ConnectionStringBuilder, error) {
	kcsb := &ConnectionStringBuilder{}
	fetchedCI, _ := GetMetadata(context.Background(), clusterURI)
	kcsb.cloudInfo = fetchedCI
	kcsb.clusterURI = clusterURI
	kcsb.azCliAuth = true
	kcsb.azCliTenentID = tenentID
	return kcsb, nil
}

//Build connection string builder for AAD Application Credentials
func BuildConnectionStringWithAadApplicationCredentials(clusterURI string, tenentID string, appClientID string, cSec string) (*ConnectionStringBuilder, error) {
	kcsb := &ConnectionStringBuilder{}
	fetchedCI, _ := GetMetadata(context.Background(), clusterURI)
	kcsb.cloudInfo = fetchedCI
	kcsb.clusterURI = clusterURI
	kcsb.applicationClientID = appClientID
	kcsb.tenentID = tenentID
	kcsb.clientSecret = cSec
	return kcsb, nil
}

func BuildConnectionStringWithUsernamePassword(clusterURI string, tenentID string, clientID string, username string, password string) (*ConnectionStringBuilder, error) {
	kcsb := &ConnectionStringBuilder{}
	fetchedCI, _ := GetMetadata(context.Background(), clusterURI)
	kcsb.cloudInfo = fetchedCI
	kcsb.tenentID = tenentID
	kcsb.clientID = clientID
	kcsb.aadUserID = username
	kcsb.password = password
	return kcsb, nil
}

// Method to be used for generating TokenCredential
func (kcsb *ConnectionStringBuilder) getTokenProvider() (*tokenProvider, error) {
	if isEmpty(kcsb.clusterURI) {
		return nil, errors.New("Error : Cluster URL not set.")
	}
	tkp := &tokenProvider{}

	resourceUri := kcsb.cloudInfo.KustoServiceResourceID

	//Update resource URI if MFA enabled
	if kcsb.cloudInfo.LoginMfaRequired {
		resourceUri = strings.Replace(resourceUri, ".kusto.", ".kustomfa.", 1)
	}
	tkp.scopes = []string{resourceUri + "/.default"}

	if kcsb.envAuth {
		envCred, err := azidentity.NewEnvironmentCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("Error : Could not able to retrieve client credentiels using Azure CLI: %s", err)
		}
		tkp.tokenCredential = envCred
		return tkp, nil

	} else if !isEmpty(kcsb.applicationClientID) {

		clientSecret := os.Getenv(clientSecretEnvVariable)
		if isEmpty(clientSecret) {
			clientSecret = kcsb.clientSecret
		}
		if !isEmpty(clientSecret) {
			ccred, err := azidentity.NewClientSecretCredential(kcsb.tenentID, kcsb.applicationClientID, clientSecret, nil)
			if err != nil {
				return nil, errors.New("Error : Could not able to retrieve client credentiels")
			}
			tkp.tokenCredential = ccred
			return tkp, nil
		} else if len(kcsb.applicationCertificates) != 0 {
			certCred, err := azidentity.NewClientCertificateCredential(kcsb.tenentID, kcsb.applicationClientID, kcsb.applicationCertificates, kcsb.applicationCertThumbprint, nil)
			if err != nil {
				return nil, fmt.Errorf("Error : Could not able to retrieve client credentiels using Certificate: %s", err)
			}
			tkp.tokenCredential = certCred
			return tkp, nil
		}
	} else if !isEmpty(kcsb.applicationToken) {
		tkp.accessToken = kcsb.applicationToken
		return tkp, nil
	} else if kcsb.manageIdentityAuth {
		miOptions := &azidentity.ManagedIdentityCredentialOptions{}
		if !isEmpty(kcsb.managedID) {
			miOptions.ID = azidentity.ClientID(kcsb.managedID)
		}
		miC, err := azidentity.NewManagedIdentityCredential(miOptions)
		if err != nil {
			return nil, fmt.Errorf("Error : Could not able to retrieve client credentiels using Managed Identity: %s", err)
		}
		tkp.tokenCredential = miC
		return tkp, nil
	} else if kcsb.azCliAuth {
		azCliOptions := &azidentity.AzureCLICredentialOptions{}
		if !isEmpty(kcsb.azCliTenentID) {
			azCliOptions.TenantID = kcsb.azCliTenentID
		}
		azCliCred, err := azidentity.NewAzureCLICredential(azCliOptions)
		if err != nil {
			return nil, fmt.Errorf("Error : Could not able to retrieve client credentiels using Azure CLI : %s", err)
		}
		tkp.tokenCredential = azCliCred
		return tkp, nil

	} else if !isEmpty(kcsb.aadUserID) && !isEmpty(kcsb.password) {
		uspCred, err := azidentity.NewUsernamePasswordCredential(kcsb.tenentID, kcsb.clientID, kcsb.aadUserID, kcsb.password, nil)
		if err != nil {
			return nil, fmt.Errorf("Error : Could not able to retrieve client credentiels using Username and password : %s", err)
		}
		tkp.tokenCredential = uspCred
		return tkp, nil
	}

	return nil, errors.New("Error : Authtype Not supported")
}

func isEmpty(str string) bool {
	return strings.TrimSpace(str) == ""
}
