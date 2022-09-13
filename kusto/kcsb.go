package kusto

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

type connectionStringBuilder struct {
	dataSource                       string
	aadUserID                        string
	password                         string
	userToken                        string
	applicationClientId              string
	applicationKey                   string
	authorityId                      string
	applicationCertificate           string
	applicationCertificateThumbprint string
	sendCertificateChain             bool
	applicationToken                 string
	azcli                            bool
	msiAuthentication                bool
	managedServiceIdentity           string
	interactiveLogin                 bool
	redirectURL                      string
	clientOptions                    *azcore.ClientOptions
}

// params mapping
const (
	dataSource                       string = "DataSource"
	aadUserId                        string = "AADUserID"
	password                         string = "Password"
	applicationClientId              string = "ApplicationClientId"
	applicationKey                   string = "ApplicationKey"
	applicationCertificate           string = "ApplicationCertificate"
	authorityId                      string = "AuthorityId"
	applicationToken                 string = "ApplicationToken"
	userToken                        string = "UserToken"
	applicationCertificateThumbprint string = "ApplicationCertificateThumbprint"
	sendCertificateChain             string = "SendCertificateChain"
	msiAuth                          string = "MSIAuthentication"
	managedServiceIdentity           string = "ManagedServiceIdentity"
	azCli                            string = "AZCLI"
	interactiveLogin                 string = "InteractiveLogin"
	domainHint                       string = "RedirectURL"
)

var csMapping = map[string]string{"datasource": dataSource, "data source": dataSource, "addr": dataSource, "address": dataSource, "network address": dataSource, "server": dataSource,
	"aad user id": aadUserId, "aaduserid": aadUserId,
	"password": password, "pwd": password,
	"application client id": applicationClientId, "applicationclientid": applicationClientId, "appclientid": applicationClientId,
	"application key": applicationKey, "applicationkey": applicationKey, "appkey": applicationKey,
	"application certificate": applicationCertificate, "applicationcertificate": applicationCertificate,
	"application certificate thumbprint": applicationCertificateThumbprint, "applicationcertificatethumbprint": applicationCertificateThumbprint,
	"sendcertificatechain": sendCertificateChain, "send certificate chain": sendCertificateChain,
	"authority id": authorityId, "authorityid": authorityId, "authority": authorityId, "tenantid": authorityId, "tenant": authorityId, "tid": authorityId,
	"application token": applicationToken, "applicationtoken": applicationToken, "apptoken": applicationToken,
	"user token": userToken, "usertoken": userToken, "usrtoken": userToken,
	"msi_auth":               msiAuth,
	"managedserviceidentity": managedServiceIdentity, "managed service identity": managedServiceIdentity,
	"interactive login": interactiveLogin, "interactivelogin": interactiveLogin,
	"az cli": azCli, "azcli": azCli,
	"domain hint": domainHint, "domainhint": domainHint,
}

func requireNonEmpty(key string, value string) {
	if isEmpty(value) {
		panic(fmt.Sprintf("Error: %s cannot be null", key))
	}
}

func assignValue(kcsb *connectionStringBuilder, rawKey string, value string) {
	rawKey = strings.ToLower(strings.Trim(rawKey, " "))
	parsedKey := csMapping[rawKey]
	switch parsedKey {
	case dataSource:
		kcsb.dataSource = value
	case aadUserId:
		kcsb.aadUserID = value
	case password:
		kcsb.password = value
	case applicationClientId:
		kcsb.applicationClientId = value
	case applicationKey:
		kcsb.applicationKey = value
	case applicationCertificate:
		kcsb.applicationCertificate = value
	case applicationCertificateThumbprint:
		kcsb.applicationCertificateThumbprint = value
	case sendCertificateChain:
		bval, _ := strconv.ParseBool(value)
		kcsb.sendCertificateChain = bval
	case authorityId:
		kcsb.authorityId = value
	case applicationToken:
		kcsb.applicationToken = value
	case userToken:
		kcsb.userToken = value
	case msiAuth:
		bval, _ := strconv.ParseBool(value)
		kcsb.msiAuthentication = bval
	case managedServiceIdentity:
		kcsb.managedServiceIdentity = value
	case azCli:
		bval, _ := strconv.ParseBool(value)
		kcsb.azcli = bval
	case interactiveLogin:
		bval, _ := strconv.ParseBool(value)
		kcsb.interactiveLogin = bval
	case domainHint:
		kcsb.redirectURL = value
	}
}

/*
   Creates new KustoConnectionStringBuilder.
   Params takes kusto connection string connStr: string.  Kusto connection string should be of the format:
   https://<clusterName>.kusto.windows.net;AAD User ID="user@microsoft.com";Password=P@ssWord
   For more information please look at:
   https://docs.microsoft.com/azure/data-explorer/kusto/api/connection-strings/kusto
*/
func GetConnectionStringBuilder(connStr string) connectionStringBuilder {
	kcsb := connectionStringBuilder{}
	if isEmpty(connStr) {
		panic("Error : Connection string cannot be empty")
	}

	if !strings.Contains(strings.Split(connStr, ";")[0], "=") {
		connStr = "Data Source=" + connStr
	}

	for _, kvp := range strings.Split(connStr, ";") {
		kvparr := strings.Split(kvp, "=")
		val := strings.Trim(kvparr[1], " ")
		if isEmpty(val) {
			continue
		}
		assignValue(&kcsb, kvparr[0], val)

	}

	return kcsb
}

//Creates a KustoConnection string builder that will authenticate with AAD user name and password.
func (kcsb connectionStringBuilder) WithAadUserPassAuth(uname string, pswrd string, authorityID string) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.dataSource)
	requireNonEmpty(aadUserId, uname)
	requireNonEmpty(password, pswrd)
	kcsb.aadUserID = uname
	kcsb.password = pswrd
	kcsb.authorityId = authorityID
	return kcsb
}

//Creates a KustoConnection string builder that will authenticate with AAD user token
func (kcsb connectionStringBuilder) WitAadUserToken(userTkn string) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.dataSource)
	requireNonEmpty(userToken, userTkn)
	kcsb.userToken = userTkn
	return kcsb
}

//Creates a KustoConnection string builder that will authenticate with AAD application and key.
func (kcsb connectionStringBuilder) WithAadAppKey(appId string, appKey string, authorityID string) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.dataSource)
	requireNonEmpty(applicationClientId, appId)
	requireNonEmpty(applicationKey, appKey)
	requireNonEmpty(authorityId, authorityID)
	kcsb.applicationClientId = appId
	kcsb.applicationKey = appKey
	kcsb.authorityId = authorityID
	return kcsb
}

//Creates a KustoConnection string builder that will authenticate with AAD application using a certificate.
func (kcsb connectionStringBuilder) WithAppCertificate(appId string, certificate string, thumprint string, sendCertChain bool, authorityID string) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.dataSource)
	requireNonEmpty(applicationCertificate, certificate)
	requireNonEmpty(authorityId, authorityID)
	kcsb.applicationClientId = appId
	kcsb.authorityId = authorityID

	kcsb.applicationCertificate = certificate
	kcsb.applicationCertificateThumbprint = thumprint
	kcsb.sendCertificateChain = sendCertChain
	return kcsb
}

// Creates a KustoConnection string builder that will authenticate with AAD application and an application token.
func (kcsb connectionStringBuilder) WithApplicationToken(appId string, appToken string) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.dataSource)
	requireNonEmpty(applicationToken, appToken)
	kcsb.applicationToken = appToken
	return kcsb
}

// Creates a KustoConnection string builder that will use existing authenticated az cli profile password.
func (kcsb connectionStringBuilder) WithAzCli() connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.dataSource)
	kcsb.azcli = true
	return kcsb
}

/*
Creates a KustoConnection string builder that will authenticate with AAD application, using
  an application token obtained from a Microsoft Service Identity endpoint. An optional user
  assigned application ID can be added to the token.
*/
func (kcsb connectionStringBuilder) WithManagedServiceID(clientID string, resId string) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.dataSource)
	kcsb.msiAuthentication = true
	if !isEmpty(clientID) {
		kcsb.managedServiceIdentity = clientID
	} else if !isEmpty(resId) {
		kcsb.managedServiceIdentity = resId
	}
	return kcsb
}

func (kcsb connectionStringBuilder) WithInteractiveLogin(clientID string, authorityID string, redirectURL string) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.dataSource)
	if !isEmpty(clientID) {
		kcsb.applicationClientId = clientID
	}
	if !isEmpty(authorityID) {
		kcsb.authorityId = authorityID
	}
	if !isEmpty(redirectURL) {
		kcsb.redirectURL = redirectURL
	}
	kcsb.interactiveLogin = true
	return kcsb
}

func (kcsb connectionStringBuilder) AttachClientOptions(options *azcore.ClientOptions) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.dataSource)
	if options == nil {
		kcsb.clientOptions = options
	}
	return kcsb
}

// Method to be used for generating TokenCredential
func (kcsb connectionStringBuilder) getTokenProvider() (*tokenProvider, error) {
	tkp := &tokenProvider{}
	tkp.dataSource = kcsb.dataSource
	if kcsb.interactiveLogin {
		inOps := &azidentity.InteractiveBrowserCredentialOptions{}

		if !isEmpty(kcsb.applicationClientId) {
			inOps.ClientID = kcsb.applicationClientId
		}
		if !isEmpty(kcsb.authorityId) {
			inOps.TenantID = kcsb.authorityId
		}
		if kcsb.clientOptions != nil {
			inOps.ClientOptions = *kcsb.clientOptions
		}
		cred, err := azidentity.NewInteractiveBrowserCredential(inOps)
		if err != nil {
			return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels using Interactive Login. Error: %s", err)
		}
		tkp.tokenCred = cred
	} else if !isEmpty(kcsb.aadUserID) && !isEmpty(kcsb.password) {
		var ops *azidentity.UsernamePasswordCredentialOptions

		if kcsb.clientOptions != nil {
			ops = &azidentity.UsernamePasswordCredentialOptions{ClientOptions: *kcsb.clientOptions}
		}

		cred, err := azidentity.NewUsernamePasswordCredential(kcsb.authorityId, kcsb.applicationClientId, kcsb.aadUserID, kcsb.password, ops)
		if err != nil {
			return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels using Username Password. Error: %s", err)
		}
		tkp.tokenCred = cred
	} else if !isEmpty(kcsb.applicationClientId) && !isEmpty(kcsb.applicationKey) {
		var opts *azidentity.ClientSecretCredentialOptions
		if kcsb.clientOptions != nil {
			opts = &azidentity.ClientSecretCredentialOptions{}
			opts.ClientOptions = *kcsb.clientOptions
		}
		cred, err := azidentity.NewClientSecretCredential(kcsb.authorityId, kcsb.applicationClientId, kcsb.applicationKey, opts)
		if err != nil {
			return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels using Client Secret. Error: %s", err)
		}
		tkp.tokenCred = cred
		return tkp, nil
	} else if !isEmpty(kcsb.applicationCertificate) {
		cert, thumprintKey, err := azidentity.ParseCertificates([]byte(kcsb.applicationCertificate), []byte(kcsb.applicationCertificateThumbprint))
		if err != nil {
			return nil, err
		}
		opts := &azidentity.ClientCertificateCredentialOptions{}
		opts.SendCertificateChain = kcsb.sendCertificateChain
		if kcsb.clientOptions != nil {
			opts.ClientOptions = *kcsb.clientOptions
		}
		cred, err := azidentity.NewClientCertificateCredential(kcsb.authorityId, kcsb.applicationClientId, cert, thumprintKey, opts)
		if err != nil {
			return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels using Application Certificate: %s", err)
		}
		tkp.tokenCred = cred
	} else if kcsb.msiAuthentication {
		opts := &azidentity.ManagedIdentityCredentialOptions{}
		if !isEmpty(kcsb.managedServiceIdentity) {
			opts.ID = azidentity.ClientID(kcsb.managedServiceIdentity)
		}
		if kcsb.clientOptions != nil {
			opts.ClientOptions = *kcsb.clientOptions
		}

		cred, err := azidentity.NewManagedIdentityCredential(opts)
		if err != nil {
			return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels using Managed Identity: %s", err)
		}
		tkp.tokenCred = cred
	} else if !isEmpty(kcsb.userToken) {
		tkp.customToken = kcsb.userToken
	} else if !isEmpty(kcsb.applicationToken) {
		tkp.customToken = kcsb.applicationToken
	} else if kcsb.azcli {
		opts := &azidentity.AzureCLICredentialOptions{}
		opts.TenantID = kcsb.authorityId
		cred, err := azidentity.NewAzureCLICredential(opts)
		if err != nil {
			return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels using Azure CLI: %s", err)
		}
		tkp.tokenCred = cred
	} else {
		//Default Azure authentication
		opts := &azidentity.DefaultAzureCredentialOptions{}
		if kcsb.clientOptions != nil {
			opts.ClientOptions = *kcsb.clientOptions
		}
		if !isEmpty(kcsb.authorityId) {
			opts.TenantID = kcsb.authorityId
		}
		cred, err := azidentity.NewDefaultAzureCredential(opts)
		if err != nil {
			return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels for DefaultAzureCredential: %s", err)
		}
		tkp.tokenCred = cred
	}
	return tkp, nil
}

func isEmpty(str string) bool {
	return strings.TrimSpace(str) == ""
}
