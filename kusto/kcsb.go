package kusto

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

type connectionStringBuilder struct {
	DataSource                       string `json:"DataSource"`
	AadUserID                        string `json:"AADUserID"`
	Password                         string `json:"Password"`
	UserToken                        string `json:"UserToken"`
	ApplicationClientId              string `json:"ApplicationClientId"`
	ApplicationKey                   string `json:"ApplicationKey"`
	AuthorityId                      string `json:"AuthorityId" default:"organizations"`
	ApplicationCertificate           string `json:"ApplicationCertificate"`
	ApplicationCertificateThumbprint string `json:"ApplicationCertificateThumbprint"`
	SendCertificateChain             bool   `json:"SendCertificateChain"`
	ApplicationToken                 string `json:"ApplicationToken"`
	AZCLI                            bool   `json:"AZCLI"`
	MSIAuthentication                bool   `json:"MSIAuthentication"`
	ManagedServiceIdentity           string `json:"ManagedServiceIdentity"`
	InteractiveLogin                 bool   `json:"InteractiveLogin"`
	RedirectURL                      string `json:"RedirectURL"`
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
	applicationCertificateThumbprint string = "ApplicationCertificateThumbprint"
	sendCertificateChain             string = "SendCertificateChain"
	authorityId                      string = "AuthorityId"
	applicationToken                 string = "ApplicationToken"
	userToken                        string = "UserToken"
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

func contains(list []string, tofind string) bool {
	for _, s := range list {
		if tofind == s {
			return true
		}
	}
	return false
}

func assignValue(kcsb *connectionStringBuilder, rawKey string, value string) {
	rawKey = strings.ToLower(strings.Trim(rawKey, " "))
	if contains([]string{"datasource", "data source", "addr", "address", "network address", "server"}, rawKey) {
		kcsb.DataSource = value
	}
	if contains([]string{"aad user id", "aaduserid"}, rawKey) {
		kcsb.AadUserID = value
	}
	if contains([]string{"password", "pwd"}, rawKey) {
		kcsb.Password = value
	}
	if contains([]string{"application client id", "applicationclientid", "appclientid"}, rawKey) {
		kcsb.ApplicationClientId = value
	}
	if contains([]string{"application key", "applicationkey", "appkey"}, rawKey) {
		kcsb.ApplicationKey = value
	}
	if contains([]string{"application certificate", "applicationcertificate"}, rawKey) {
		kcsb.ApplicationCertificate = value
	}
	if contains([]string{"application certificate thumbprint", "applicationcertificatethumbprint"}, rawKey) {
		kcsb.ApplicationCertificateThumbprint = value
	}
	if contains([]string{"sendcertificatechain", "send certificate chain"}, rawKey) {
		bval, _ := strconv.ParseBool(value)
		kcsb.SendCertificateChain = bval
	}
	if contains([]string{"authority id", "authorityid", "authority", "tenantid", "tenant", "tid"}, rawKey) {
		kcsb.AuthorityId = value
	}
	if contains([]string{"application token", "applicationtoken", "apptoken"}, rawKey) {
		kcsb.ApplicationToken = value
	}
	if contains([]string{"user token", "usertoken", "usrtoken"}, rawKey) {
		kcsb.UserToken = value
	}
	if contains([]string{"msi_auth"}, rawKey) {
		bval, _ := strconv.ParseBool(value)
		kcsb.MSIAuthentication = bval
	}
	if contains([]string{"managedserviceidentity", "managed service identity"}, rawKey) {
		kcsb.ManagedServiceIdentity = value
	}
	if contains([]string{"az cli", "azcli"}, rawKey) {
		bval, _ := strconv.ParseBool(value)
		kcsb.AZCLI = bval
	}
	if contains([]string{"interactive login", "interactivelogin"}, rawKey) {
		bval, _ := strconv.ParseBool(value)
		kcsb.InteractiveLogin = bval
	}
	if contains([]string{"domain hint", "domainhint"}, rawKey) {
		kcsb.RedirectURL = value
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

	/*for _, kvp := range strings.Split(connStr, ";") {
		kvparr := strings.Split(kvp, "=")
		val := strings.Trim(kvparr[1], " ")
		if isEmpty(val) {
			continue
		}
		assignValue(&kcsb, kvparr[0], val)

	}*/

	csTemp := make(map[string]interface{}, 20)
	connStr = strings.Replace(connStr, ";", "&", -1)

	q, e := url.ParseQuery(connStr)
	if e != nil {
		fmt.Println(connStr)
		panic(fmt.Sprintf("Error: parsing the connection string : %s", e))
	}
	for key, value := range q {
		if len(value) != 0 {
			csTemp[csMapping[key]] = strings.Trim(value[0], " ")
		}
	}

	jsonbody, err := json.Marshal(csTemp)
	if err != nil {
		panic(fmt.Sprintf(`Error : Connection string JSON parsing error : %s`, err))
	}

	csb := connectionStringBuilder{}
	if err := json.Unmarshal(jsonbody, &csb); err != nil {
		panic(fmt.Sprintf(`Error : Connection string parsing error : %s`, err))
	}
	return kcsb
}

//Creates a KustoConnection string builder that will authenticate with AAD user name and password.
func (kcsb connectionStringBuilder) WithAadUserPassAuth(uname string, pswrd string, authorityID string) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.DataSource)
	requireNonEmpty(aadUserId, uname)
	requireNonEmpty(password, pswrd)
	kcsb.AadUserID = uname
	kcsb.Password = pswrd
	kcsb.AuthorityId = authorityID
	return kcsb
}

//Creates a KustoConnection string builder that will authenticate with AAD user token
func (kcsb connectionStringBuilder) WitAadUserToken(userTkn string) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.DataSource)
	requireNonEmpty(userToken, userTkn)
	kcsb.UserToken = userTkn
	return kcsb
}

//Creates a KustoConnection string builder that will authenticate with AAD application and key.
func (kcsb connectionStringBuilder) WithAadAppKey(appId string, appKey string, authorityID string) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.DataSource)
	requireNonEmpty(applicationClientId, appId)
	requireNonEmpty(applicationKey, appKey)
	requireNonEmpty(authorityId, authorityID)
	kcsb.ApplicationClientId = appId
	kcsb.ApplicationKey = appKey
	kcsb.AuthorityId = authorityID
	return kcsb
}

//Creates a KustoConnection string builder that will authenticate with AAD application using a certificate.
func (kcsb connectionStringBuilder) WithAppCertificate(appId string, certificate string, thumprint string, sendCertChain bool, authorityID string) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.DataSource)
	requireNonEmpty(applicationCertificate, certificate)
	requireNonEmpty(applicationCertificateThumbprint, thumprint)
	requireNonEmpty(authorityId, authorityID)
	kcsb.ApplicationClientId = appId
	kcsb.AuthorityId = authorityID

	kcsb.ApplicationCertificate = certificate
	kcsb.ApplicationCertificateThumbprint = thumprint
	kcsb.SendCertificateChain = sendCertChain
	return kcsb
}

// Creates a KustoConnection string builder that will authenticate with AAD application and an application token.
func (kcsb connectionStringBuilder) WithApplicationToken(appId string, appToken string) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.DataSource)
	requireNonEmpty(applicationToken, appToken)
	kcsb.ApplicationToken = appToken
	return kcsb
}

// Creates a KustoConnection string builder that will use existing authenticated az cli profile password.
func (kcsb connectionStringBuilder) WithAzCli() connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.DataSource)
	kcsb.AZCLI = true
	return kcsb
}

/*
Creates a KustoConnection string builder that will authenticate with AAD application, using
  an application token obtained from a Microsoft Service Identity endpoint. An optional user
  assigned application ID can be added to the token.
*/
func (kcsb connectionStringBuilder) WithManagedServiceID(clientID string, resId string) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.DataSource)
	kcsb.MSIAuthentication = true
	if !isEmpty(clientID) {
		kcsb.ManagedServiceIdentity = clientID
	} else if !isEmpty(resId) {
		kcsb.ManagedServiceIdentity = resId
	}
	return kcsb
}

func (kcsb connectionStringBuilder) WithInteractiveLogin(clientID string, authorityID string, redirectURL string) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.DataSource)
	if !isEmpty(clientID) {
		kcsb.ApplicationClientId = clientID
	}
	if !isEmpty(authorityID) {
		kcsb.AuthorityId = authorityID
	}
	if !isEmpty(redirectURL) {
		kcsb.RedirectURL = redirectURL
	}
	kcsb.InteractiveLogin = true
	return kcsb
}

func (kcsb connectionStringBuilder) AttachClientOptions(options *azcore.ClientOptions) connectionStringBuilder {
	requireNonEmpty(dataSource, kcsb.DataSource)
	if options == nil {
		kcsb.clientOptions = options
	}
	return kcsb
}

// Method to be used for generating TokenCredential
func (kcsb *connectionStringBuilder) getTokenProvider(ctx context.Context) (*tokenProvider, error) {
	tkp := &tokenProvider{}
	//Fetches cloud meta data
	fetchedCI, cierr := GetMetadata(context.Background(), kcsb.DataSource)
	if cierr != nil {
		return nil, cierr
	}
	//Update resource URI if MFA enabled
	resourceURI := fetchedCI.KustoServiceResourceID
	if fetchedCI.LoginMfaRequired {
		resourceURI = strings.Replace(resourceURI, ".kusto.", ".kustomfa.", 1)
	}
	tkp.scopes = []string{fmt.Sprintf("%s/.default", resourceURI)}

	if kcsb.InteractiveLogin {
		inOps := &azidentity.InteractiveBrowserCredentialOptions{}

		if !isEmpty(kcsb.ApplicationClientId) {
			inOps.ClientID = kcsb.ApplicationClientId
		}
		if !isEmpty(kcsb.AuthorityId) {
			inOps.TenantID = kcsb.AuthorityId
		}
		if kcsb.clientOptions != nil {
			inOps.ClientOptions = *kcsb.clientOptions
		}
		cred, err := azidentity.NewInteractiveBrowserCredential(inOps)
		if err != nil {
			return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels using Interactive Login. Error: %s", err)
		}
		tkp.tokenCred = cred
	} else if !isEmpty(kcsb.AadUserID) && !isEmpty(kcsb.Password) {
		var ops *azidentity.UsernamePasswordCredentialOptions

		if kcsb.clientOptions != nil {
			ops = &azidentity.UsernamePasswordCredentialOptions{ClientOptions: *kcsb.clientOptions}
		}

		cred, err := azidentity.NewUsernamePasswordCredential(kcsb.AuthorityId, kcsb.ApplicationClientId, kcsb.AadUserID, kcsb.Password, ops)
		if err != nil {
			return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels using Username Password. Error: %s", err)
		}
		tkp.tokenCred = cred
	} else if !isEmpty(kcsb.ApplicationClientId) && !isEmpty(kcsb.ApplicationKey) {
		fmt.Println("HERE ARE PARAMS : ", kcsb.AuthorityId, kcsb.ApplicationClientId, kcsb.ApplicationKey)
		opts := &azidentity.ClientSecretCredentialOptions{}
		if kcsb.clientOptions != nil {
			opts.ClientOptions = *kcsb.clientOptions
		}
		cred, err := azidentity.NewClientSecretCredential(kcsb.AuthorityId, kcsb.ApplicationClientId, kcsb.ApplicationKey, opts)
		if err != nil {
			return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels using Client Secret. Error: %s", err)
		}
		tkp.tokenCred = cred
		return tkp, nil
	} else if !isEmpty(kcsb.ApplicationCertificate) {
		cert, thumprintKey, err := azidentity.ParseCertificates([]byte(kcsb.ApplicationCertificate), []byte(kcsb.ApplicationCertificateThumbprint))
		if err != nil {
			return nil, err
		}
		opts := &azidentity.ClientCertificateCredentialOptions{}
		opts.SendCertificateChain = kcsb.SendCertificateChain
		if kcsb.clientOptions != nil {
			opts.ClientOptions = *kcsb.clientOptions
		}
		cred, err := azidentity.NewClientCertificateCredential(kcsb.AuthorityId, kcsb.ApplicationClientId, cert, thumprintKey, opts)
		if err != nil {
			return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels using Application Certificate: %s", err)
		}
		tkp.tokenCred = cred
	} else if kcsb.MSIAuthentication {
		opts := &azidentity.ManagedIdentityCredentialOptions{}
		if !isEmpty(kcsb.ManagedServiceIdentity) {
			opts.ID = azidentity.ClientID(kcsb.ManagedServiceIdentity)
		}
		if kcsb.clientOptions != nil {
			opts.ClientOptions = *kcsb.clientOptions
		}

		cred, err := azidentity.NewManagedIdentityCredential(opts)
		if err != nil {
			return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels using Managed Identity: %s", err)
		}
		tkp.tokenCred = cred
	} else if !isEmpty(kcsb.UserToken) {
		tkp.customToken = kcsb.UserToken
	} else if !isEmpty(kcsb.ApplicationToken) {
		tkp.customToken = kcsb.ApplicationToken
	} else if kcsb.AZCLI {
		opts := &azidentity.AzureCLICredentialOptions{}
		opts.TenantID = kcsb.AuthorityId
		cred, err := azidentity.NewAzureCLICredential(opts)
		if err != nil {
			return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels using Azure CLI: %s", err)
		}
		tkp.tokenCred = cred
	} else {
		//env variables based auth
		opts := &azidentity.EnvironmentCredentialOptions{}
		if kcsb.clientOptions != nil {
			opts.ClientOptions = *kcsb.clientOptions
		}
		cred, err := azidentity.NewEnvironmentCredential(opts)
		if err != nil {
			return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels: %s", err)
		}
		tkp.tokenCred = cred
	}
	return tkp, nil
}

func isEmpty(str string) bool {
	return strings.TrimSpace(str) == ""
}
