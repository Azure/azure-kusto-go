package kusto

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

type ConnectionStringBuilder struct {
	clusterURI  string
	authType    string
	resourceURI string
	authParams  map[string]interface{}
	cloudInfo   CloudInfo
}

// environmental variables
const (
	tenantIdEnvVariable     = "AZURE_TENANT_ID"
	clientIdEnvVariable     = "AZURE_CLIENT_ID"
	clientSecretEnvVariable = "AZURE_CLIENT_SECRET"
)

// params mapping
const (
	tenantIDStr      string = "TenantID"
	clientIDStr      string = "ClientID"
	clientSecretStr  string = "ClientSecret"
	appCertStr       string = "ApplicationCertificates"
	appCertKeyStr    string = "ApplicationCertificateKey"
	usernameStr      string = "Username"
	userPasswordStr  string = "UserPassword"
	appTokenStr      string = "ApplicationToken"
	clientOptionsStr string = "ClientOptions"
	sendCertChainStr string = "SendCertificateChain"
	managedIDStr     string = "ManagedIdentityID"
)

// authtype mapping
const (
	envAuth        string = "EnvironmentVars"
	azCliAuth      string = "AzCLI"
	managedIDAuth  string = "ManagedIdentity"
	clientCredAuth string = "ClientCredentials"
	appCertAuth    string = "ApplicationCetrifiate"
	unamePassAuth  string = "UsernamePassword"
	appTokenAuth   string = "ApplicationToken"
)

// Method to be used for generating TokenCredential
func (kcsb *ConnectionStringBuilder) getTokenProvider(ctx context.Context) (*TokenProvider, error) {
	tkp := &TokenProvider{}

	scopes := []string{fmt.Sprintf("%s/.default", kcsb.resourceURI)}

	clientOptions, clopsok := (kcsb.authParams[clientOptionsStr]).(azcore.ClientOptions)

	switch kcsb.authType {
	case clientCredAuth:
		{

			tenantID, tnok := kcsb.authParams[tenantIDStr]
			clientId, clok := kcsb.authParams[clientIDStr]
			clientSec, csok := kcsb.authParams[clientSecretStr]

			if !(tnok && clok && csok) {
				return nil, errors.New("Error : Couldn't get token provider due to insufficient parameters")
			}

			ccred, err := azidentity.NewClientSecretCredential(tenantID.(string), clientId.(string), clientSec.(string), nil)
			if err != nil {
				return nil, fmt.Errorf("Error : Couldn't get client credentiels. Error: %s", err)
			}
			tkp.accessToken, err = ccred.GetToken(ctx, policy.TokenRequestOptions{Scopes: scopes})
			if err != nil {
				return nil, fmt.Errorf("Error while getting token using ClientSecretCredential: %s", err)
			}
			return tkp, nil
		}
	case appCertAuth:
		{
			tenantID, tnok := kcsb.authParams[tenantIDStr]
			clientId, clok := kcsb.authParams[clientIDStr]
			certificate, crtok := kcsb.authParams[appCertStr]
			pvtkey, pkok := kcsb.authParams[appCertKeyStr]
			sndCrtChain, sccok := kcsb.authParams[sendCertChainStr]
			var pvtkeyStr string

			if !(tnok && clok && crtok) {
				return nil, errors.New("Error : Couldn't get token provider due to insufficient parameters")
			}
			if pkok {
				pvtkeyStr = pvtkey.(string)
			} else {
				pvtkeyStr = ""
			}

			certs, thumprintKey, err := azidentity.ParseCertificates([]byte((certificate).(string)), []byte(pvtkeyStr))
			if err != nil {
				return nil, err
			}

			cccOpts := &azidentity.ClientCertificateCredentialOptions{}
			if sccok {
				cccOpts.SendCertificateChain = sndCrtChain.(bool)
			}
			if clopsok {
				cccOpts.ClientOptions = clientOptions
			}

			certCred, err := azidentity.NewClientCertificateCredential(tenantID.(string), clientId.(string), certs, thumprintKey, cccOpts)
			if err != nil {
				return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels using Application Certificate: %s", err)
			}
			tkp.accessToken, err = certCred.GetToken(ctx, policy.TokenRequestOptions{Scopes: scopes})
			if err != nil {
				return nil, fmt.Errorf("Error while getting token using ClientCertificateCredential: %s", err)
			}
			return tkp, nil

		}
	case unamePassAuth:
		{
			tenantID, tnok := kcsb.authParams[tenantIDStr]
			clientId, clok := kcsb.authParams[clientIDStr]
			uname, unok := kcsb.authParams[usernameStr]
			upass, upok := kcsb.authParams[userPasswordStr]

			if !(tnok && clok && unok && upok) {
				return nil, errors.New("Error : Couldn't get token provider due to insufficient parameters")
			}

			uspCred, err := azidentity.NewUsernamePasswordCredential(tenantID.(string), clientId.(string), uname.(string), upass.(string), nil)
			if err != nil {
				return nil, fmt.Errorf("Error : Could not able to retrieve client credentiels using Username and password : %s", err)
			}
			tkp.accessToken, err = uspCred.GetToken(ctx, policy.TokenRequestOptions{Scopes: scopes})
			if err != nil {
				return nil, fmt.Errorf("Error while getting token using UserNamePasswordCredential : %s", err)
			}
			return tkp, nil

		}
	case appTokenAuth:
		{
			atoken, tok := kcsb.authParams[appTokenStr]
			if !(tok) {
				return nil, errors.New("Error : Couldn't get token provider due to insufficient parameters")
			}
			act := azcore.AccessToken{}
			act.Token = atoken.(string)
			tkp.accessToken = act
			return tkp, nil
		}
	case managedIDAuth:
		{
			miOptions := &azidentity.ManagedIdentityCredentialOptions{}

			managedID, midok := kcsb.authParams[managedIDStr]
			if midok {
				miOptions.ID = azidentity.ClientID(managedID.(string))
			}

			miC, err := azidentity.NewManagedIdentityCredential(miOptions)
			if err != nil {
				return nil, fmt.Errorf("Error : Could not able to retrieve client credentiels using Managed Identity: %s", err)
			}
			tkp.accessToken, err = miC.GetToken(ctx, policy.TokenRequestOptions{Scopes: scopes})
			if err != nil {
				return nil, fmt.Errorf("Error while getting token using MangedIdentityCredential: %s", err)
			}
			return tkp, nil

		}
	default:
		{
			//environmental variables based auth
			envOpts := &azidentity.EnvironmentCredentialOptions{}
			if clopsok {
				envOpts.ClientOptions = clientOptions
			}
			envCred, err := azidentity.NewEnvironmentCredential(envOpts)
			if err != nil {
				//TODO: no need to return error at this step. Should we log?
			}

			azCliOptions := &azidentity.AzureCLICredentialOptions{}
			tenantID, ok := kcsb.authParams[tenantIDStr]
			if ok {
				azCliOptions.TenantID = (tenantID).(string)
			}
			azCliCred, err := azidentity.NewAzureCLICredential(azCliOptions)
			if err != nil {
				//TODO: no need to return error at this step. Should we log?
			}

			chainedCred, err := azidentity.NewChainedTokenCredential([]azcore.TokenCredential{azCliCred, envCred}, &azidentity.ChainedTokenCredentialOptions{RetrySources: true})
			if err != nil {
				return nil, fmt.Errorf("Error : Couldn't retrieve client credentiels| Error: %s", err)
			}
			tkp.accessToken, err = chainedCred.GetToken(ctx, policy.TokenRequestOptions{Scopes: scopes})
			if err != nil {
				return nil, fmt.Errorf("Error while getting token using chained token credential: %s", err)
			}
			return tkp, nil
		}
	}
}

func isEmpty(str string) bool {
	return strings.TrimSpace(str) == ""
}
