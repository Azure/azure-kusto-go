package kusto

import (
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/kusto/utils"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

type TokenProvider struct {
	tokenCred   azcore.TokenCredential                  //Holds the received token credential as per the authorization
	tokenScheme string                                  //Contains token scheme for tokenprovider
	customToken string                                  //Holds the custom auth token to be used for authorization
	initOnce    utils.OnceWithInit[*tokenWrapperResult] //To ensure tokenprovider will be initialized only once while aquiring token
	scopes      []string                                //Contains scopes of the auth token
}

// tokenProvider need to be received as reference, to reflect updations to the structs
func (tkp *TokenProvider) AcquireToken(ctx context.Context) (string, string, error) {
	if !isEmpty(tkp.customToken) {
		return tkp.customToken, tkp.tokenScheme, nil
	}

	if tkp.initOnce != nil {
		_, err := tkp.initOnce.DoWithInit()
		if err != nil {
			return "", "", err
		}
	}

	if tkp.tokenCred != nil {
		token, err := tkp.tokenCred.GetToken(ctx, policy.TokenRequestOptions{Scopes: tkp.scopes})
		if err != nil {
			return "", "", err
		}
		return token.Token, tkp.tokenScheme, nil
	}

	return "", "", fmt.Errorf("Error: No token info present in token provider")
}

func (tkp *TokenProvider) AuthorizationRequired() bool {
	return !(tkp.initOnce == nil && tkp.tokenCred == nil && isEmpty(tkp.customToken))
}

type tokenWrapperResult struct {
	credential azcore.TokenCredential
	scopes     []string
}

func (tkp *TokenProvider) setInit(kcsb *ConnectionStringBuilder, f func(*CloudInfo, *azcore.ClientOptions, string) (azcore.TokenCredential, error)) {
	tkp.initOnce = utils.NewOnceWithInit(func() (*tokenWrapperResult, error) {
		wrapper, err := tokenWrapper(kcsb, f)
		if err != nil {
			return nil, err
		}

		tkp.tokenCred = wrapper.credential
		tkp.scopes = wrapper.scopes

		return wrapper, err
	})
}

func tokenWrapper(kcsb *ConnectionStringBuilder, f func(*CloudInfo, *azcore.ClientOptions, string) (azcore.TokenCredential, error)) (*tokenWrapperResult,
	error) {
	ci, cliOpts, appClientId, err := getCommonCloudInfo(kcsb)
	if err != nil {
		return nil, err
	}

	credential, err := f(ci, cliOpts, appClientId)
	if err != nil {
		return nil, err
	}

	resourceURI := ci.KustoServiceResourceID
	if ci.LoginMfaRequired {
		resourceURI = strings.Replace(resourceURI, ".kusto.", ".kustomfa.", 1)
	}
	scopes := []string{fmt.Sprintf("%s/.default", resourceURI)}

	return &tokenWrapperResult{
		credential: credential,
		scopes:     scopes,
	}, nil
}

func getCommonCloudInfo(kcsb *ConnectionStringBuilder) (*CloudInfo, *azcore.ClientOptions, string, error) {
	cloud, err := GetMetadata(kcsb.DataSource)
	if err != nil {
		return nil, nil, "", err
	}
	cliOpts := kcsb.ClientOptions
	appClientId := kcsb.ApplicationClientId
	if cliOpts == nil {
		cliOpts = &azcore.ClientOptions{}
	}
	if isEmpty(cliOpts.Cloud.ActiveDirectoryAuthorityHost) {
		cliOpts.Cloud.ActiveDirectoryAuthorityHost = cloud.LoginEndpoint
	}
	if isEmpty(appClientId) {
		appClientId = cloud.KustoClientAppID
	}
	return &cloud, cliOpts, appClientId, nil
}
