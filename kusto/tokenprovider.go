package kusto

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

type TokenProvider struct {
	tokenCred   azcore.TokenCredential //Holds the received token credential as per the authorization
	tokenScheme string                 //Contains token scheme for tokenprovider
	customToken string                 //Holds the custom auth token to be used for authorization
	initOnce    *sync.Once             //To ensure tokenprovider will be initialized only once while aquiring token
	init        func()                 //Initialiser for initialising the tokencredential and cloudinfo
	ci          CloudInfo              //Contains cloud setting meto information
	scopes      []string               //Contains scopes of the auth token
	err         error                  //To monitor errors in the process of init tokenprovider
}

// tokenProvider need to be received as reference, to reflect updations to the structs
func (tkp *TokenProvider) AcquireToken(ctx context.Context) (string, string, error) {
	if !isEmpty(tkp.customToken) {
		return tkp.customToken, tkp.tokenScheme, nil
	}

	if tkp.tokenCred == nil {
		// initialise the tokenCredential and cloud info
		tkp.initOnce.Do(func() {
			tkp.init()
			// Update resource URI if MFA enabled
			resourceURI := tkp.ci.KustoServiceResourceID
			if tkp.ci.LoginMfaRequired {
				resourceURI = strings.Replace(resourceURI, ".kusto.", ".kustomfa.", 1)
			}
			scopes := []string{fmt.Sprintf("%s/.default", resourceURI)}
			tkp.scopes = scopes
		})
		if tkp.err != nil {
			return "", "", tkp.err
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

func (tkp TokenProvider) AuthorizationRequired() bool {
	return !(tkp.init == nil && tkp.tokenCred == nil && isEmpty(tkp.customToken))
}
