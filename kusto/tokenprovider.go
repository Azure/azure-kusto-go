package kusto

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

type tokenProvider struct {
	tokenCred   azcore.TokenCredential //holds the received token credential as per the authentication
	customToken string
	scopes      []string
}

func (tkp tokenProvider) getToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	act := azcore.AccessToken{}
	if tkp.tokenCred != nil {
		accessToken, err := tkp.tokenCred.GetToken(ctx, options)
		return accessToken, err
	} else {
		act.Token = tkp.customToken
	}
	return act, nil
}

func (tkp tokenProvider) isInitialized() bool {
	return !(tkp.tokenCred == nil && isEmpty(tkp.customToken))
}
