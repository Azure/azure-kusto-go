package kusto

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

type tokenProvider struct {
	tokenCredential azcore.TokenCredential
	accessToken     string
	scopes          []string
}

func (tkp *tokenProvider) getToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	act := azcore.AccessToken{}
	if isEmpty(tkp.accessToken) {
		accessToken, err := tkp.tokenCredential.GetToken(ctx, options)
		return accessToken, err
	} else {
		act.Token = tkp.accessToken
	}
	return act, nil
}
