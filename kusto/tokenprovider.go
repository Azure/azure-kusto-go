package kusto

import (
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

type TokenProvider struct {
	tokenCred     azcore.TokenCredential //holds the received token credential as per the authentication
	tokenScheme   string
	customToken   string
	dataSource    string
	cloudInfoInit bool
	scopes        []string
}

// tokenProvider need to be received as reference, to reflect updations to the structs
func (tkp *TokenProvider) AcquireToken(ctx context.Context) (string, string, error) {
	if tkp.tokenCred != nil {
		if !tkp.cloudInfoInit {
			//Fetches cloud meta data
			fetchedCI, cierr := GetMetadata(context.Background(), tkp.dataSource)
			if cierr != nil {
				return "", "", fmt.Errorf("Error: couldn't retrieve the clould Meta Info: %s", cierr)
			}
			tkp.cloudInfoInit = true
			//Update resource URI if MFA enabled
			resourceURI := fetchedCI.KustoServiceResourceID
			if fetchedCI.LoginMfaRequired {
				resourceURI = strings.Replace(resourceURI, ".kusto.", ".kustomfa.", 1)
			}
			tkp.scopes = []string{fmt.Sprintf("%s/.default", resourceURI)}
		}
		token, err := tkp.tokenCred.GetToken(ctx, policy.TokenRequestOptions{Scopes: tkp.scopes})
		if err != nil {
			return "", "", err
		}
		return token.Token, tkp.tokenScheme, nil
	}

	if !isEmpty(tkp.customToken) {
		return tkp.customToken, tkp.tokenScheme, nil
	}
	return "", "", fmt.Errorf("Error: No token info present in token provider")
}

func (tkp TokenProvider) AuthorizationRequired() bool {
	return !(tkp.tokenCred == nil && isEmpty(tkp.customToken))
}
