package kusto

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

type tokenProvider struct {
	tokenCred     azcore.TokenCredential //holds the received token credential as per the authentication
	customToken   string
	dataSource    string
	cloudInfoInit bool
	scopes        []string
}

//tokenProvider need to be received as reference, to reflect updations to the structs
func (tkp *tokenProvider) acquireToken(ctx context.Context) (azcore.AccessToken, error) {
	act := azcore.AccessToken{}
	if tkp.tokenCred != nil {
		if !tkp.cloudInfoInit {
			//Fetches cloud meta data
			fetchedCI, cierr := GetMetadata(context.Background(), tkp.dataSource)
			if cierr != nil {
				return azcore.AccessToken{
					Token:     "",
					ExpiresOn: time.Time{},
				}, fmt.Errorf("Error: couldn't retrieve the clould Meta Info: %s", cierr)
			}
			tkp.cloudInfoInit = true
			//Update resource URI if MFA enabled
			resourceURI := fetchedCI.KustoServiceResourceID
			if fetchedCI.LoginMfaRequired {
				resourceURI = strings.Replace(resourceURI, ".kusto.", ".kustomfa.", 1)
			}
			tkp.scopes = []string{fmt.Sprintf("%s/.default", resourceURI)}
		}
		return tkp.tokenCred.GetToken(ctx, policy.TokenRequestOptions{Scopes: tkp.scopes})
		
	} else {
		act.Token = tkp.customToken
	}
	return act, nil
}

func (tkp tokenProvider) isInitialized() bool {
	return !(tkp.tokenCred == nil && isEmpty(tkp.customToken))
}
