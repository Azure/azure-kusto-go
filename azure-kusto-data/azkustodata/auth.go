package azkustodata

import (
	"github.com/Azure/go-autorest/autorest/adal"
)

const activeDirectoryEndpoint = "https://login.microsoftonline.com/"


func getTokenWithAadApp(appId string, appSecret string, resource string, tenantId string) (*adal.Token, error) {
	oauthConfig, err := adal.NewOAuthConfig(activeDirectoryEndpoint, tenantId)

	if err != nil {
		return nil, err
	}

	spt, err := adal.NewServicePrincipalToken(
		*oauthConfig,
		appId,
		appSecret,
		resource)
	if err != nil {
		return nil, err
	}

	// Acquire a new access token
	err = spt.Refresh()
	if err != nil {
		return nil, err
	}

	token := spt.Token()
	return  &token, nil
}