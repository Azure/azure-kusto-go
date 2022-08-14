package kusto

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

type TokenProvider struct {
	accessToken azcore.AccessToken //holds the received AccessToken for respective credential
}

func (tkp TokenProvider) getToken() azcore.AccessToken {
	return tkp.accessToken
}
