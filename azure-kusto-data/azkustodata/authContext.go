package azkustodata

import "github.com/Azure/go-autorest/autorest/adal"

type TokenGetter interface {
	GetToken() (*adal.Token, error);
}

type KustoAuthContext struct {
	cluster   string;
	appId     string;
	appSecret string;
	tenantId  string;
}

func AuthenticateWithAadApp(cluster string, appId string, appSecret string, tenantId string) (*KustoAuthContext, error) {
	return &KustoAuthContext{
		cluster,
		appId,
		appSecret,
		tenantId,
	}, nil;
}

func (kac KustoAuthContext) GetToken() (*adal.Token, error) {
	token, error := getTokenWithAadApp(kac.appId, kac.appSecret, kac.cluster, kac.tenantId)

	if error != nil {
		return nil, error
	}

	return token, nil
}
