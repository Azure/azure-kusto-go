// Package authentication - in charge of authenticating the user with the system
package authentication

import (
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/quickstart/utils"
	"os"
)

type AuthenticationModeOptions string

const (
	UserPrompt      AuthenticationModeOptions = "UserPrompt"
	ManagedIdentity AuthenticationModeOptions = "ManagedIdentity"
	AppKey          AuthenticationModeOptions = "AppKey"
	AppCertificate  AuthenticationModeOptions = "AppCertificate"
)

// GenerateConnectionString  Generates Kusto Connection String based on given Authentication Mode.
func GenerateConnectionString(clusterUrl string, authenticationMode AuthenticationModeOptions) *azkustodata.ConnectionStringBuilder {
	// Learn More: For additional information on how to authorize users and apps in Kusto, see:
	// https://docs.microsoft.com/azure/data-explorer/manage-database-permissions
	var kcs = azkustodata.NewConnectionStringBuilder(clusterUrl)
	switch authenticationMode {
	case UserPrompt:
		// Prompt user for credentials
		return kcs.WithInteractiveLogin("")
	case ManagedIdentity:
		// Authenticate using a System-Assigned managed identity provided to an azure service, or using a User-Assigned managed identity.
		// For more information, see https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview
		return kcs.WithSystemManagedIdentity()
	case AppKey:
		// Learn More: For information about how to procure an AAD Application,
		// see: https://docs.microsoft.com/azure/data-explorer/provision-azure-ad-app
		// TODO (config - optional): App ID and tenant, and App Key to authenticate with
		return kcs.WithAadAppKey(os.Getenv("APP_ID"), os.Getenv("APP_KEY"), os.Getenv("APP_TENANT"))
	case AppCertificate:
		// Authenticate using a certificate file.
		return createApplicationCertificateConnectionString(kcs)
	default:
		utils.ErrorHandler(fmt.Sprintf("Authentication mode '%s' is not supported", authenticationMode), nil)
		return nil
	}
}

// createApplicationCertificateConnectionString Generates Kusto Connection String based on 'AppCertificate' Authentication Mode
func createApplicationCertificateConnectionString(kcs *azkustodata.ConnectionStringBuilder) *azkustodata.ConnectionStringBuilder {
	var appId = os.Getenv("APP_ID")
	var cert = os.Getenv("PUBLIC_CERT_FILE_PATH")
	var thumbprint = os.Getenv("CERT_THUMBPRINT")
	var appTenant = os.Getenv("APP_TENANT")
	return kcs.WithAppCertificate(appId, cert, thumbprint, true, appTenant)
}
