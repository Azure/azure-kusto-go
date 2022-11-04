package kusto

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/tj/assert"
)

func TestGetConnectionStringBuilder(t *testing.T) {

	tests := []struct {
		name             string
		connectionString string
		want             ConnectionStringBuilder
		wantErr          string
	}{
		{
			name:             "test_conn_string_validURL",
			connectionString: "https://endpoint",
			want: ConnectionStringBuilder{
				DataSource: "https://endpoint",
			},
		},
		{
			name:             "test_conn_string_emptyconnstr",
			connectionString: "",
			wantErr:          "Error : Connection string cannot be empty",
		},
		{
			name:             "test_conn_string_fullstring",
			connectionString: "https://help.kusto.windows.net/Samples;aad user id=1234;password=****;application key=1234;application client id=1234;application key=0987;application certificate=avsefsfbsrgbrb; authority id=123456;application token=token;user token=usertoken; msi_auth=true;ManagedServiceIdentity=123456; azcli=true;interactivelogin=false; domainhint=www.google.com",
			want: ConnectionStringBuilder{
				DataSource:                       "https://help.kusto.windows.net/Samples",
				AadUserID:                        "1234",
				Password:                         "****",
				UserToken:                        "usertoken",
				ApplicationClientId:              "1234",
				ApplicationKey:                   "0987",
				AuthorityId:                      "123456",
				ApplicationCertificate:           "avsefsfbsrgbrb",
				ApplicationCertificateThumbprint: "",
				SendCertificateChain:             false,
				ApplicationToken:                 "token",
				Azcli:                            true,
				MsiAuthentication:                true,
				ManagedServiceIdentity:           "123456",
				InteractiveLogin:                 false,
				RedirectURL:                      "www.google.com",
			},
		},
	}

	for _, test := range tests {
		if isEmpty(test.wantErr) {
			actual := GetConnectionStringBuilder(test.connectionString)
			assert.EqualValues(t, test.want, *actual)
		} else {
			defer func() {
				if res := recover(); res == nil {
					t.Errorf("Should have panic")
				} else if res != test.wantErr {
					t.Errorf("Wrong panic message: %s", res)
				}
			}()
			GetConnectionStringBuilder(test.connectionString)

		}
	}
}

func TestWithAadUserPassAuth(t *testing.T) {
	want := ConnectionStringBuilder{
		DataSource:  "endpoint",
		AadUserID:   "userid",
		Password:    "password",
		AuthorityId: "authorityID",
	}

	actual := GetConnectionStringBuilder("endpoint").WithAadUserPassAuth("userid", "password", "authorityID")
	assert.EqualValues(t, want, *actual)
}

func TestWithAadUserPassAuthErr(t *testing.T) {
	defer func() {
		if res := recover(); res == nil {
			t.Errorf("Should have panic")
		} else if res != "Error: Password cannot be null" {
			t.Errorf("Wrong panic message: %s", res)
		}
	}()
	GetConnectionStringBuilder("endpoint").WithAadUserPassAuth("userid", "", "authorityID")

}

func TestWitAadUserToken(t *testing.T) {
	want := ConnectionStringBuilder{
		DataSource: "endpoint",
		UserToken:  "token",
	}

	actual := GetConnectionStringBuilder("endpoint").WitAadUserToken("token")
	assert.EqualValues(t, want, *actual)
}

func TestWitAadUserTokenErr(t *testing.T) {
	defer func() {
		if res := recover(); res == nil {
			t.Errorf("Should have panic")
		} else if res != "Error: UserToken cannot be null" {
			t.Errorf("Wrong panic message: %s", res)
		}
	}()
	GetConnectionStringBuilder("endpoint").WitAadUserToken("")

}

func TestGetTokenProviderHappy(t *testing.T) {
	s := newTestServ()
	payload := `{"AzureAD": {"LoginEndpoint": "https://login.microsofdummy.com","LoginMfaRequired": false,"KustoClientAppId": "db662dc1-0cfe-4e1c-a843-19a68e65xxxx","KustoClientRedirectUri": "https://microsoft/dummykustoclient","KustoServiceResourceId": "https://kusto.windows.net","FirstPartyAuthorityUrl": "https://login.microsofdummy.com/f8cdef31-a31e-4b4a-93e4-5f571e9xxxxx"  },  "dSTS": {"CloudEndpointSuffix": "windows.net","DstsRealm": "realm://xxx.windows.net","DstsInstance": "xxx.core.windows.net","KustoDnsHostName": "kusto.windows.net","ServiceName": "kusto"}}`
	tests := []struct {
		name    string
		kcsb    ConnectionStringBuilder
		payload string
	}{
		{
			name: "test_tokenprovider_usernamepasswordauth",
			kcsb: ConnectionStringBuilder{
				DataSource:          s.urlStr() + "/test_tokenprovider_usernamepasswordauth",
				AuthorityId:         "tenantID",
				ApplicationClientId: "clientID",
				AadUserID:           "ussername",
				Password:            "userpass",
			},
		}, {
			name: "test_tokenprovider_intLogin",
			kcsb: ConnectionStringBuilder{
				DataSource:          s.urlStr() + "/test_tokenprovider_intLogin",
				InteractiveLogin:    true,
				AuthorityId:         "tenantID",
				ApplicationClientId: "clientID",
			},
		},
		{
			name: "test_tokenprovider_clientsec",
			kcsb: ConnectionStringBuilder{
				DataSource:          s.urlStr() + "/test_tokenprovider_clientsec",
				InteractiveLogin:    true,
				AuthorityId:         "tenantID",
				ApplicationClientId: "clientID",
				ApplicationKey:      "somekey",
			},
		}, {
			name: "test_tokenprovider_managedsi",
			kcsb: ConnectionStringBuilder{
				DataSource:             s.urlStr() + "/test_tokenprovider_managedsi",
				ManagedServiceIdentity: "managedid",
				MsiAuthentication:      true,
				ClientOptions:          &azcore.ClientOptions{},
			},
		}, {
			name: "test_tokenprovider_managedidauth2",
			kcsb: ConnectionStringBuilder{
				DataSource:        s.urlStr() + "/test_tokenprovider_managedidauth2",
				MsiAuthentication: true,
			},
		}, {
			name: "test_tokenprovider_usertoken",
			kcsb: ConnectionStringBuilder{
				DataSource: s.urlStr() + "/test_tokenprovider_usertoken",
				UserToken:  "token",
			},
		}, {
			name: "test_tokenprovider_apptoken",
			kcsb: ConnectionStringBuilder{
				DataSource: s.urlStr() + "/test_tokenprovider_apptoken",
				UserToken:  "token",
			},
		},
	}
	for _, test := range tests {
		kscb := test.kcsb
		s.code = 200
		s.payload = []byte(payload)
		got, err := kscb.getTokenProvider()
		assert.Nil(t, err)
		assert.NotNil(t, got)
	}

}
