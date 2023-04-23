package truestedEndpoints

import (
	"fmt"
	"github.com/samber/lo"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const defaultPublicLoginUrl = "https://login.microsoftonline.com"
const chinaCloudLoginUrl = "https://login.partner.microsoftonline.cn"

func validateEndpoint(address string, login_endpoint string) error {
	return Instance.ValidateTrustedEndpoint(address, login_endpoint)
}

func checkEndpoint(clusterName string, defaultPublicLoginUrl string, expectFail bool) error {
	if expectFail {
		err := validateEndpoint(clusterName, defaultPublicLoginUrl)
		if strings.Contains(err.Error(), "Can't communicate with") {
			return nil
		}
		return err

	} else {
		return validateEndpoint(clusterName, defaultPublicLoginUrl)
	}
}

func TestTrustedEndpoints_RandomKustoClusters(t *testing.T) {
	for _, c := range []string{
		"https://127.0.0.1",
		"https://127.1.2.3",
		"https://kustozszokb5yrauyq.westeurope.kusto.windows.net",
		"https://kustofrbwrznltavls.centralus.kusto.windows.net",
		"https://kusto7j53clqswr4he.germanywestcentral.kusto.windows.net",
		"https://rpe2e0422132101fct2.eastus2euap.kusto.windows.net",
		"https://kustooq2gdfraeaxtq.westcentralus.kusto.windows.net",
		"https://kustoesp3ewo4s5cow.westcentralus.kusto.windows.net",
		"https://kustowmd43nx4ihnjs.southeastasia.kusto.windows.net",
		"https://createt210723t0601.westus2.kusto.windows.net",
		"https://kusto2rkgmaskub3fy.eastus2.kusto.windows.net",
		"https://kustou7u32pue4eij4.australiaeast.kusto.windows.net",
		"https://kustohme3e2jnolxys.northeurope.kusto.windows.net",
		"https://kustoas7cx3achaups.southcentralus.kusto.windows.net",
		"https://rpe2e0104160100act.westus2.kusto.windows.net",
		"https://kustox5obddk44367y.southcentralus.kusto.windows.net",
		"https://kustortnjlydpe5l6u.canadacentral.kusto.windows.net",
		"https://kustoz74sj7ikkvftk.southeastasia.kusto.windows.net",
		"https://rpe2e1004182350fctf.westus2.kusto.windows.net",
		"https://rpe2e1115095448act.westus2.kusto.windows.net",
		"https://kustoxenx32x3tuznw.southafricawest.kusto.windows.net",
		"https://kustowc3m5jpqtembw.canadacentral.kusto.windows.net",
		"https://rpe2e1011182056fctf.westus2.kusto.windows.net",
		"https://kusto3ge6xthiafqug.eastus.kusto.windows.net",
		"https://teamsauditservice.westus.kusto.windows.net",
		"https://kustooubnzekmh4doy.canadacentral.kusto.windows.net",
		"https://rpe2e1206081632fct2f.westus2.kusto.windows.net",
		"https://stopt402211020t0606.automationtestworkspace402.kusto.azuresynapse.net",
		"https://delt402210818t2309.automationtestworkspace402.kusto.azuresynapse.net",
		"https://kusto42iuqj4bejjxq.koreacentral.kusto.windows.net",
		"https://kusto3rv75hibmg6vu.southeastasia.kusto.windows.net",
		"https://kustogmhxb56nqjrje.westus2.kusto.windows.net",
		"https://kustozu5wg2p3aw3um.koreasouth.kusto.windows.net",
		"https://kustos36f2amn2agwk.australiaeast.kusto.windows.net",
		"https://kustop4htq3k676jau.eastus.kusto.windows.net",
		"https://kustojdny5lga53cts.southcentralus.kusto.windows.net",
		"https://customerportalprodeast.kusto.windows.net",
		"https://rpe2e0730231650und.westus2.kusto.windows.net",
		"https://kusto7lxdbebadivjw.southeastasia.kusto.windows.net",
		"https://alprd2neu000003s.northeurope.kusto.windows.net",
		"https://kustontnwqy3eler5g.northeurope.kusto.windows.net",
		"https://kustoap2wpozj7qpio.eastus.kusto.windows.net",
		"https://kustoajnxslghxlee4.japaneast.kusto.windows.net",
		"https://oiprdseau234x.australiasoutheast.kusto.windows.net",
		"https://kusto7yevbo7ypsnx4.germanywestcentral.kusto.windows.net",
		"https://kustoagph5odbqyquq.westus3.kusto.windows.net",
		"https://kustovs2hxo3ftud5e.westeurope.kusto.windows.net",
		"https://kustorzuk2dgiwdryc.uksouth.kusto.windows.net",
		"https://kustovsb4ogsdniwqk.eastus2.kusto.windows.net",
		"https://kusto3g3mpmkm3p3xc.switzerlandnorth.kusto.windows.net",
		"https://kusto2e2o7er7ypx2o.westus2.kusto.windows.net",
		"https://kustoa3qqlh23yksim.southafricawest.kusto.windows.net",
		"https://rpe2evnt11021711comp.rpe2evnt11021711-wksp.kusto.azuresynapse.net",
		"https://cdpkustoausas01.australiasoutheast.kusto.windows.net",
		"https://testinge16cluster.uksouth.kusto.windows.net",
		"https://testkustopoolbs6ond.workspacebs6ond.kusto.azuresynapse.net",
		"https://offnodereportingbcdr1.southcentralus.kusto.windows.net",
		"https://mhstorage16red.westus.kusto.windows.net",
		"https://kusto7kza5q2fmnh2w.northeurope.kusto.windows.net",
		"https://tvmquerycanc.centralus.kusto.windows.net",
		"https://kustowrcde4olp4zho.eastus.kusto.windows.net",
		"https://delt403210910t0727.automationtestworkspace403.kusto.azuresynapse.net",
		"https://foprdcq0004.brazilsouth.kusto.windows.net",
		"https://rpe2e0827133746fctf.eastus2euap.kusto.windows.net",
		"https://kustoz7yrvoaoa2yaa.australiaeast.kusto.windows.net",
		"https://rpe2e1203125809und.westus2.kusto.windows.net",
		"https://kustoywilbpggrltk4.francecentral.kusto.windows.net",
		"https://stopt402210825t0408.automationtestworkspace402.kusto.azuresynapse.net",
		"https://kustonryfjo5klvrh4.westeurope.kusto.windows.net",
		"https://kustowwqgogzpseg6o.eastus2.kusto.windows.net",
		"https://kustor3gjpwqum3olw.canadacentral.kusto.windows.net",
		"https://dflskfdslfkdslkdsfldfs.westeurope.kusto.data.microsoft.com",
		"https://dflskfdslfkdslkdsfldfs.westeurope.kusto.fabric.microsoft.com",
	} {
		err := validateEndpoint(c, defaultPublicLoginUrl)
		require.NoError(t, err)

		// Test case sensitivity
		clusterName := strings.ToUpper(c)
		err = validateEndpoint(clusterName, defaultPublicLoginUrl)
		require.NoError(t, err)

		specialUrls := []string{
			"synapse",
			"data.microsoft.com",
			"fabric.microsoft.com",
		}

		// Test MFA endpoints
		if lo.NoneBy(specialUrls, func(s string) bool { return strings.Contains(c, s) }) {
			clusterName = strings.Replace(c, ".kusto.", ".kustomfa.", 1)
			err = validateEndpoint(clusterName, defaultPublicLoginUrl)
			require.NoError(t, err)
		}

		// Test dev endpoints
		if lo.NoneBy(specialUrls, func(s string) bool { return strings.Contains(c, s) }) {
			clusterName = strings.Replace(c, ".kusto.", ".kustodev.", 1)
			err = validateEndpoint(clusterName, defaultPublicLoginUrl)
			require.NoError(t, err)
		}
	}
}

func TestWellTrustedEndpoints_NationalClouds(t *testing.T) {
	for _, c := range []string{
		fmt.Sprintf("https://kustozszokb5yrauyq.kusto.chinacloudapi.cn,%s", chinaCloudLoginUrl),
		"https://kustofrbwrznltavls.kusto.usgovcloudapi.net,https://login.microsoftonline.us",
		"https://kusto7j53clqswr4he.kusto.core.eaglex.ic.gov,https://login.microsoftonline.eaglex.ic.gov",
		"https://rpe2e0422132101fct2.kusto.core.microsoft.scloud,https://login.microsoftonline.microsoft.scloud",
		fmt.Sprintf("https://kustozszokb5yrauyq.kusto.chinacloudapi.cn,%s", chinaCloudLoginUrl),
		"https://kustofrbwrznltavls.kusto.usgovcloudapi.net,https://login.microsoftonline.us",
		"https://kusto7j53clqswr4he.kusto.core.eaglex.ic.gov,https://login.microsoftonline.eaglex.ic.gov",
		"https://rpe2e0422132101fct2.kusto.core.microsoft.scloud,https://login.microsoftonline.microsoft.scloud",
	} {
		clusterAndLoginEndpoint := strings.Split(c, ",")
		err := validateEndpoint(clusterAndLoginEndpoint[0], clusterAndLoginEndpoint[1])
		require.NoError(t, err)
		// Test case sensitivity
		err = validateEndpoint(strings.ToUpper(clusterAndLoginEndpoint[0]), strings.ToUpper(clusterAndLoginEndpoint[1]))
		require.NoError(t, err)
	}
}

func TestWellTrustedEndpoints_ProxyTest(t *testing.T) {
	for _, c := range []string{
		fmt.Sprintf("https://kustozszokb5yrauyq.kusto.chinacloudapi.cn,%s", chinaCloudLoginUrl),
		fmt.Sprintf("https://kusto.aria.microsoft.com,%s", defaultPublicLoginUrl),
		fmt.Sprintf("https://ade.loganalytics.io,%s", defaultPublicLoginUrl),
		fmt.Sprintf("https://ade.applicationinsights.io,%s", defaultPublicLoginUrl),
		fmt.Sprintf("https://kusto.aria.microsoft.com,%s", defaultPublicLoginUrl),
		fmt.Sprintf("https://adx.monitor.azure.com,%s", defaultPublicLoginUrl),
		fmt.Sprintf("https://cluster.playfab.com,%s", defaultPublicLoginUrl),
		fmt.Sprintf("https://cluster.playfabapi.com,%s", defaultPublicLoginUrl),
		fmt.Sprintf("https://cluster.playfab.cn,%s", chinaCloudLoginUrl),
	} {
		clusterAndLoginEndpoint := strings.Split(c, ",")
		err := validateEndpoint(clusterAndLoginEndpoint[0], clusterAndLoginEndpoint[1])
		require.NoError(t, err)
		// Test case sensitivity
		err = validateEndpoint(strings.ToUpper(clusterAndLoginEndpoint[0]), strings.ToUpper(clusterAndLoginEndpoint[1]))
		require.NoError(t, err)
	}
}

func TestWellTrustedEndpoints_ProxyNegativeTest(t *testing.T) {
	for _, c := range []string{
		"https://cluster.kusto.aria.microsoft.com",
		"https://cluster.eu.kusto.aria.microsoft.com",
		"https://cluster.ade.loganalytics.io",
		"https://cluster.ade.applicationinsights.io",
		"https://cluster.adx.monitor.azure.com",
		"https://cluster.adx.applicationinsights.azure.cn",
		"https://cluster.adx.monitor.azure.eaglex.ic.gov",
	} {
		err := checkEndpoint(c, defaultPublicLoginUrl, true)
		require.NoError(t, err)
	}
}

func TestWellTrustedEndpoints_EndpointsNegative(t *testing.T) {
	for _, c := range []string{
		"https://localhostess",
		"https://127.0.0.1.a",
		"https://some.azurewebsites.net",
		"https://kusto.azurewebsites.net",
		"https://test.kusto.core.microsoft.scloud",
		"https://cluster.kusto.azuresynapse.azure.cn",
	} {
		err := checkEndpoint(c, defaultPublicLoginUrl, true)
		require.NoError(t, err)
	}
}

func TestWellTrustedEndpoints_EndpointsOverride(t *testing.T) {
	defer Instance.SetOverridePolicy(nil)

	Instance.SetOverridePolicy(func(host string) bool {
		return true
	})
	err := checkEndpoint("https://kusto.kusto.windows.net", "", false)
	require.NoError(t, err)
	err = checkEndpoint("https://bing.com", "", false)
	require.NoError(t, err)

	Instance.SetOverridePolicy(func(host string) bool {
		return false
	})
	err = checkEndpoint("https://kusto.kusto.windows.net", "", true)
	require.NoError(t, err)
	err = checkEndpoint("https://bing.com", "", true)
	require.NoError(t, err)

	Instance.SetOverridePolicy(nil)
	err = checkEndpoint("https://kusto.kusto.windows.net", defaultPublicLoginUrl, false)
	require.NoError(t, err)
	err = checkEndpoint("https://bing.com", defaultPublicLoginUrl, true)
	require.NoError(t, err)
}

func TestWellTrustedEndpoints_AdditionalWebsites(t *testing.T) {
	Instance.AddTrustedHosts([]MatchRule{{suffix: ".someotherdomain1.net", exact: false}}, true)

	// 2nd call - to validate that addition works
	Instance.AddTrustedHosts([]MatchRule{{suffix: "www.someotherdomain2.net", exact: true}}, false)
	Instance.AddTrustedHosts([]MatchRule{{suffix: "www.someotherdomain3.net", exact: true}}, false)

	for _, clusterName := range []string{"https://some.someotherdomain1.net", "https://www.someotherdomain2.net"} {
		err := checkEndpoint(clusterName, defaultPublicLoginUrl, false)
		require.NoError(t, err)
	}

	err := checkEndpoint("https://some.someotherdomain2.net", defaultPublicLoginUrl, true)
	require.NoError(t, err)

	// Reset additional hosts
	Instance.AddTrustedHosts(nil, true)
	// Validate that hosts are not allowed anymore
	for _, clusterName := range []string{"https://some.someotherdomain1.net", "https://www.someotherdomain2.net"} {
		err := checkEndpoint(clusterName, defaultPublicLoginUrl, true)
		require.NoError(t, err)
	}
}
