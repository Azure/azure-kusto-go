package resources

import (
	"context"
	goErrors "errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata"
	dataErrors "github.com/Azure/azure-kusto-go/azkustodata/errors"
	v1 "github.com/Azure/azure-kusto-go/azkustodata/query/v1"

	"github.com/stretchr/testify/assert"

	"github.com/Azure/azure-kusto-go/azkustodata/types"
	"github.com/Azure/azure-kusto-go/azkustodata/value"
)

type authResponse struct {
	token     string
	err       error
	multiRows bool
}

type authSequenceMgmt struct {
	mu        sync.Mutex
	responses []authResponse
	calls     int
}

func (a *authSequenceMgmt) Mgmt(_ context.Context, _ string, statement azkustodata.Statement, _ ...azkustodata.QueryOption) (v1.Dataset, error) {
	if statement.String() != ".get kusto identity token" {
		return nil, fmt.Errorf("unexpected statement: %s", statement.String())
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.calls++
	if len(a.responses) == 0 {
		return nil, goErrors.New("no responses configured")
	}

	idx := a.calls - 1
	if idx >= len(a.responses) {
		idx = len(a.responses) - 1
	}

	resp := a.responses[idx]
	if resp.err != nil {
		return nil, resp.err
	}

	if resp.multiRows {
		return multiRowAuthDataset(resp.token)
	}

	return authDataset(resp.token)
}

func (a *authSequenceMgmt) callCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

func authDataset(tok string) (v1.Dataset, error) {
	return v1.NewDataset(context.Background(), dataErrors.OpMgmt, v1.V1{Tables: []v1.RawTable{{
		TableName: "Table",
		Columns: []v1.RawColumn{{
			ColumnName: "AuthorizationContext",
			ColumnType: string(types.String),
		}},
		Rows: []v1.RawRow{{Row: []interface{}{tok}}},
	}}})
}

func multiRowAuthDataset(tok string) (v1.Dataset, error) {
	return v1.NewDataset(context.Background(), dataErrors.OpMgmt, v1.V1{Tables: []v1.RawTable{{
		TableName: "Table",
		Columns: []v1.RawColumn{{
			ColumnName: "AuthorizationContext",
			ColumnType: string(types.String),
		}},
		Rows: []v1.RawRow{
			{Row: []interface{}{tok}},
			{Row: []interface{}{tok + "-extra"}},
		},
	}}})
}

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc           string
		url            string
		err            bool
		wantAccount    string
		wantObjectName string
	}{
		{
			desc: "no object name provided",
			url:  "https://account.invalid.core.windows.net/",
			err:  true,
		},
		{
			desc: "bad scheme",
			url:  "http://account.table.core.windows.net/objectname",
			err:  true,
		},
		{
			desc:           "success",
			url:            "https://account.table.core.windows.net/objectname",
			wantAccount:    "account.table.core.windows.net",
			wantObjectName: "objectname",
		},
		{
			desc:           "success non-public",
			url:            "https://account.table.kusto.chinacloudapi.cn/objectname",
			wantAccount:    "account.table.kusto.chinacloudapi.cn",
			wantObjectName: "objectname",
		},
		{
			desc:           "success dns zone",
			url:            "https://account.z01.blob.storage.azure.net/objectname",
			wantAccount:    "account.z01.blob.storage.azure.net",
			wantObjectName: "objectname",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			got, err := Parse(test.url)

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.Equal(t, test.wantAccount, got.Account())
			assert.Equal(t, test.wantObjectName, got.ObjectName())
			assert.Equal(t, test.url, got.String())
		})
	}
}

func FakeAuthContext(rows []value.Values, setErr bool) *FakeMgmt {
	cols := []v1.RawColumn{
		{
			ColumnName: "AuthorizationContext",
			ColumnType: string(types.String),
		},
	}

	fm := NewFakeMgmt(cols, rows, setErr)
	return fm
}

func TestAuthContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc     string
		fakeMgmt *FakeMgmt
		err      bool
		want     string
	}{
		{
			desc: "Mgmt returns an error",
			fakeMgmt: FakeAuthContext(
				[]value.Values{
					{
						value.NewString("authtoken"),
					},
				},
				false,
			).SetMgmtErr(),
			err: true,
		},
		{
			desc: "Returned two rows, only allowed one",
			fakeMgmt: FakeAuthContext(
				[]value.Values{
					{
						value.NewString("authtoken"),
					},
					{
						value.NewString("authtoken2"),
					},
				},
				false,
			),
			err: true,
		},
		{
			desc: "Success",
			fakeMgmt: FakeAuthContext(
				[]value.Values{
					{
						value.NewString("authtoken"),
					},
				},
				false,
			),
			want: "authtoken",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			manager := &Manager{client: test.fakeMgmt, now: func() time.Time { return time.Now().UTC() }}

			got, err := manager.AuthContext(context.Background())

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			assert.Equal(t, test.want, got)
		})
	}
}

func TestAuthContextCachedTokenWithinRefreshInterval(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seq := &authSequenceMgmt{responses: []authResponse{{token: "authtoken-1"}}}
	m := &Manager{
		client: seq,
		now:    func() time.Time { return now },
	}

	got, err := m.AuthContext(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "authtoken-1", got)
	assert.Equal(t, 1, seq.callCount())

	// Within refresh interval - cached token returned, no Mgmt call.
	now = now.Add(authRefreshInterval - time.Second)
	got, err = m.AuthContext(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "authtoken-1", got)
	assert.Equal(t, 1, seq.callCount())
}

func TestAuthContextSuccessfulRefreshReplacesToken(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seq := &authSequenceMgmt{responses: []authResponse{
		{token: "authtoken-1"},
		{token: "authtoken-2"},
	}}
	m := &Manager{
		client: seq,
		now:    func() time.Time { return now },
	}

	got, err := m.AuthContext(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "authtoken-1", got)
	assert.Equal(t, 1, seq.callCount())

	// Exactly at authTokenRefreshTime - refresh triggered, token replaced.
	now = now.Add(authRefreshInterval)
	got, err = m.AuthContext(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "authtoken-2", got)
	assert.Equal(t, 2, seq.callCount())

	// Cached token-2 returned without Mgmt call.
	now = now.Add(time.Minute)
	got, err = m.AuthContext(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "authtoken-2", got)
	assert.Equal(t, 2, seq.callCount())
}

func TestAuthContextRefreshFailureReturnsStaleAndThrottles(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		failResp authResponse
	}{
		{name: "mgmt_error", failResp: authResponse{err: goErrors.New("refresh failed")}},
		{name: "parse_error", failResp: authResponse{token: "ignored", multiRows: true}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			seq := &authSequenceMgmt{responses: []authResponse{
				{token: "authtoken-1"},
				tc.failResp,
			}}
			m := &Manager{
				client: seq,
				now:    func() time.Time { return now },
			}

			got, err := m.AuthContext(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, "authtoken-1", got)

			// Past refresh interval - refresh attempted and fails; stale token returned.
			now = now.Add(authRefreshInterval + time.Second)
			got, err = m.AuthContext(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, "authtoken-1", got)
			assert.Equal(t, 2, seq.callCount())

			// Within long throttle - cached returned, no new Mgmt call.
			now = now.Add(time.Minute)
			got, err = m.AuthContext(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, "authtoken-1", got)
			assert.Equal(t, 2, seq.callCount())
		})
	}
}

func TestAuthContextExpiredStaleReturnsError(t *testing.T) {
	t.Parallel()

	originalStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := originalStart
	seq := &authSequenceMgmt{responses: []authResponse{
		{token: "authtoken-1"},
		{err: goErrors.New("refresh failed")},
		{err: goErrors.New("refresh failed again")},
	}}
	m := &Manager{
		client: seq,
		now:    func() time.Time { return now },
	}

	got, err := m.AuthContext(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "authtoken-1", got)

	// Failed refresh well before validUntil - sets long throttle, returns stale.
	now = now.Add(authRefreshInterval + time.Second)
	got, err = m.AuthContext(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "authtoken-1", got)
	assert.Equal(t, 2, seq.callCount())

	// Exactly at authTokenValidUntil - cached token no longer valid, refresh attempted and fails.
	now = originalStart.Add(authNoRefreshTimeout)
	got, err = m.AuthContext(context.Background())
	assert.Error(t, err)
	assert.Empty(t, got)
	assert.Equal(t, 3, seq.callCount())
}

func TestAuthContextFailureWithoutCachedTokenThrottlesAndRetries(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seq := &authSequenceMgmt{responses: []authResponse{
		{err: goErrors.New("boom")},
		{token: "authtoken-1"},
	}}
	m := &Manager{
		client: seq,
		now:    func() time.Time { return now },
	}

	got, err := m.AuthContext(context.Background())
	assert.Error(t, err)
	assert.Empty(t, got)
	assert.Equal(t, 1, seq.callCount())

	// Within short throttle window - throttled, no Mgmt call.
	now = now.Add(authRetryIntervalWithoutCachedToken - time.Second)
	got, err = m.AuthContext(context.Background())
	assert.Error(t, err)
	assert.Empty(t, got)
	assert.Equal(t, 1, seq.callCount())

	// Exactly at nextAuthRefreshAttempt - retry permitted and succeeds.
	now = now.Add(time.Second)
	got, err = m.AuthContext(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "authtoken-1", got)
	assert.Equal(t, 2, seq.callCount())
}

// TestAuthContextExpiredTokenBypassesLongThrottle exercises the unique branch
// where a cached token expires while a long throttle is still active, forcing
// a refresh attempt despite the throttle.
func TestAuthContextExpiredTokenBypassesLongThrottle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seq := &authSequenceMgmt{responses: []authResponse{
		{token: "authtoken-1"},
		{err: goErrors.New("refresh failed")},
		{err: goErrors.New("refresh failed again")},
		{err: goErrors.New("refresh failed yet again")},
		{token: "authtoken-2"},
	}}
	m := &Manager{
		client: seq,
		now:    func() time.Time { return now },
	}

	got, err := m.AuthContext(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "authtoken-1", got)

	// Refresh fails close to token expiry. Long throttle (15m) extends past
	// authTokenValidUntil (10h) so it remains active after the token expires.
	// authTokenValidUntil = 10:00:00, advance to 09:55:00 -> nextAuthRefreshAttempt = 10:10:00.
	now = now.Add(authNoRefreshTimeout - 5*time.Minute)
	got, err = m.AuthContext(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "authtoken-1", got)
	assert.Equal(t, 2, seq.callCount())

	// Token expired (>10:00:00) but long throttle still active (until 10:10:00).
	// Refresh attempt is made anyway because cached token is no longer valid.
	now = now.Add(7 * time.Minute) // 10:02:00
	got, err = m.AuthContext(context.Background())
	assert.Error(t, err)
	assert.Empty(t, got)
	assert.Equal(t, 3, seq.callCount())

	// After failed bypass, nextAuthRefreshAttempt is now short (3s). Immediate retry is throttled.
	now = now.Add(time.Second)
	got, err = m.AuthContext(context.Background())
	assert.Error(t, err)
	assert.Empty(t, got)
	assert.Equal(t, 3, seq.callCount())

	// Past short throttle, refresh attempted again and fails.
	now = now.Add(authRetryIntervalWithoutCachedToken)
	got, err = m.AuthContext(context.Background())
	assert.Error(t, err)
	assert.Empty(t, got)
	assert.Equal(t, 4, seq.callCount())

	// After short throttle elapses, refresh succeeds.
	now = now.Add(authRetryIntervalWithoutCachedToken)
	got, err = m.AuthContext(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "authtoken-2", got)
	assert.Equal(t, 5, seq.callCount())
}

func TestAuthContextConcurrentCallersTriggerSingleRefresh(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seq := &authSequenceMgmt{responses: []authResponse{{token: "authtoken-1"}}}
	m := &Manager{
		client: seq,
		now:    func() time.Time { return now },
	}

	const workers = 32
	start := make(chan struct{})
	var wg sync.WaitGroup

	errs := make(chan error, workers)
	tokens := make(chan string, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			tk, err := m.AuthContext(context.Background())
			errs <- err
			tokens <- tk
		}()
	}

	close(start)
	wg.Wait()
	close(errs)
	close(tokens)

	for err := range errs {
		assert.NoError(t, err)
	}
	for tk := range tokens {
		assert.Equal(t, "authtoken-1", tk)
	}

	assert.Equal(t, 1, seq.callCount())
}

func mustParse(s string) *URI {
	u, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

func TestResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		desc     string
		fakeMgmt *FakeMgmt
		err      bool
		want     Ingestion
	}{
		{
			desc: "Mgmt returns an error",
			fakeMgmt: FakeResources(
				[]value.Values{},
				false,
			).SetMgmtErr(),
			err: true,
		},
		{
			desc: "Bad StorageRoot value",
			fakeMgmt: FakeResources(
				[]value.Values{
					{
						value.NewString("TempStorage"),
						value.NewString("https://.blob.core.windows.net/"),
					},
				},
				false,
			),
			err: true,
		},
		{
			desc:     "Success",
			fakeMgmt: SuccessfulFakeResources(),
			want: Ingestion{
				Queues:     []*URI{mustParse("https://account.blob.core.windows.net/storageroot1")},
				Containers: []*URI{mustParse("https://account.blob.core.windows.net/storageroot0")},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()

			manager := &Manager{client: test.fakeMgmt, rankedStorageAccount: newDefaultRankedStorageAccountSet()}

			err := manager.fetch(context.Background())

			if test.err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			got, err := manager.getResources()
			assert.NoError(t, err)

			assert.Equal(t, test.want, got)

			containers, err := manager.GetRankedStorageContainers()
			assert.NoError(t, err)
			assert.Equal(t, test.want.Containers, containers)

			queues, err := manager.GetRankedStorageQueues()
			assert.NoError(t, err)
			assert.Equal(t, test.want.Queues, queues)
		})
	}
}
