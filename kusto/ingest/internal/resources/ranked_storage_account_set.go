package resources

import (
	"math/rand"
	"sync"
	"time"
)

const (
	defaultNumberOfBuckets         = 6
	defaultBucketDurationInSeconds = 10
)

var defaultTiersValue = [4]int{90, 70, 30, 0}
var defaultTimeProvider = func() int64 { return time.Now().Unix() }

type RankedStorageAccountSet struct {
	accounts          map[string]*RankedStorageAccount
	number_of_buckets int
	bucket_duration   int64
	tiers             []int
	time_provider     func() int64
	lock              sync.Mutex
}

func newRankedStorageAccountSet(
	number_of_buckets int,
	bucket_duration int64,
	tiers []int,
	time_provider func() int64,
) *RankedStorageAccountSet {
	return &RankedStorageAccountSet{
		accounts:          make(map[string]*RankedStorageAccount),
		number_of_buckets: number_of_buckets,
		bucket_duration:   bucket_duration,
		tiers:             tiers,
		time_provider:     time_provider,
		lock:              sync.Mutex{},
	}
}

func newDefaultRankedStorageAccountSet() *RankedStorageAccountSet {
	return newRankedStorageAccountSet(defaultNumberOfBuckets, defaultBucketDurationInSeconds, defaultTiersValue[:], defaultTimeProvider)
}

func (r *RankedStorageAccountSet) addAccountResult(accountName string, success bool) {
	r.lock.Lock()
	defer r.lock.Unlock()

	account, ok := r.accounts[accountName]
	if ok {
		account.logResult(success)
	}
}

func (r *RankedStorageAccountSet) registerStorageAccount(accountName string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if _, ok := r.accounts[accountName]; !ok {
		r.accounts[accountName] = newRankedStorageAccount(accountName, r.number_of_buckets, r.bucket_duration, r.time_provider)
	}
}

func (r *RankedStorageAccountSet) getStorageAccount(accountName string) (*RankedStorageAccount, bool) {
	r.lock.Lock()
	defer r.lock.Unlock()

	account, ok := r.accounts[accountName]
	return account, ok
}

func (r *RankedStorageAccountSet) getRankedShuffledAccounts() []RankedStorageAccount {
	r.lock.Lock()
	defer r.lock.Unlock()

	accountsByTier := make([][]RankedStorageAccount, len(r.tiers))
	for i := range accountsByTier {
		accountsByTier[i] = []RankedStorageAccount{}
	}

	for _, account := range r.accounts {
		rankPercentage := int(account.getRank() * 100.0)
		for i := range r.tiers {
			if rankPercentage >= r.tiers[i] {
				accountsByTier[i] = append(accountsByTier[i], *account)
				break
			}
		}
	}

	for _, tier := range accountsByTier {
		rand.Shuffle(len(tier), func(i, j int) {
			tier[i], tier[j] = tier[j], tier[i]
		})
	}

	var result []RankedStorageAccount
	for _, sublist := range accountsByTier {
		result = append(result, sublist...)
	}

	return result
}
