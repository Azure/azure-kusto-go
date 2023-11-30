package resources

import (
	"math/rand"
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
	}
}

func newDefaultRankedStorageAccountSet() *RankedStorageAccountSet {
	return newRankedStorageAccountSet(defaultNumberOfBuckets, defaultBucketDurationInSeconds, defaultTiersValue[:], defaultTimeProvider)
}

func (r *RankedStorageAccountSet) addAccountResult(accountName string, success bool) {
	account, ok := r.accounts[accountName]
	if ok {
		account.logResult(success)
	}
}

func (r *RankedStorageAccountSet) registerStorageAccount(accountName string) {
	if _, ok := r.accounts[accountName]; !ok {
		r.accounts[accountName] = newRankedStorageAccount(accountName, r.number_of_buckets, r.bucket_duration, r.time_provider)
	}
}

func (r *RankedStorageAccountSet) getStorageAccount(accountName string) (*RankedStorageAccount, bool) {
	account, ok := r.accounts[accountName]
	return account, ok
}

func (r *RankedStorageAccountSet) getRankedShuffledAccounts() []*RankedStorageAccount {
	accountsByTier := make([][]*RankedStorageAccount, len(r.tiers))
	for i := range accountsByTier {
		accountsByTier[i] = []*RankedStorageAccount{}
	}

	for _, account := range r.accounts {
		rankPercentage := int(account.getRank() * 100.0)
		for i := range r.tiers {
			if rankPercentage >= r.tiers[i] {
				accountsByTier[i] = append(accountsByTier[i], account)
				break
			}
		}
	}

	for _, tier := range accountsByTier {
		rand.Shuffle(len(tier), func(i, j int) {
			tier[i], tier[j] = tier[j], tier[i]
		})
	}

	var result []*RankedStorageAccount
	for _, sublist := range accountsByTier {
		result = append(result, sublist...)
	}

	return result
}
