package resources

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRankedStorageAccountSet_TestDefaultRank(t *testing.T) {
	r := newDefaultRankedStorageAccountSet()

	// Add 3 accounts
	r.registerStorageAccount("test-account-1")
	r.registerStorageAccount("test-account-2")
	r.registerStorageAccount("test-account-3")

	accounts := r.getRankedShuffledAccounts()

	assert.Len(t, accounts, 3)

	for i := 0; i < len(accounts); i++ {
		assert.EqualValues(t, 1, accounts[i].getRank(), "Expected account %s to have rank 1, but got %f", accounts[i].accountName, accounts[i].getRank())
	}
}

func TestRankedStorageAccountSet_TestRanking(t *testing.T) {
	currentTime := int64(0)
	timeProvider := func() int64 { return currentTime }
	r := newRankedStorageAccountSet(6, 10, []int{90, 70, 30, 0}, timeProvider)

	// Add 3 accounts
	r.registerStorageAccount("test-account-1")
	r.registerStorageAccount("test-account-2")
	r.registerStorageAccount("test-account-3")
	r.registerStorageAccount("test-account-4")
	r.registerStorageAccount("test-account-5")

	// Log results for 60 seconds
	for i := 0; i < 60; i++ {
		r.addAccountResult("test-account-1", true)      // 100% success rate
		r.addAccountResult("test-account-2", i%10 != 0) // 90% success rate
		r.addAccountResult("test-account-3", i%2 == 0)  // 50% success rate
		r.addAccountResult("test-account-4", i%3 == 0)  // 33% success rate
		r.addAccountResult("test-account-5", false)     // 0% success rate
		currentTime++
	}

	//Check if ranking is as expected
	accounts := r.getRankedShuffledAccounts()
	assert.Equal(t, accounts[0].getAccountName(), "test-account-1")                                //tier 1
	assert.Equal(t, accounts[1].getAccountName(), "test-account-2")                                //tier 2
	assert.Contains(t, []string{"test-account-3", "test-account-4"}, accounts[2].getAccountName()) //tier 3
	assert.Contains(t, []string{"test-account-3", "test-account-4"}, accounts[3].getAccountName()) //tier 3
	assert.Equal(t, accounts[4].getAccountName(), "test-account-5")                                // tier 4

	if val, ok := r.getStorageAccount("test-account-1"); ok {
		assert.EqualValues(t, 100, val.getRank()*100)
	}

	if val, ok := r.getStorageAccount("test-account-2"); ok {
		assert.EqualValues(t, 90, math.Round(val.getRank()*100))
	}

	if val, ok := r.getStorageAccount("test-account-3"); ok {
		assert.EqualValues(t, 50, val.getRank()*100)
	}

	if val, ok := r.getStorageAccount("test-account-4"); ok {
		assert.Greater(t, val.getRank()*100, 32.0)
	}

	if val, ok := r.getStorageAccount("test-account-5"); ok {
		assert.EqualValues(t, 0, val.getRank())
	}
}

func TestRankedStorageAccountSet_TestResultsWeightOverTime(t *testing.T) {
	currentTime := int64(0)
	timeProvider := func() int64 { return currentTime }
	r := newRankedStorageAccountSet(6, 10, []int{90, 70, 30, 0}, timeProvider)

	// Add 1 accounts
	r.registerStorageAccount("test-account-1")

	r.addAccountResult("test-account-1", true)
	currentTime += 11
	r.addAccountResult("test-account-1", true)
	currentTime += 11
	r.addAccountResult("test-account-1", true)
	currentTime += 11
	r.addAccountResult("test-account-1", false)
	currentTime += 11
	r.addAccountResult("test-account-1", false)
	currentTime += 11
	r.addAccountResult("test-account-1", false)

	// Rank should be les than 50% as new results are weighted more

	if val, ok := r.getStorageAccount("test-account-1"); ok {
		assert.Less(t, val.getRank(), 50.0)
	} else {
		assert.Fail(t, "test-account-1 not found")
	}
}

func TestRankedStorageAccountSet_TestRankWhenResultsComeInLargeGap(t *testing.T) {
	currentTime := int64(0)
	timeProvider := func() int64 { return currentTime }
	r := newRankedStorageAccountSet(6, 10, []int{90, 70, 30, 0}, timeProvider)

	// Add 1 accounts
	r.registerStorageAccount("test-account-1")

	r.addAccountResult("test-account-1", true)
	currentTime += 11
	r.addAccountResult("test-account-1", true)
	currentTime += 11
	r.addAccountResult("test-account-1", true)

	if val, ok := r.getStorageAccount("test-account-1"); ok {
		assert.Greater(t, val.getRank(), 0.0)
	} else {
		assert.Fail(t, "test-account-1 not found")
	}

	currentTime += 1000

	// Log new results after a large gap
	r.addAccountResult("test-account-1", false)
	// Rank should be 0%
	if val, ok := r.getStorageAccount("test-account-1"); ok {
		assert.EqualValues(t, 0, val.getRank())
	} else {
		assert.Fail(t, "test-account-1 not found")
	}
}

func TestNewRankedStorageAccountSet(t *testing.T) {
	numberOfBuckets := 6
	bucketDuration := int64(10)
	tiers := []int{90, 70, 30, 0}
	timeProvider := func() int64 { return 11 }

	r := newRankedStorageAccountSet(numberOfBuckets, bucketDuration, tiers, timeProvider)

	assert.Equal(t, numberOfBuckets, r.numberOfBuckets, "Expected number_of_buckets to be %d, but got %d", numberOfBuckets, r.numberOfBuckets)
	assert.Equal(t, bucketDuration, r.bucketDuration, "Expected bucket_duration to be %d, but got %d", bucketDuration, r.bucketDuration)
	assert.Len(t, r.tiers, len(tiers), "Expected tiers to have length %d, but got %d", len(tiers), len(r.tiers))

	for i := 0; i < len(tiers); i++ {
		assert.Equal(t, tiers[i], r.tiers[i], "Expected tiers[%d] to be %d, but got %d", i, tiers[i], r.tiers[i])
	}

	assert.Equal(t, timeProvider(), r.timeProvider(), "Expected time_provider to return the same value as the provided function, but got a different value")
}

func TestNewDefaultRankedStorageAccountSet(t *testing.T) {
	r := newDefaultRankedStorageAccountSet()

	assert.EqualValues(t, defaultNumberOfBuckets, r.numberOfBuckets, "Expected number_of_buckets to be %d, but got %d", defaultNumberOfBuckets, r.numberOfBuckets)
	assert.EqualValues(t, defaultBucketDurationInSeconds, r.bucketDuration, "Expected bucket_duration to be %d, but got %d", defaultBucketDurationInSeconds, r.bucketDuration)
	assert.Len(t, r.tiers, len(defaultTiersValue), "Expected tiers to have length %d, but got %d", len(defaultTiersValue), len(r.tiers))

	for i := 0; i < len(defaultTiersValue); i++ {
		assert.EqualValues(t, defaultTiersValue[i], r.tiers[i], "Expected tiers[%d] to be %d, but got %d", i, defaultTiersValue[i], r.tiers[i])
	}
}

func TestRankedStorageAccountSet_AddAccountResult(t *testing.T) {
	r := newDefaultRankedStorageAccountSet()
	accountName := "test-account"

	r.registerStorageAccount(accountName)
	r.addAccountResult(accountName, true)

	account, ok := r.getStorageAccount(accountName)
	if !ok {
		assert.Fail(t, "Expected account %s to be registered, but it was not found", accountName)
	}

	successCount := account.buckets[account.currentBucketIndex].successCount
	totalCount := account.buckets[account.currentBucketIndex].totalCount

	assert.EqualValues(t, 1, successCount, "Expected account %s to have 1 successful request, but got %d", accountName, successCount)
	assert.EqualValues(t, 1, totalCount, "Expected account %s to have 1 total requests, but got %d", accountName, totalCount)
}

func TestRankedStorageAccountSet_GetStorageAccount(t *testing.T) {
	r := newDefaultRankedStorageAccountSet()
	accountName := "test-account"

	account, ok := r.getStorageAccount(accountName)
	if ok {
		assert.Fail(t, "Expected account %s to not be registered, but it was found", accountName)
	}

	if account != nil {
		assert.Fail(t, "Expected account to be nil, but got %+v", account)
	}
}
