package azkustoingest

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingest/internal/status"
	"github.com/stretchr/testify/assert"
)

type TableClientReaderFunc func(ctx context.Context, ingestionSourceID string) (map[string]interface{}, error)

func (t TableClientReaderFunc) Read(ctx context.Context, ingestionSourceID string) (map[string]interface{}, error) {
	return t(ctx, ingestionSourceID)
}

var _ status.TableClientReader = (*TableClientReaderFunc)(nil)

func TestWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		res := &Result{
			reportToTable: true,
			tableClient: TableClientReaderFunc(func(ctx context.Context, ingestionSourceID string) (map[string]interface{}, error) {
				assert.FailNow(t, "Expected table client to not be called")
				return nil, nil
			}),
			record: statusRecord{
				Status: Pending,
			},
		}

		ch := res.Wait(t.Context())
		synctest.Wait()

		select {
		case <-ch:
			assert.FailNow(t, "Expected nothing to be sent on channel")
		default:
		}
	})
}

func TestWait_ImmediateFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		res := &Result{
			reportToTable: true,
			tableClient: TableClientReaderFunc(func(ctx context.Context, ingestionSourceID string) (map[string]any, error) {
				return map[string]any{"Status": string(Succeeded)}, nil
			}),
			record: statusRecord{
				Status: Pending,
			},
		}

		ch := res.Wait(t.Context(), WithImmediateFirst())
		synctest.Wait()

		select {
		case <-ch:
		default:
			assert.FailNow(t, "Expected something to be sent on channel")
		}
	})
}

func TestWait_WithInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		startTime := time.Now()
		var calledTimes []time.Duration

		res := &Result{
			reportToTable: true,
			tableClient: TableClientReaderFunc(func(ctx context.Context, ingestionSourceID string) (map[string]any, error) {
				calledTimes = append(calledTimes, time.Now().Sub(startTime))
				ret := map[string]any{"Status": string(Pending)}
				if len(calledTimes) >= 3 {
					ret["Status"] = string(Failed)
				}
				return ret, nil
			}),
			record: statusRecord{
				Status: Pending,
			},
		}

		ch := res.Wait(t.Context(), WithInterval(5*time.Second))
		synctest.Wait()
		assert.Empty(t, calledTimes, "Expected no calls to table client")

		time.Sleep(5 * time.Second)
		synctest.Wait()
		assert.Len(t, calledTimes, 1)
		assert.Equal(t, 5*time.Second, calledTimes[0])

		time.Sleep(5 * time.Second)
		synctest.Wait()
		assert.Len(t, calledTimes, 2)
		assert.Equal(t, 10*time.Second, calledTimes[1])

		time.Sleep(5 * time.Second)
		synctest.Wait()
		assert.Len(t, calledTimes, 3)
		assert.Equal(t, 15*time.Second, calledTimes[2])

		select {
		case <-ch:
		default:
			assert.FailNow(t, "Expected something to be sent on channel")
		}
	})
}

func TestWait_WithRetryBackoffDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		startTime := time.Now()
		var calledTimes []time.Duration

		res := &Result{
			reportToTable: true,
			tableClient: TableClientReaderFunc(func(ctx context.Context, ingestionSourceID string) (map[string]any, error) {
				calledTimes = append(calledTimes, time.Now().Sub(startTime))
				return nil, assert.AnError
			}),
			record: statusRecord{
				Status: Pending,
			},
		}

		ch := res.Wait(t.Context(), WithRetryBackoffDelay(1*time.Second, 3*time.Second), WithRetryBackoffJitter(0))
		synctest.Wait()
		assert.Empty(t, calledTimes, "Expected no calls to table client")

		// First call after DefaultWaitPollInterval (10s)
		time.Sleep(10 * time.Second)
		synctest.Wait()
		assert.Len(t, calledTimes, 1)
		assert.Equal(t, 10*time.Second, calledTimes[0])

		// Second call after first backoff delay (1s) + poll interval (10s)
		time.Sleep(11 * time.Second)
		synctest.Wait()
		assert.Len(t, calledTimes, 2)
		assert.Equal(t, 21*time.Second, calledTimes[1])

		// Third call after second backoff delay (3s) + poll interval (10s)
		time.Sleep(13 * time.Second)
		synctest.Wait()
		assert.Len(t, calledTimes, 3)
		assert.Equal(t, 34*time.Second, calledTimes[2])

		select {
		case err := <-ch:
			assert.NotNil(t, err)
		default:
			assert.FailNow(t, "Expected something to be sent on channel")
		}
	})
}
