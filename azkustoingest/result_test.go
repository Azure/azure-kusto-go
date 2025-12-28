package azkustoingest

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingest/internal/status"
	"github.com/stretchr/testify/assert"
)

type safeSlice[T any] struct {
	mu    sync.Mutex
	items []T
}

func (s *safeSlice[T]) Append(item T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, item)
}

func (s *safeSlice[T]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

func (s *safeSlice[T]) Get(i int) T {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.items[i]
}

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
		var calledTimes safeSlice[time.Duration]

		res := &Result{
			reportToTable: true,
			tableClient: TableClientReaderFunc(func(ctx context.Context, ingestionSourceID string) (map[string]any, error) {
				calledTimes.Append(time.Since(startTime))
				ret := map[string]any{"Status": string(Pending)}
				if calledTimes.Len() >= 3 {
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
		assert.Equal(t, 0, calledTimes.Len(), "Expected no calls to table client")

		time.Sleep(5 * time.Second)
		synctest.Wait()
		assert.Equal(t, 1, calledTimes.Len())
		assert.Equal(t, 5*time.Second, calledTimes.Get(0))

		time.Sleep(5 * time.Second)
		synctest.Wait()
		assert.Equal(t, 2, calledTimes.Len())
		assert.Equal(t, 10*time.Second, calledTimes.Get(1))

		time.Sleep(5 * time.Second)
		synctest.Wait()
		assert.Equal(t, 3, calledTimes.Len())
		assert.Equal(t, 15*time.Second, calledTimes.Get(2))

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
		var calledTimes safeSlice[time.Duration]

		res := &Result{
			reportToTable: true,
			tableClient: TableClientReaderFunc(func(ctx context.Context, ingestionSourceID string) (map[string]any, error) {
				calledTimes.Append(time.Since(startTime))
				return nil, assert.AnError
			}),
			record: statusRecord{
				Status: Pending,
			},
		}

		ch := res.Wait(t.Context(), WithRetryBackoffDelay(1*time.Second, 3*time.Second), WithRetryBackoffJitter(0))
		synctest.Wait()
		assert.Equal(t, 0, calledTimes.Len(), "Expected no calls to table client")

		// First call after DefaultWaitPollInterval (10s)
		time.Sleep(10 * time.Second)
		synctest.Wait()
		assert.Equal(t, 1, calledTimes.Len())
		assert.Equal(t, 10*time.Second, calledTimes.Get(0))

		// Second call after first backoff delay (1s) + poll interval (10s)
		time.Sleep(11 * time.Second)
		synctest.Wait()
		assert.Equal(t, 2, calledTimes.Len())
		assert.Equal(t, 21*time.Second, calledTimes.Get(1))

		// Third call after second backoff delay (3s) + poll interval (10s)
		time.Sleep(13 * time.Second)
		synctest.Wait()
		assert.Equal(t, 3, calledTimes.Len())
		assert.Equal(t, 34*time.Second, calledTimes.Get(2))

		select {
		case err := <-ch:
			assert.NotNil(t, err)
		default:
			assert.FailNow(t, "Expected something to be sent on channel")
		}
	})
}
