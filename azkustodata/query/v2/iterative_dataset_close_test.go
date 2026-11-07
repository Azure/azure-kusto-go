package v2

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const stalledPrefix = `[{"FrameType":"DataSetHeader","IsProgressive":false,"Version":"v2.0","IsFragmented":true,"ErrorReportingPlacement":"EndOfTable"}
,{"FrameType":"DataTable","TableId":0,"TableKind":"QueryProperties","TableName":"@ExtendedProperties","Columns":[],"Rows":[]}
`

// stalledBody serves a few frames and then blocks in Read until it is closed.
type stalledBody struct {
	prefix  io.Reader
	blocked chan struct{}
	closed  chan struct{}

	blockedOnce sync.Once
	closeOnce   sync.Once
	closeCalls  int
	mu          sync.Mutex
}

func newStalledBody() *stalledBody {
	return &stalledBody{
		prefix:  io.NopCloser(strings.NewReader(stalledPrefix)),
		blocked: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (b *stalledBody) Read(p []byte) (int, error) {
	n, err := b.prefix.Read(p)
	if n > 0 {
		return n, nil
	}
	if err == io.EOF {
		b.blockedOnce.Do(func() { close(b.blocked) })
		<-b.closed
		return 0, io.ErrClosedPipe
	}
	return n, err
}

func (b *stalledBody) Close() error {
	b.mu.Lock()
	b.closeCalls++
	b.mu.Unlock()
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func TestIterativeDatasetCloseInterruptsStalledRead(t *testing.T) {
	body := newStalledBody()
	d, err := NewIterativeDataset(context.Background(), body, 1, 1, 1)
	require.NoError(t, err)

	select {
	case <-body.blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("reader never reached the stalled read")
	}

	require.NoError(t, d.Close())

	select {
	case <-body.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not close the response body")
	}

	// Closing again, and the read routine's own cleanup, must not close the body twice.
	require.NoError(t, d.Close())
	time.Sleep(50 * time.Millisecond)
	body.mu.Lock()
	defer body.mu.Unlock()
	assert.Equal(t, 1, body.closeCalls)
}
