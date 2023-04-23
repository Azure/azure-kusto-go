package v2

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/internal/frames"
)

// Current:
// BenchmarkDecode-16    	    1174	    985828 ns/op	  602654 B/op	   10,688 allocs/op (using map)
// New:
// BenchmarkDecode-16    	     374	   3106952 ns/op	 1464426 B/op	   32,642 allocs/op
// BenchmarkDecode-16    	     837	   1356686 ns/op	  514834 B/op	    8448 allocs/op (removing RawMessage from Rows and extra decoder for [][]interface{}, plus RawMessage resuse)
// BenchmarkDecode-16    	     950	   1189885 ns/op	  364138 B/op	    6614 allocs/op // (added reuse of [][]interface{})
// BenchmarkDecode-16    	     924	   1223022 ns/op	  354568 B/op	    6501 allocs/op // json.Number change to Unmarshal

func BenchmarkDecode(b *testing.B) {
	b.ReportAllocs()
	stream, err := os.ReadFile("./testdata/stream_small.json")
	if err != nil {
		b.SkipNow() // We don't want to use our current testdata file, but want to keep the benchmark.
		return
	}

	dec := Decoder{}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		r := io.NopCloser(bytes.NewReader(stream))
		framesCh := dec.Decode(context.Background(), r, errors.OpQuery)

		for fr := range framesCh {
			if ef, ok := fr.(frames.Error); ok {
				panic(ef.Error())
			}
		}
	}
}

// Current:
// BenchmarkGetFrameType-16    	 4642632	       256 ns/op	     176 B/op	       3 allocs/op (split loops)
// New:
// BenchmarkGetFrameType-16    	 1845040	       581 ns/op	      48 B/op	       1 allocs/op (regex)
// Newest:
// BenchmarkGetFrameType-16    	 8389330	       149 ns/op	      64 B/op	       2 allocs/op
// New Newest:
// BenchmarkGetFrameType-16    	12634101	        82.7 ns/op	      32 B/op	       1 allocs/op
func BenchmarkGetFrameType(b *testing.B) {
	stream := []byte(`
	{
		"FrameType":"dataSetHeader",
		"IsProgressive":false,
		"Version":"v2.0"
	},
	`)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := getFrameType(stream)
		if err != nil {
			panic(err)
		}
	}
}
