package azkustodata

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	queryv2 "github.com/Azure/azure-kusto-go/azkustodata/query/v2"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func getData(k int) string {
	// cache files in bench/test/{k}.json

	if bytes, err := os.ReadFile("d:\\bench\\" + strconv.Itoa(k) + ".json"); err == nil {
		return string(bytes)
	}

	kcsb := NewConnectionStringBuilder("https://help.kusto.windows.net/").WithAzCli()
	client, err := New(kcsb)

	res, err := client.QueryToJson(context.Background(), "Samples", kql.New("StormEvents | take ").AddInt(int32(k)).AddLiteral(" ; StormEvents | take ").AddInt(int32(k)).AddLiteral(" ; StormEvents | take ").AddInt(int32(k)),
		V2FragmentPrimaryTables(), V2NewlinesBetweenFrames(), ResultsErrorReportingPlacement(ResultsErrorReportingPlacementEndOfTable), NoTruncation(), NoRequestTimeout())

	if err != nil {
		panic(err)
	}

	err = os.WriteFile("d:\\bench\\"+strconv.Itoa(k)+".json", []byte(res), 0644)
	if err != nil {
		return ""
	}

	return res

}

func benchmarkIterative(b *testing.B, k int, frameCapacity int, rowCapacity int, fragmentCapacity int) {
	res := getData(k)
	b.ReportAllocs()
	b.ResetTimer()

	//factor := 10000000 / k
	factor := 1

	/*	results, err := os.Create("g:\\bench\\" + strconv.Itoa(k) + ".results.txt")
		if err != nil {
			panic(err)
		}

		defer results.Close()
	*/
	for u := 0; u < b.N; u++ {
		for i := 0; i < factor; i++ {
			ctx := context.Background()
			dataset, err := queryv2.NewIterativeDataset(ctx, io.NopCloser(strings.NewReader(res)), frameCapacity, rowCapacity, fragmentCapacity)

			time.Sleep(5 * time.Second)
			if err != nil {
				panic(err)
			}
			for tableResult := range dataset.Tables() {
				if tableResult.Err() != nil {
					panic(tableResult.Err())
				}

				table := tableResult.Table()
				if !table.IsPrimaryResult() {
					break
				}

				c := int32(0)
				for res := range table.Rows() {
					if res.Err() != nil {
						panic(res.Err())
					}
					id, err := res.Row().IntByName("EventId")
					if err != nil {
						panic(err)
					}
					if id == nil || *id == 0 {
						panic("invalid id")
					}
					//results.WriteString(fmt.Sprintf("%d:%d,", c, *id))
					c++
				}
			}
		}
	}
}

// default values

/*
	func BenchmarkIterative1(b *testing.B) {
		benchmarkIterative(b, 1, queryv2.DefaultFrameCapacity, queryv2.DefaultFragmentCapacity, queryv2.DefaultRowCapacity)
	}

	func BenchmarkIterative10(b *testing.B) {
		benchmarkIterative(b, 10, queryv2.DefaultFrameCapacity, queryv2.DefaultFragmentCapacity, queryv2.DefaultRowCapacity)
	}
*/
func BenchmarkIterative100(b *testing.B) {
	benchmarkIterative(b, 100, 0, queryv2.DefaultRowCapacity, 1)
}

/*
	func BenchmarkIterative1000(b *testing.B) {
		benchmarkIterative(b, 1000, 0, queryv2.DefaultFragmentCapacity, queryv2.DefaultRowCapacity)
	}
*/
func BenchmarkIterative10000(b *testing.B) {
	benchmarkIterative(b, 10000, 0, queryv2.DefaultRowCapacity, 1)
}

func BenchmarkIterative118132(b *testing.B) {
	benchmarkIterative(b, 118132, 0, queryv2.DefaultRowCapacity, 1)
}

/*
	func BenchmarkIterative1000000(b *testing.B) {
		benchmarkIterative(b, 1000000, queryv2.DefaultFrameCapacity, queryv2.DefaultFragmentCapacity, queryv2.DefaultRowCapacity)
	}

	func BenchmarkIterative10000000(b *testing.B) {
		benchmarkIterative(b, 10000000, queryv2.DefaultFrameCapacity, queryv2.DefaultFragmentCapacity, queryv2.DefaultRowCapacity)
	}
*/
func BenchmarkIterativeNoBuffer100(b *testing.B) {
	benchmarkIterative(b, 100, 0, 0, 1)
}

func BenchmarkIterativeNoBuffer10000(b *testing.B) {
	benchmarkIterative(b, 10000, 0, 0, 1)
}

func BenchmarkIterativeNoBuffer118132(b *testing.B) {
	benchmarkIterative(b, 118132, 0, 0, 1)
}

/*func BenchmarkIterativeNoBuffer1000000(b *testing.B) {
	benchmarkIterative(b, 1000000, 0, 0, 0)
}
*/
/*func BenchmarkIterativeNoBuffer10000000(b *testing.B) {
	benchmarkIterative(b, 10000000, 0, 0, 0)
}
*/
func BenchmarkIterativeOneBuffer100(b *testing.B) {
	benchmarkIterative(b, 100, 0, 1, 1)
}
func BenchmarkIterativeOneBuffer10000(b *testing.B) {
	benchmarkIterative(b, 10000, 0, 1, 1)
}

func BenchmarkIterativeOneBuffer118132(b *testing.B) {
	benchmarkIterative(b, 118132, 0, 1, 1)
}

/*func BenchmarkIterativeOneBuffer1000000(b *testing.B) {
	benchmarkIterative(b, 1000000, 1, 1, 1)
}
*/
/*func BenchmarkIterativeOneBuffer10000000(b *testing.B) {
	benchmarkIterative(b, 10000000, 1, 1, 1)
}
*/
func BenchmarkIterativeBigBuffer100(b *testing.B) {
	benchmarkIterative(b, 100, 0, queryv2.DefaultRowCapacity*100, 1)
}
func BenchmarkIterativeBigBuffer10000(b *testing.B) {
	benchmarkIterative(b, 10000, 0, queryv2.DefaultRowCapacity*100, 1)
}
func BenchmarkIterativeBigBuffer118132(b *testing.B) {
	benchmarkIterative(b, 118132, 0, queryv2.DefaultRowCapacity*100, 1)
}

/*func BenchmarkIterativeBigBuffer1000000(b *testing.B) {
	benchmarkIterative(b, 1000000, queryv2.DefaultFrameCapacity*100, queryv2.DefaultFragmentCapacity*100, queryv2.DefaultRowCapacity*100)
}
*/
/*func BenchmarkIterativeBigBuffer10000000(b *testing.B) {
	benchmarkIterative(b, 10000000, queryv2.DefaultFrameCapacity*100, queryv2.DefaultFragmentCapacity*100, queryv2.DefaultRowCapacity*100)
}
*/
/*func BenchmarkFull1(b *testing.B)       { benchmarkFull(b, 1) }
func BenchmarkFull10(b *testing.B)      { benchmarkFull(b, 10) }
func BenchmarkFull100(b *testing.B)     { benchmarkFull(b, 100) }
func BenchmarkFull1000(b *testing.B)    { benchmarkFull(b, 1000) }
func BenchmarkFull10000(b *testing.B)   { benchmarkFull(b, 10000) }
func BenchmarkFull100000(b *testing.B)  { benchmarkFull(b, 100000) }
func BenchmarkFull1000000(b *testing.B) { benchmarkFull(b, 1000000) }
*/
