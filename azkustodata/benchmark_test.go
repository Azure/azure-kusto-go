package azkustodata

import (
	"context"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	queryv2 "github.com/Azure/azure-kusto-go/azkustodata/query/v2"
	"io"
	"strings"
	"testing"
)

func getData(k int) (string, context.Context) {
	kcsb := NewConnectionStringBuilder("https://help.kusto.windows.net/").WithAzCli()
	client, err := New(kcsb)

	res, err := client.QueryToJson(context.Background(), "Samples", kql.New("StormEvents | limit ").AddInt(int32(k)),
		V2FragmentPrimaryTables(), V2NewlinesBetweenFrames(), ResultsErrorReportingPlacement(ResultsErrorReportingPlacementEndOfTable))

	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	return res, ctx

}

func benchmarkIterative(b *testing.B, k int) {
	res, ctx := getData(k)
	b.ReportAllocs()
	b.ResetTimer()

	for k := 0; k < b.N; k++ {
		dataset, err := queryv2.NewIterativeDataset(ctx, io.NopCloser(strings.NewReader(res)), 5)
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
				c++
			}
		}
	}
}
func benchmarkFull(b *testing.B, k int) {
	res, ctx := getData(k)
	b.ReportAllocs()
	b.ResetTimer()

	for k := 0; k < b.N; k++ {
		dataset, err := queryv2.NewIterativeDataset(ctx, io.NopCloser(strings.NewReader(res)), 5)
		if err != nil {
			panic(err)
		}

		full, err := dataset.ToFullDataset()
		if err != nil {
			panic(err)
		}

		for _, table := range full.Tables() {
			if !table.IsPrimaryResult() {
				break
			}

			c := int32(0)
			for _, res := range table.Rows() {
				id, err := res.IntByName("EventId")
				if err != nil {
					panic(err)
				}
				if id == nil || *id == 0 {
					panic("invalid id")
				}
				c++
			}
		}
	}
}

func BenchmarkIterative1(b *testing.B)      { benchmarkIterative(b, 1) }
func BenchmarkIterative10(b *testing.B)     { benchmarkIterative(b, 10) }
func BenchmarkIterative100(b *testing.B)    { benchmarkIterative(b, 100) }
func BenchmarkIterative1000(b *testing.B)   { benchmarkIterative(b, 1000) }
func BenchmarkIterative10000(b *testing.B)  { benchmarkIterative(b, 10000) }
func BenchmarkIterative100000(b *testing.B) { benchmarkIterative(b, 100000) }

func BenchmarkIterative1000000(b *testing.B) { benchmarkIterative(b, 1000000) }

func BenchmarkFull1(b *testing.B)       { benchmarkFull(b, 1) }
func BenchmarkFull10(b *testing.B)      { benchmarkFull(b, 10) }
func BenchmarkFull100(b *testing.B)     { benchmarkFull(b, 100) }
func BenchmarkFull1000(b *testing.B)    { benchmarkFull(b, 1000) }
func BenchmarkFull10000(b *testing.B)   { benchmarkFull(b, 10000) }
func BenchmarkFull100000(b *testing.B)  { benchmarkFull(b, 100000) }
func BenchmarkFull1000000(b *testing.B) { benchmarkFull(b, 1000000) }
