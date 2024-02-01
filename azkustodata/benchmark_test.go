package azkustodata

import (
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	queryv2 "github.com/Azure/azure-kusto-go/azkustodata/query/v2"
	"io"
	"strings"
	"testing"
	"time"
)

type StormData struct {
	StartTime        time.Time
	EndTime          time.Time
	EpisodeId        int
	EventId          int
	State            string
	EventType        string
	InjuriesDirect   int
	InjuriesIndirect int
	DeathsDirect     int
	DeathsIndirect   int
	DamageProperty   int
	DamageCrops      int
	Source           string
	BeginLocation    string
	EndLocation      string
	BeginLat         float64
	BeginLon         float64
	EndLat           float64
	EndLon           float64
	EpisodeNarrative string
	EventNarrative   string
	StormSummary     map[string]interface{} // assuming dynamic translates to a map
}

func BenchmarkQuery(b *testing.B) {
	kcsb := NewConnectionStringBuilder("https://help.kusto.windows.net/").WithAzCli()
	client, err := New(kcsb)

	if err != nil {
		panic(err)
	}

	for n := int32(1); n < 100_000; n *= 10 {
		res, err := client.QueryToJson(context.Background(), "Samples", kql.New("StormEvents | limit ").AddInt(n),
			V2FragmentPrimaryTables(), V2NewlinesBetweenFrames(), ResultsErrorReportingPlacement(ResultsErrorReportingPlacementEndOfTable))

		if err != nil {
			panic(err)
		}

		ctx := context.Background()

		b.ResetTimer()

		b.Run(fmt.Sprintf("IterativeQuery %d", n), func(b *testing.B) {
			b.ReportAllocs()

			b.SetParallelism(100)
			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				reader := io.NopCloser(strings.NewReader(res))
				dataset, err := queryv2.NewIterativeDataset(ctx, reader, 5)
				if err != nil {
					panic(err)
				}
				for tableResult := range dataset.Results() {
					if tableResult.Err() != nil {
						panic(tableResult.Err())
					}

					iterative := query.ToStructsIterative[StormData](tableResult.Table())
					for res := range iterative {
						if res.Err != nil {
							panic(res.Err)
						}
						if res.Out.State == "" {
							panic("invalid state")
						}
					}
				}
			}
		})

		b.Run(fmt.Sprintf("IterativeQuery No Buffer -  %d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetParallelism(100)
			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				reader := io.NopCloser(strings.NewReader(res))
				dataset, err := queryv2.NewIterativeDataset(ctx, reader, 0)
				if err != nil {
					panic(err)
				}
				for tableResult := range dataset.Results() {
					if tableResult.Err() != nil {
						panic(tableResult.Err())
					}

					iterative := query.ToStructsIterative[StormData](tableResult.Table())
					for res := range iterative {
						if res.Err != nil {
							panic(res.Err)
						}
						if res.Out.State == "" {
							panic("invalid state")
						}
					}
				}
			}
		})

		b.Run(fmt.Sprintf("Full Dataset %d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetParallelism(100)
			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				reader := io.NopCloser(strings.NewReader(res))
				dataset, err := queryv2.NewFullDataSet(ctx, reader)
				if err != nil {
					panic(err)
				}

				rows, err := dataset.Results()[0].GetAllRows()
				if err != nil {
					panic(err)
				}

				for _, tb := range rows {
					sts, err := query.ToStructs[StormData](tb)
					if err != nil {
						panic(err)
					}
					for _, res := range sts {
						if res.State == "" {
							panic("invalid state")
						}
					}
				}
			}
		})
	}

}
