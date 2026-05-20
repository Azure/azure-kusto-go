package utils

import (
	"testing"

	"github.com/Azure/azure-kusto-go/azkustoingest/ingestoptions"
	"github.com/stretchr/testify/assert"
)

func TestEstimateRawDataSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		compression ingestoptions.CompressionType
		fileSize    int64
		want        int64
	}{
		{
			name:        "GZIP applies compression factor",
			compression: ingestoptions.GZIP,
			fileSize:    100,
			want:        100 * EstimatedCompressionFactor,
		},
		{
			name:        "ZIP applies compression factor",
			compression: ingestoptions.ZIP,
			fileSize:    100,
			want:        100 * EstimatedCompressionFactor,
		},
		{
			name:        "CTNone returns original size",
			compression: ingestoptions.CTNone,
			fileSize:    100,
			want:        100,
		},
		{
			name:        "CTUnknown returns original size",
			compression: ingestoptions.CTUnknown,
			fileSize:    100,
			want:        100,
		},
		{
			name:        "zero size returns zero",
			compression: ingestoptions.GZIP,
			fileSize:    0,
			want:        0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := EstimateRawDataSize(tt.compression, tt.fileSize)
			assert.Equal(t, tt.want, got)
		})
	}
}
