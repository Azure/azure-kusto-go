package etoe

import (
	"testing"
)

func TestIgestionWithoutStatusReporting(t *testing.T) {
	testConfig, err := NewConfig()
	if err != nil {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	// TODO do some testing instead
	if testConfig.Endpoint == "" {
		t.FailNow()
	}
}

func TestIgestionWithStatusReporting(t *testing.T) {
	testConfig, err := NewConfig()
	if err != nil {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	// TODO do some testing instead
	if testConfig.Endpoint == "" {
		t.FailNow()
	}
}

func TestIgestionWithFailedStatusReporting(t *testing.T) {
	testConfig, err := NewConfig()
	if err != nil {
		t.Skipf("end to end tests disabled: missing config.json file in etoe directory")
	}

	// TODO do some testing instead
	if testConfig.Endpoint == "" {
		t.FailNow()
	}
}
