package utils

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		withInit bool
		failOnce bool
		err      error
	}{
		{
			name:     "Test Success",
			withInit: false,
		},
		{
			name:     "Test Success With Init",
			withInit: true,
		},
		{
			name:     "Test Failure",
			withInit: false,
			err:      errors.New("test"),
		},
		{
			name:     "Test Failure With Init",
			withInit: true,
			err:      errors.New("test"),
		},
		{
			name:     "Test Twice",
			withInit: false,
			failOnce: true,
			err:      errors.New("test"),
		},
		{
			name:     "Test Twice With Init",
			withInit: true,
			failOnce: true,
			err:      errors.New("test"),
		},
	}

	for _, test := range tests {
		test := test // Capture
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			counter := 0
			funErr := test.err
			funFailOnce := test.failOnce

			f := func() (int, error) {
				counter++
				if (funFailOnce && counter == 1) || (!funFailOnce && funErr != nil) {
					return 0, errors.New("test")
				} else {
					return 1, nil
				}
			}

			var result int
			var err error
			var once Once[int]
			if test.withInit {
				once = NewOnce[int]()
			} else {
				once = NewOnceWithInit[int](f)
			}

			expectedOnSuccess := 1

			for i := 0; i < 10; i++ {
				if withInit, ok := once.(OnceWithInit[int]); ok {
					result, err = withInit.DoWithInit()
				} else {
					result, err = once.Do(f)
				}

				isDone, onceResult, onceErr := once.Result()
				if test.err != nil {
					assert.Equal(t, i+1, counter)
					assert.False(t, isDone)
					assert.Equal(t, 0, onceResult)
					assert.Equal(t, test.err, onceErr)
					assert.Equal(t, test.err, err)
				} else {
					assert.Equal(t, counter, expectedOnSuccess)
					assert.True(t, once.Done())
					assert.True(t, isDone)
					assert.Equal(t, 1, onceResult)
					require.NoError(t, err)
					assert.Equal(t, 1, result)
				}

				if test.failOnce {
					test.err = nil
					expectedOnSuccess = 2
				}
			}

		})
	}
}
