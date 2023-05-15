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
		useTwice bool
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
			err:      errors.New("test"),
		},
		{
			name:     "Test Twice With Init",
			withInit: true,
			err:      errors.New("test"),
		},
	}

	for _, test := range tests {
		test := test // Capture
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var f func() (int, error)
			if test.useTwice {
				counter := 0
				f = func() (int, error) {
					if counter == 0 {
						counter++
						return 0, errors.New("test")
					} else {
						return 1, nil
					}
				}
			} else if test.err != nil {
				f = func() (int, error) {
					return 0, errors.New("test")
				}
			} else {
				f = func() (int, error) {
					return 1, nil
				}
			}

			var result int
			var err error
			var once Once[int]
			if test.withInit {
				once = NewOnce[int]()
				result, err = once.Do(f)
			} else {
				onceWithInit := NewOnceWithInit[int](f)
				result, err = onceWithInit.DoWithInit()
				once = onceWithInit
			}

			if test.useTwice {
				isDone, onceResult, onceErr := once.Result()
				assert.False(t, isDone)
				assert.Equal(t, 0, onceResult)
				assert.Equal(t, test.err, onceErr)
				assert.Equal(t, test.err, err)
				if test.withInit {
					result, err = once.Do(f)
				} else {
					result, err = once.(OnceWithInit[int]).DoWithInit()
				}
				test.err = nil
			}

			isDone, onceResult, onceErr := once.Result()
			if test.err != nil {
				assert.False(t, isDone)
				assert.Equal(t, 0, onceResult)
				assert.Equal(t, test.err, onceErr)
				assert.Equal(t, test.err, err)
			} else {
				assert.True(t, once.Done())
				assert.True(t, isDone)
				assert.Equal(t, 1, onceResult)
				require.NoError(t, err)
				assert.Equal(t, 1, result)
			}
		})
	}
}
