package utils

import "fmt"

// ErrorHandler Error handling function. Will mention the appropriate error message (and the exception itself if exists), and will quit the program.
func ErrorHandler(errorMsg string, e error) {
	fmt.Printf("\nScript failed with error: %s\n", errorMsg)
	if e != nil {
		panic(fmt.Sprintf("Exception: '%s'\n", e))
	}
}
