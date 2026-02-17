// Package jsonpath provides validation utilities for JSONPath expressions.
package jsonpath

import (
	"regexp"
	"strings"
)

// segmentRegex matches valid JSONPath segments:
// - .fieldName (identifier starting with letter or underscore)
// - ["key"] or ['key'] (bracket notation for map keys)
// - [0] (array index)
var segmentRegex = regexp.MustCompile(`^(\.[a-zA-Z_][a-zA-Z0-9_]*|\[["'][^"']+["']\]|\[[0-9]+\])`)

// Validator provides JSONPath validation functionality.
type Validator struct{}

// NewValidator creates a new JSONPath validator.
func NewValidator() *Validator {
	return &Validator{}
}

// Validate validates that a path is a valid JSONPath expression.
// Valid paths:
//   - .spec.name
//   - .metadata.labels["app"]
//   - .spec.containers[0].name
//   - .metadata.annotations["kubernetes.io/name"]
//
// Returns an error message if invalid, empty string if valid.
func (v *Validator) Validate(path string) string {
	return ValidatePath(path)
}

// ValidatePath validates a JSONPath expression.
// This is a standalone function for convenience.
// Returns an error message if invalid, empty string if valid.
func ValidatePath(path string) string {
	if path == "" {
		return "path cannot be empty"
	}

	if !strings.HasPrefix(path, ".") {
		return "path must start with '.'"
	}

	// Parse the path segment by segment
	remaining := path
	for len(remaining) > 0 {
		match := segmentRegex.FindString(remaining)
		if match == "" {
			return "invalid path syntax at: " + remaining
		}
		remaining = remaining[len(match):]
	}

	return ""
}
