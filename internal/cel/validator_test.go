package cel

import (
	"strings"
	"testing"
)

func TestValidator_Validate(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		wantErrors []string
	}{
		{
			name:       "valid metadata equality",
			expression: "metadata.name == 'foo'",
			wantErrors: nil,
		},
		{
			name:       "valid spec comparison",
			expression: "spec.replicas > 1",
			wantErrors: nil,
		},
		{
			name:       "valid status list exists",
			expression: "status.conditions.exists(c, c.type == 'Ready')",
			wantErrors: nil,
		},
		{
			name:       "valid string function startsWith",
			expression: "metadata.name.startsWith('prod-')",
			wantErrors: nil,
		},
		{
			name:       "valid map access",
			expression: "metadata.labels['app'] == 'backend'",
			wantErrors: nil,
		},
		{
			name:       "valid map membership",
			expression: "'app' in metadata.labels",
			wantErrors: nil,
		},
		{
			name:       "valid list membership",
			expression: "metadata.name in ['a', 'b']",
			wantErrors: nil,
		},
		{
			name:       "valid ternary",
			expression: "'env' in metadata.labels ? metadata.labels['env'] == 'prod' : false",
			wantErrors: nil,
		},
		{
			name:       "empty expression",
			expression: "",
			wantErrors: nil,
		},
		{
			name:       "syntax error",
			expression: "metadata.name ==",
			wantErrors: []string{"Syntax error"},
		},
		{
			name:       "type error string",
			expression: "'string result'",
			wantErrors: []string{"expression must evaluate to a boolean"},
		},
		{
			name:       "type error int",
			expression: "123",
			wantErrors: []string{"expression must evaluate to a boolean"},
		},
		{
			name:       "invalid root variable",
			expression: "other.field == 'foo'",
			wantErrors: []string{"undeclared reference to 'other'"},
		},
		{
			name:       "disallowed operator concatenation",
			expression: "metadata.name + 'suffix' == 'foo'",
			wantErrors: []string{"operator or function '_+_' is not allowed"},
		},
		{
			name:       "disallowed string size (not compiled but checked as operator)",
			expression: "metadata.name.size() > 0",
			wantErrors: nil, // Wait, size is in allowed list!
		},
		{
			name:       "valid size usage",
			expression: "spec.containers.size() > 0",
			wantErrors: nil,
		},
		{
			name:       "valid map literal",
			expression: "{'k1': 'v1', 'k2': 'v2'}.size() == 2",
			wantErrors: nil,
		},
		{
			name:       "complex chained comprehensions",
			expression: "status.conditions.filter(c, c.type == 'Ready').all(c, c.status == 'True')",
			wantErrors: nil,
		},
		{
			name:       "complex safe map access",
			expression: "has(metadata.labels) && 'app' in metadata.labels && metadata.labels['app'].startsWith('backend-')",
			wantErrors: nil,
		},
		{
			name:       "complex list quantification with regex",
			expression: "spec.tags.exists(t, t.matches('^v[0-9]+'))",
			wantErrors: nil,
		},
		{
			name:       "complex list member string check",
			expression: "['prod', 'staging'].exists(env, metadata.name.endsWith(env))",
			wantErrors: nil,
		},
		{
			name:       "complex boolean logic with ternary",
			expression: "(spec.replicas > 0 && status.availableReplicas == spec.replicas) ? true : (metadata.name == 'maintenance')",
			wantErrors: nil,
		},
		{
			name:       "invalid list element operator",
			expression: "['a', metadata.name + 'b'].size() > 0",
			wantErrors: []string{"operator or function '_+_' is not allowed"},
		},
		{
			name:       "invalid map key operator",
			expression: "{metadata.name + 'b': 'val'}.size() > 0",
			wantErrors: []string{"operator or function '_+_' is not allowed"},
		},
		{
			name:       "invalid map value operator",
			expression: "{'key': metadata.name + 'b'}.size() > 0",
			wantErrors: []string{"operator or function '_+_' is not allowed"},
		},
		{
			name:       "invalid comprehension body operator",
			expression: "[1, 2].exists(x, x + 1 == 2)",
			wantErrors: []string{"operator or function '_+_' is not allowed"},
		},
		// --- Additional Extended Test Cases ---
		// Comparison & Logical
		{
			name:       "valid complex comparison",
			expression: "(100 >= 50) && (20 < 30) && (10 != 5) && (1 == 1)",
			wantErrors: nil,
		},
		{
			name:       "valid complex logical negation",
			expression: "!((true || false) && false)",
			wantErrors: nil,
		},

		// String Functions
		{
			name:       "valid string contains",
			expression: "'team-a-xy'.contains('am-a')",
			wantErrors: nil,
		},
		{
			name:       "valid string matches regex",
			expression: "'v1.2.3'.matches('^v\\\\d+\\\\.\\\\d+\\\\.\\\\d+$')",
			wantErrors: nil,
		},
		{
			name:       "valid string endsWith chain",
			expression: "'filename.text.txt'.endsWith('.txt') && !'filename.text.txt'.endsWith('.go')",
			wantErrors: nil,
		},

		// List/Map Macros & Access
		{
			name:       "valid list map macro",
			expression: "[1, 2, 3].map(x, x).size() == 3",
			wantErrors: nil,
		},
		{
			name:       "valid list filter macro",
			expression: "[1, 2, 3, 4].filter(x, x > 2).size() == 2",
			wantErrors: nil,
		},
		{
			name:       "valid nested map in list comprehension",
			expression: "[{'a': 1}, {'a': 2}].all(m, m['a'] > 0)",
			wantErrors: nil,
		},

		// Explicitly Allowed Operations
		{
			name:       "valid list concatenation (allowed for macro support)",
			expression: "[1] + [2] == [1, 2]",
			wantErrors: nil,
		},

		// Disallowed Math Operators
		{
			name:       "invalid subtraction",
			expression: "10 - 5 > 0",
			wantErrors: []string{"operator or function '_-_' is not allowed"},
		},
		{
			name:       "invalid multiplication",
			expression: "10 * 5 > 0",
			wantErrors: []string{"operator or function '_*_' is not allowed"},
		},
		{
			name:       "invalid division",
			expression: "10 / 5 > 0",
			wantErrors: []string{"operator or function '_/_' is not allowed"},
		},
		{
			name:       "invalid modulo",
			expression: "5 % 2 == 1",
			wantErrors: []string{"operator or function '_%_' is not allowed"},
		},

		// Disallowed Concatenation on non-lists
		{
			name:       "invalid string concatenation",
			expression: "'a' + 'b' == 'ab'",
			wantErrors: []string{"operator or function '_+_' is not allowed"},
		},

		// Disallowed Functions
		{
			name:       "invalid duration function",
			expression: "duration('10m') < duration('1h')",
			wantErrors: []string{"operator or function 'duration' is not allowed"},
		},
		{
			name:       "invalid timestamp function",
			expression: "timestamp('2023-01-01T00:00:00Z') > timestamp('2022-01-01T00:00:00Z')",
			wantErrors: []string{"operator or function 'timestamp' is not allowed"},
		},
		{
			name:       "valid list exists_one",
			expression: "[1, 2, 3].exists_one(x, x == 2)",
			wantErrors: []string{"operator or function"},
		},

		// Complex Real-world Policy Scenarios
		{
			name:       "k8s enforce readonly root filesystem",
			expression: "spec.containers.all(c, c.securityContext.readOnlyRootFilesystem == true)",
			wantErrors: nil,
		},
		{
			name:       "k8s enforce image registry",
			expression: "spec.containers.all(c, c.image.startsWith('gcr.io/my-org/'))",
			wantErrors: nil,
		},
		{
			name:       "complex recursive logic with property existence",
			expression: "has(spec.volumes) ? spec.volumes.all(v, !has(v.hostPath)) : true",
			wantErrors: nil,
		},
	}

	validator, err := NewValidator(50)
	if err != nil {
		t.Fatalf("Failed to create validator: %v", err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validator.Validate(tt.expression)

			if len(tt.wantErrors) == 0 {
				if len(errs) > 0 {
					t.Errorf("Validate() returned unexpected errors: %v", errs)
				}
			} else {
				if len(errs) == 0 {
					t.Errorf("Validate() returned 0 errors, want at least 1 containing %q", tt.wantErrors[0])
				} else {
					// Check if the received error contains the expected substring
					if !strings.Contains(errs[0], tt.wantErrors[0]) {
						t.Errorf("Validate() error = %q, want substring %q, but got %q", errs[0], tt.wantErrors[0], errs)
					}
				}
			}
		})
	}
}

func TestValidator_MaxDepth(t *testing.T) {
	validator, err := NewValidator(50)
	if err != nil {
		t.Fatalf("Failed to create validator: %v", err)
	}

	// Create a deeply nested expression: !(!(!(...)))
	// 60 levels deep, which should exceed the default limit of 50
	var sb strings.Builder
	for i := 0; i < 60; i++ {
		sb.WriteString("!(")
	}
	sb.WriteString("true")
	for i := 0; i < 60; i++ {
		sb.WriteString(")")
	}

	expr := sb.String()

	errs := validator.Validate(expr)
	if len(errs) == 0 {
		t.Errorf("Expected max depth error, got none")
	} else {
		found := false
		for _, e := range errs {
			if strings.Contains(e, "expression complexity exceeds maximum depth") {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected max depth error, got: %v", errs)
		}
	}
}
