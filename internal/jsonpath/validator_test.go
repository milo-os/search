package jsonpath

import "testing"

func TestValidatePath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		// Valid paths
		{name: "simple field", path: ".spec.name", wantErr: false},
		{name: "nested field", path: ".spec.container.image", wantErr: false},
		{name: "metadata name", path: ".metadata.name", wantErr: false},
		{name: "status field", path: ".status.phase", wantErr: false},
		{name: "bracket notation double quotes", path: `.metadata.labels["app"]`, wantErr: false},
		{name: "bracket notation single quotes", path: `.metadata.labels['app']`, wantErr: false},
		{name: "bracket notation with special chars", path: `.metadata.annotations["kubernetes.io/name"]`, wantErr: false},
		{name: "array index", path: ".spec.containers[0].name", wantErr: false},
		{name: "multiple array indices", path: ".spec.containers[0].ports[1].containerPort", wantErr: false},
		{name: "underscore in field name", path: ".spec.my_field", wantErr: false},
		{name: "mixed bracket and dot", path: `.spec.template.metadata.labels["app.kubernetes.io/name"]`, wantErr: false},

		// Invalid paths
		{name: "empty path", path: "", wantErr: true},
		{name: "missing leading dot", path: "spec.name", wantErr: true},
		{name: "double dots", path: ".spec..name", wantErr: true},
		{name: "starts with number", path: ".1spec.name", wantErr: true},
		{name: "unclosed bracket", path: ".metadata.labels[app", wantErr: true},
		{name: "missing quotes in bracket", path: ".metadata.labels[app]", wantErr: true},
		{name: "trailing dot", path: ".spec.name.", wantErr: true},
		{name: "spaces in path", path: ".spec. name", wantErr: true},
		{name: "special chars without bracket", path: ".spec.my-field", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errMsg := ValidatePath(tt.path)
			if tt.wantErr && errMsg == "" {
				t.Errorf("ValidatePath(%q) expected error but got none", tt.path)
			}
			if !tt.wantErr && errMsg != "" {
				t.Errorf("ValidatePath(%q) unexpected error: %s", tt.path, errMsg)
			}
		})
	}
}

func TestValidator(t *testing.T) {
	v := NewValidator()

	// Test valid path
	if err := v.Validate(".spec.name"); err != "" {
		t.Errorf("Validate() unexpected error: %s", err)
	}

	// Test invalid path
	if err := v.Validate("spec.name"); err == "" {
		t.Error("Validate() expected error but got none")
	}
}
