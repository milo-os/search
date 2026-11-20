package resourcesearchquery

import (
	"strings"
	"testing"

	"k8s.io/apiserver/pkg/authentication/user"
)

func TestExtractParentContext(t *testing.T) {
	const (
		typeKey = "iam.miloapis.com/parent-type"
		nameKey = "iam.miloapis.com/parent-name"
	)
	extra := func(kv map[string][]string) *user.DefaultInfo {
		return &user.DefaultInfo{Name: "alice", Extra: kv}
	}
	tests := []struct {
		name    string
		u       user.Info
		want    *parentContext
		wantErr string // substring of the error; "" means no error
	}{
		{
			name: "nil user is a root request",
			u:    nil,
		},
		{
			name: "no extras is a root request",
			u:    &user.DefaultInfo{Name: "alice"},
		},
		{
			name: "User context",
			u:    extra(map[string][]string{typeKey: {"User"}, nameKey: {"alice"}}),
			want: &parentContext{Type: "User", Name: "alice"},
		},
		{
			name: "Project context",
			u:    extra(map[string][]string{typeKey: {"Project"}, nameKey: {"acme-prod"}}),
			want: &parentContext{Type: "Project", Name: "acme-prod"},
		},
		{
			name:    "only parent-type is refused",
			u:       extra(map[string][]string{typeKey: {"Project"}}),
			wantErr: "has iam.miloapis.com/parent-type but no iam.miloapis.com/parent-name",
		},
		{
			name:    "only parent-name is refused",
			u:       extra(map[string][]string{nameKey: {"acme-prod"}}),
			wantErr: "has iam.miloapis.com/parent-name but no iam.miloapis.com/parent-type",
		},
		{
			name:    "empty type slice is refused",
			u:       extra(map[string][]string{typeKey: {}, nameKey: {"acme-prod"}}),
			wantErr: "but no iam.miloapis.com/parent-type",
		},
		{
			name:    "empty name slice is refused",
			u:       extra(map[string][]string{typeKey: {"Project"}, nameKey: {}}),
			wantErr: "but no iam.miloapis.com/parent-name",
		},
		{
			name:    "empty-string type is refused",
			u:       extra(map[string][]string{typeKey: {""}, nameKey: {"acme-prod"}}),
			wantErr: "but no iam.miloapis.com/parent-type",
		},
		{
			name:    "empty-string name is refused",
			u:       extra(map[string][]string{typeKey: {"Project"}, nameKey: {""}}),
			wantErr: "but no iam.miloapis.com/parent-name",
		},
		{
			name:    "only parent-name with empty value is refused",
			u:       extra(map[string][]string{nameKey: {""}}),
			wantErr: "must set both iam.miloapis.com/parent-type and iam.miloapis.com/parent-name",
		},
		{
			name:    "both keys with empty values are refused",
			u:       extra(map[string][]string{typeKey: {""}, nameKey: {""}}),
			wantErr: "must set both iam.miloapis.com/parent-type and iam.miloapis.com/parent-name",
		},
		{
			name:    "whitespace-only name is refused",
			u:       extra(map[string][]string{typeKey: {"Project"}, nameKey: {"  "}}),
			wantErr: "has iam.miloapis.com/parent-type but no iam.miloapis.com/parent-name",
		},
		{
			name:    "whitespace-only type is refused",
			u:       extra(map[string][]string{typeKey: {" \t"}, nameKey: {"acme-prod"}}),
			wantErr: "has iam.miloapis.com/parent-name but no iam.miloapis.com/parent-type",
		},
		{
			name:    "Organization context is refused",
			u:       extra(map[string][]string{typeKey: {"Organization"}, nameKey: {"datum"}}),
			wantErr: "not served in Organization contexts",
		},
		{
			name:    "type is matched case-sensitively",
			u:       extra(map[string][]string{typeKey: {"user"}, nameKey: {"alice"}}),
			wantErr: "not served in user contexts",
		},
		{
			name: "multi-valued keys take first value",
			u: extra(map[string][]string{
				typeKey: {"Project", "Organization"},
				nameKey: {"primary", "secondary"},
			}),
			want: &parentContext{Type: "Project", Name: "primary"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractParentContext(tt.u)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got context %#v", tt.wantErr, got)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error %q does not contain %q", err, tt.wantErr)
				}
				if got != nil {
					t.Fatalf("expected nil context with error, got %#v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if (got == nil) != (tt.want == nil) {
				t.Fatalf("nilness mismatch: got %#v want %#v", got, tt.want)
			}
			if got != nil && *got != *tt.want {
				t.Fatalf("got %#v want %#v", got, tt.want)
			}
		})
	}
}

func TestBuildScopedFilter(t *testing.T) {
	tests := []struct {
		name string
		in   *parentContext
		want string
	}{
		{
			name: "nil context returns empty filter",
			in:   nil,
			want: "",
		},
		{
			name: "Project context lowercases type",
			in:   &parentContext{Type: "Project", Name: "acme-prod"},
			want: `(_tenant_type = "project" AND _tenant = "acme-prod")`,
		},
		{
			name: "Organization context",
			in:   &parentContext{Type: "Organization", Name: "datum"},
			want: `(_tenant_type = "organization" AND _tenant = "datum")`,
		},
		{
			name: "mixed-case type is lowercased",
			in:   &parentContext{Type: "ProJECT", Name: "p1"},
			want: `(_tenant_type = "project" AND _tenant = "p1")`,
		},
		{
			name: "name with double quote is escaped",
			in:   &parentContext{Type: "Project", Name: `evil"name`},
			want: `(_tenant_type = "project" AND _tenant = "evil\"name")`,
		},
		{
			name: "name with backslash is escaped",
			in:   &parentContext{Type: "Project", Name: `back\slash`},
			want: `(_tenant_type = "project" AND _tenant = "back\\slash")`,
		},
		{
			name: "non-ASCII name is preserved literally",
			in:   &parentContext{Type: "Project", Name: "café"},
			want: `(_tenant_type = "project" AND _tenant = "café")`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildScopedFilter(tt.in)
			if got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
			if got != "" && (!strings.HasPrefix(got, "(") || !strings.HasSuffix(got, ")")) {
				t.Fatalf("filter not wrapped in parens: %q", got)
			}
		})
	}
}

func TestBuildRootVisibleFilter(t *testing.T) {
	scope := buildScopedFilter(&parentContext{Type: "Organization", Name: "acme"})
	want := `(_tenant_type = "organization" AND _tenant = "acme") OR _tenant_type IN ["platform", "Global"]`
	if got := buildRootVisibleFilter(scope); got != want {
		t.Errorf("buildRootVisibleFilter = %q, want %q", got, want)
	}
	if got := buildRootVisibleFilter(""); got != "" {
		t.Errorf("unscoped: got %q, want empty", got)
	}
}
