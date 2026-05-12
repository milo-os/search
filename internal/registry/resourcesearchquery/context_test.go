package resourcesearchquery

import (
	"strings"
	"testing"

	"k8s.io/apiserver/pkg/authentication/user"
)

func TestExtractParentContext(t *testing.T) {
	tests := []struct {
		name string
		u    user.Info
		want *parentContext
	}{
		{
			name: "nil user returns nil",
			u:    nil,
			want: nil,
		},
		{
			name: "both keys present returns context",
			u: &user.DefaultInfo{
				Name: "alice",
				Extra: map[string][]string{
					"iam.miloapis.com/parent-type": {"Project"},
					"iam.miloapis.com/parent-name": {"acme-prod"},
				},
			},
			want: &parentContext{Type: "Project", Name: "acme-prod"},
		},
		{
			name: "only type present returns nil",
			u: &user.DefaultInfo{
				Extra: map[string][]string{
					"iam.miloapis.com/parent-type": {"Project"},
				},
			},
			want: nil,
		},
		{
			name: "only name present returns nil",
			u: &user.DefaultInfo{
				Extra: map[string][]string{
					"iam.miloapis.com/parent-name": {"acme-prod"},
				},
			},
			want: nil,
		},
		{
			name: "empty type slice returns nil",
			u: &user.DefaultInfo{
				Extra: map[string][]string{
					"iam.miloapis.com/parent-type": {},
					"iam.miloapis.com/parent-name": {"acme-prod"},
				},
			},
			want: nil,
		},
		{
			name: "empty name slice returns nil",
			u: &user.DefaultInfo{
				Extra: map[string][]string{
					"iam.miloapis.com/parent-type": {"Project"},
					"iam.miloapis.com/parent-name": {},
				},
			},
			want: nil,
		},
		{
			name: "no extras at all returns nil",
			u:    &user.DefaultInfo{Name: "alice"},
			want: nil,
		},
		{
			name: "multi-valued keys take first value",
			u: &user.DefaultInfo{
				Extra: map[string][]string{
					"iam.miloapis.com/parent-type": {"Organization", "Project"},
					"iam.miloapis.com/parent-name": {"primary", "secondary"},
				},
			},
			want: &parentContext{Type: "Organization", Name: "primary"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractParentContext(tt.u)
			if (got == nil) != (tt.want == nil) {
				t.Fatalf("nilness mismatch: got %#v want %#v", got, tt.want)
			}
			if got != nil && (got.Type != tt.want.Type || got.Name != tt.want.Name) {
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
