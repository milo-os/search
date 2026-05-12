package resourcesearchquery

import (
	"fmt"
	"strings"

	"k8s.io/apiserver/pkg/authentication/user"
)

// Canonical extra keys that Milo's URL-pattern middleware sets when a request
// comes in on a tenant-scoped URL (e.g. /apis/.../projects/<name>/control-plane/...).
const (
	parentTypeKey = "iam.miloapis.com/parent-type"
	parentNameKey = "iam.miloapis.com/parent-name"
)

// parentContext captures the (type, name) pair that scopes a request to a
// single tenant. nil means "no scope" — handlers should treat that as
// preserving unscoped behavior, not as an error.
type parentContext struct {
	Type string
	Name string
}

// extractParentContext returns a parentContext when BOTH parent-type and
// parent-name are present and non-empty in user.Info.Extra. Multi-valued
// keys take the first value (in practice these are single-valued).
// Returns nil when either key is absent or empty, including when u is nil.
func extractParentContext(u user.Info) *parentContext {
	if u == nil {
		return nil
	}
	extra := u.GetExtra()
	types, hasType := extra[parentTypeKey]
	names, hasName := extra[parentNameKey]
	if !hasType || !hasName || len(types) == 0 || len(names) == 0 {
		return nil
	}
	return &parentContext{Type: types[0], Name: names[0]}
}

// buildScopedFilter formats a Meilisearch filter clause that restricts results
// to a single tenant. Returns "" when p is nil (no scoping). Uses %q quoting
// to handle names containing quotes or backslashes safely. Type is lowercased
// because stored _tenant_type values are conventionally lowercase.
func buildScopedFilter(p *parentContext) string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf(`(_tenant_type = %q AND _tenant = %q)`,
		strings.ToLower(p.Type), p.Name)
}
