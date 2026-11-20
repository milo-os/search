package resourcesearchquery

import (
	"fmt"
	"strings"

	"k8s.io/apiserver/pkg/authentication/user"

	"go.miloapis.net/search/internal/tenant"
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
// to handle names containing quotes or backslashes safely. Type is lowercased,
// but stored _tenant_type values are often title case: reindexing and the
// audit indexer store "Project", "User" and "Global". They still match because
// Meilisearch compares filter strings case-insensitively;
// test/e2e/search-flow-user-context pins this.
func buildScopedFilter(p *parentContext) string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf(`(_tenant_type = %q AND _tenant = %q)`,
		strings.ToLower(p.Type), p.Name)
}

// tenantTypeGlobal is the tenant type the indexer stores for a write at
// Milo's root "global" scope (it title-cases every scope type except
// "platform").
const tenantTypeGlobal = "Global"

// rootTenantTypes are the tenant types of documents written at the platform
// root: "Global" from audit events and "platform" from reindexing.
var rootTenantTypes = []string{tenant.TenantTypePlatform, tenantTypeGlobal}

// buildRootVisibleFilter extends a scoped filter to also match documents
// written at the platform root, for indexes visible from all contexts.
// Documents written in any other tenant still don't match. Returns "" when
// scopeFilter is "" (no scoping).
func buildRootVisibleFilter(scopeFilter string) string {
	if scopeFilter == "" {
		return ""
	}
	quoted := make([]string, len(rootTenantTypes))
	for i, t := range rootTenantTypes {
		quoted[i] = fmt.Sprintf("%q", t)
	}
	return fmt.Sprintf(`%s OR _tenant_type IN [%s]`, scopeFilter, strings.Join(quoted, ", "))
}
