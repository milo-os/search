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

// Parent types that search is served in. Milo sets parent-type in exact title
// case and compares it case-sensitively, so these are matched exactly.
const (
	parentTypeUser    = "User"
	parentTypeProject = "Project"
)

// supportedParentTypes is the set of parent types a scoped query may run in.
// Any other type, such as Organization, is refused because search isn't
// served there.
var supportedParentTypes = map[string]bool{
	parentTypeUser:    true,
	parentTypeProject: true,
}

// parentContext captures the (type, name) pair that scopes a request to a
// single tenant. extractParentContext returns nil only for a platform-root
// request, which carries neither parent extra and runs unscoped.
type parentContext struct {
	Type string
	Name string
}

// extractParentContext reads the tenant scope from user.Info.Extra. It fails
// closed:
//   - neither parent-type nor parent-name present (or u is nil): returns nil,
//     nil. This is a platform-root request and runs unscoped.
//   - only one of the two present, or either one empty or blank: returns an
//     error, so an incomplete context is refused rather than run unfiltered.
//   - a parent type other than User or Project (compared case-sensitively):
//     returns an error.
//
// Multi-valued keys take the first value (in practice these are
// single-valued).
func extractParentContext(u user.Info) (*parentContext, error) {
	if u == nil {
		return nil, nil
	}
	extra := u.GetExtra()
	types, hasType := extra[parentTypeKey]
	names, hasName := extra[parentNameKey]
	if !hasType && !hasName {
		return nil, nil
	}
	// A key counts as set only when its first value isn't blank.
	typeSet := len(types) > 0 && strings.TrimSpace(types[0]) != ""
	nameSet := len(names) > 0 && strings.TrimSpace(names[0]) != ""
	switch {
	case typeSet && !nameSet:
		return nil, fmt.Errorf("search request has %s but no %s; refusing to run an unscoped query",
			parentTypeKey, parentNameKey)
	case nameSet && !typeSet:
		return nil, fmt.Errorf("search request has %s but no %s; refusing to run an unscoped query",
			parentNameKey, parentTypeKey)
	case !typeSet || !nameSet:
		return nil, fmt.Errorf("search request must set both %s and %s; refusing to run an unscoped query",
			parentTypeKey, parentNameKey)
	}
	if !supportedParentTypes[types[0]] {
		return nil, fmt.Errorf("search is not served in %s contexts; supported parent types are %s and %s",
			types[0], parentTypeUser, parentTypeProject)
	}
	return &parentContext{Type: types[0], Name: names[0]}, nil
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
