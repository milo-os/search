package main

import (
	"crypto/rand"
	"fmt"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
)

// generateQueryName mints a DNS-1123-safe, collision-resistant name for the
// ephemeral ResourceSearchQuery. The resource is create-only and never
// persisted, but the apiserver still wants a name on the submitted object.
func generateQueryName() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	const n = 10
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		for i := range b {
			b[i] = 'x'
		}
		return "search-" + string(b)
	}
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return "search-" + string(b)
}

// indexedKind is the consumer-facing projection of a ResourceIndexPolicy: the
// searchable kind, its group/version, and whether its index is ready. It backs
// both `kinds` and --kind resolution.
type indexedKind struct {
	Kind      string
	Group     string
	Version   string
	Ready     bool
	ReadyText string // True | False | Unknown
	Policy    string
	IndexName string
	Created   metav1.Time
}

// gv returns the API group/version label, using "(core)" for the empty group so
// core kinds read clearly in tables.
func (k indexedKind) groupDisplay() string {
	if k.Group == "" {
		return "(core)"
	}
	return k.Group
}

// target returns the TargetResource wire form for this kind.
func (k indexedKind) target() searchv1alpha1.TargetResource {
	return searchv1alpha1.TargetResource{Group: k.Group, Version: k.Version, Kind: k.Kind}
}

// policyToKind projects a ResourceIndexPolicy into an indexedKind.
func policyToKind(p *searchv1alpha1.ResourceIndexPolicy) indexedKind {
	text, ready := readyCondition(p.Status.Conditions)
	return indexedKind{
		Kind:      p.Spec.TargetResource.Kind,
		Group:     p.Spec.TargetResource.Group,
		Version:   p.Spec.TargetResource.Version,
		Ready:     ready,
		ReadyText: text,
		Policy:    p.Name,
		IndexName: p.Status.IndexName,
		Created:   p.CreationTimestamp,
	}
}

// indexedKinds projects a list of policies into indexedKinds, sorted by kind
// then group for stable display.
func indexedKinds(policies []searchv1alpha1.ResourceIndexPolicy) []indexedKind {
	kinds := make([]indexedKind, 0, len(policies))
	for i := range policies {
		kinds = append(kinds, policyToKind(&policies[i]))
	}
	sort.Slice(kinds, func(i, j int) bool {
		if kinds[i].Kind != kinds[j].Kind {
			return kinds[i].Kind < kinds[j].Kind
		}
		return kinds[i].Group < kinds[j].Group
	})
	return kinds
}

// readyCondition extracts the Ready condition's status as display text and a
// boolean. Absent condition reads as Unknown / not ready.
func readyCondition(conds []metav1.Condition) (string, bool) {
	for i := range conds {
		if conds[i].Type == "Ready" {
			return string(conds[i].Status), conds[i].Status == metav1.ConditionTrue
		}
	}
	return "Unknown", false
}

// parseKindRef splits a --kind value of the form Kind[.group][/version] into its
// parts. The kind is the token before the first dot; the group is everything
// after it (group names contain dots, kind names do not); an optional /version
// suffix pins the version.
func parseKindRef(ref string) (kind, group, version string) {
	rest := ref
	if slash := strings.Index(rest, "/"); slash >= 0 {
		version = rest[slash+1:]
		rest = rest[:slash]
	}
	if dot := strings.Index(rest, "."); dot >= 0 {
		kind = rest[:dot]
		group = rest[dot+1:]
	} else {
		kind = rest
	}
	return kind, group, version
}

// resolveKind resolves a single --kind reference against the indexed-kinds list.
// It matches kind (case-insensitively), then narrows by group and version when
// the reference pins them. Unknown kinds return a SEARCH_NOT_FOUND error with a
// did-you-mean; a reference that still matches more than one kind returns a
// SEARCH_USAGE error naming the candidates to qualify.
func resolveKind(ref string, kinds []indexedKind) (searchv1alpha1.TargetResource, error) {
	kind, group, version := parseKindRef(ref)
	if kind == "" {
		return searchv1alpha1.TargetResource{}, newCLIError(exitUsage, fmt.Sprintf("--kind %q isn't a kind name", ref)).
			withFix("write it as Workload, or Workload.compute.datum.net to pin the group")
	}

	var matches []indexedKind
	for _, k := range kinds {
		if !strings.EqualFold(k.Kind, kind) {
			continue
		}
		if group != "" && !strings.EqualFold(k.Group, group) {
			continue
		}
		if version != "" && !strings.EqualFold(k.Version, version) {
			continue
		}
		matches = append(matches, k)
	}

	switch len(matches) {
	case 1:
		return matches[0].target(), nil
	case 0:
		return searchv1alpha1.TargetResource{}, unknownKindError(ref, kind, kinds)
	default:
		return searchv1alpha1.TargetResource{}, ambiguousKindError(ref, matches)
	}
}

// resolveKinds resolves every --kind reference to a TargetResource, preserving
// order and stopping at the first error.
func resolveKinds(refs []string, kinds []indexedKind) ([]searchv1alpha1.TargetResource, error) {
	out := make([]searchv1alpha1.TargetResource, 0, len(refs))
	for _, ref := range refs {
		tr, err := resolveKind(ref, kinds)
		if err != nil {
			return nil, err
		}
		out = append(out, tr)
	}
	return out, nil
}

// unknownKindError renders the "not a searchable kind" failure with the nearest
// searchable kind as a did-you-mean.
func unknownKindError(ref, kind string, kinds []indexedKind) *cliError {
	msg := fmt.Sprintf("%q isn't a searchable kind", ref)
	if near := nearestKind(kind, kinds); near != nil {
		msg += fmt.Sprintf(" — did you mean %s?", kindLabel(*near))
	}
	return newCLIError(exitNotFound, msg).
		withFix("run \"datumctl search kinds\" to see everything you can search")
}

// ambiguousKindError asks the user to qualify a reference that matched several
// searchable kinds.
func ambiguousKindError(ref string, matches []indexedKind) *cliError {
	var b strings.Builder
	fmt.Fprintf(&b, "%q matches more than one searchable kind:", ref)
	for _, m := range matches {
		fmt.Fprintf(&b, "\n  %s.%s/%s", m.Kind, m.Group, m.Version)
	}
	return newCLIError(exitUsage, b.String()).
		withFix("add the group to pick one, for example --kind " + matches[0].Kind + "." + matches[0].Group)
}

// kindLabel renders a kind for a did-you-mean suggestion: "Workload
// (compute.datum.net)", or "ConfigMap (core)" for the core group.
func kindLabel(k indexedKind) string {
	if k.Group == "" {
		return k.Kind + " (core)"
	}
	return fmt.Sprintf("%s (%s)", k.Kind, k.Group)
}

// nearestKind returns the searchable kind whose name is closest to target
// (case-insensitive edit distance), within a small threshold, or nil.
func nearestKind(target string, kinds []indexedKind) *indexedKind {
	best := -1
	bestDist := 1 << 30
	lt := strings.ToLower(target)
	for i := range kinds {
		d := levenshtein(lt, strings.ToLower(kinds[i].Kind))
		if d < bestDist {
			bestDist = d
			best = i
		}
	}
	// Only suggest when reasonably close: within a third of the name length.
	if best < 0 || bestDist > 1+len(target)/3 {
		return nil
	}
	return &kinds[best]
}

// levenshtein is the classic edit distance, used for --kind did-you-mean.
func levenshtein(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}
	prev := make([]int, lb+1)
	curr := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}
	for i := 1; i <= la; i++ {
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = min3(prev[j]+1, curr[j-1]+1, prev[j-1]+cost)
		}
		prev, curr = curr, prev
	}
	return prev[lb]
}

func min3(a, b, c int) int {
	m := a
	if b < m {
		m = b
	}
	if c < m {
		m = c
	}
	return m
}

// gvLabel renders a group/version pair, dropping the group for core kinds so it
// reads "v1" rather than "/v1".
func gvLabel(group, version string) string {
	if group == "" {
		return version
	}
	return group + "/" + version
}

// targetLabel renders a TargetResource as "Kind (group/version)" for warnings.
func targetLabel(tr searchv1alpha1.TargetResource) string {
	return fmt.Sprintf("%s (%s)", tr.Kind, gvLabel(tr.Group, tr.Version))
}

// resourceName renders the -o name identifier: "<kind>.<group>/<name>", with the
// kind lowercased and the group omitted for core kinds.
func resourceName(kind, group, name string) string {
	lk := strings.ToLower(kind)
	if group == "" {
		return lk + "/" + name
	}
	return lk + "." + group + "/" + name
}

// splitAPIVersion splits an unstructured object's apiVersion into group and
// version. A bare "v1" is the core group (empty group).
func splitAPIVersion(apiVersion string) (group, version string) {
	if slash := strings.Index(apiVersion, "/"); slash >= 0 {
		return apiVersion[:slash], apiVersion[slash+1:]
	}
	return "", apiVersion
}

// orDash renders an empty string as an em dash so table cells never collapse.
func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

// pluralize returns "1 thing" / "n things".
func pluralize(n int, singular string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", singular)
	}
	return fmt.Sprintf("%d %ss", n, singular)
}

// humanDuration formats an age the way kubectl does: the two most significant
// units, collapsing to a single coarse unit for long ages (e.g. 312d, 44d, 3h2m).
func humanDuration(t metav1.Time) string {
	if t.IsZero() {
		return "<unknown>"
	}
	d := time.Since(t.Time)
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		h := int(d.Hours())
		m := int(d.Minutes()) % 60
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh%dm", h, m)
	default:
		days := int(d.Hours()) / 24
		if days < 8 {
			h := int(d.Hours()) % 24
			if h == 0 {
				return fmt.Sprintf("%dd", days)
			}
			return fmt.Sprintf("%dd%dh", days, h)
		}
		return fmt.Sprintf("%dd", days)
	}
}
