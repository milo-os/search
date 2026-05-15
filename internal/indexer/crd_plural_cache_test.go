package indexer

import (
	"sync"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
)

func newCRD(group, kind, plural string) *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: plural + "." + group},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind:   kind,
				Plural: plural,
			},
		},
	}
}

func newTestCache() *CRDPluralCache {
	return &CRDPluralCache{plurals: make(map[schema.GroupKind]string)}
}

func TestCRDPluralCache_LookupAfterEvents(t *testing.T) {
	gk := schema.GroupKind{Group: "g.example.com", Kind: "Widget"}

	tests := []struct {
		name       string
		setup      func(c *CRDPluralCache)
		wantPlural string
		wantOK     bool
	}{
		{
			name:       "lookup before any add returns false",
			setup:      func(_ *CRDPluralCache) {},
			wantPlural: "",
			wantOK:     false,
		},
		{
			name: "add CRD makes plural queryable",
			setup: func(c *CRDPluralCache) {
				c.onAddOrUpdate(newCRD("g.example.com", "Widget", "widgets"))
			},
			wantPlural: "widgets",
			wantOK:     true,
		},
		{
			name: "update CRD replaces plural",
			setup: func(c *CRDPluralCache) {
				c.onAddOrUpdate(newCRD("g.example.com", "Widget", "widgets"))
				c.onAddOrUpdate(newCRD("g.example.com", "Widget", "widgetses"))
			},
			wantPlural: "widgetses",
			wantOK:     true,
		},
		{
			name: "delete CRD makes lookup return false",
			setup: func(c *CRDPluralCache) {
				crd := newCRD("g.example.com", "Widget", "widgets")
				c.onAddOrUpdate(crd)
				c.onDelete(crd)
			},
			wantPlural: "",
			wantOK:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestCache()
			tt.setup(c)
			got, ok := c.Lookup(gk)
			if ok != tt.wantOK || got != tt.wantPlural {
				t.Fatalf("Lookup() = (%q, %v), want (%q, %v)", got, ok, tt.wantPlural, tt.wantOK)
			}
		})
	}
}

func TestCRDPluralCache_DeleteTombstone_HandledCorrectly(t *testing.T) {
	c := newTestCache()
	crd := newCRD("g.example.com", "Widget", "widgets")
	c.onAddOrUpdate(crd)
	tomb := toolscache.DeletedFinalStateUnknown{Key: "widgets.g.example.com", Obj: crd}
	c.onDelete(tomb)
	got, ok := c.Lookup(schema.GroupKind{Group: "g.example.com", Kind: "Widget"})
	if ok {
		t.Fatalf("expected ok=false after tombstone delete, got %q ok=%v", got, ok)
	}
}

func TestCRDPluralCache_SameKindDifferentGroups_BothQueryable(t *testing.T) {
	c := newTestCache()
	c.onAddOrUpdate(newCRD("g1.example.com", "Widget", "widgets"))
	c.onAddOrUpdate(newCRD("g2.example.com", "Widget", "gadgets"))

	got1, ok1 := c.Lookup(schema.GroupKind{Group: "g1.example.com", Kind: "Widget"})
	got2, ok2 := c.Lookup(schema.GroupKind{Group: "g2.example.com", Kind: "Widget"})
	if !ok1 || got1 != "widgets" {
		t.Fatalf("group1 lookup: got (%q, %v), want (\"widgets\", true)", got1, ok1)
	}
	if !ok2 || got2 != "gadgets" {
		t.Fatalf("group2 lookup: got (%q, %v), want (\"gadgets\", true)", got2, ok2)
	}
}

func TestCRDPluralCache_NonCRDObject_NoOpNoPanic(t *testing.T) {
	c := newTestCache()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	c.onAddOrUpdate("not a CRD")
	c.onDelete(42)
	if len(c.plurals) != 0 {
		t.Fatalf("expected empty map after non-CRD events, got %d entries", len(c.plurals))
	}
}

func TestCRDPluralCache_ConcurrentLookupAndWrite_RaceFree(t *testing.T) {
	t.Parallel()

	const concurrentOps = 20

	c := newTestCache()
	c.onAddOrUpdate(newCRD("g.example.com", "Widget", "widgets"))

	var wg sync.WaitGroup
	for i := 0; i < concurrentOps; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			c.onAddOrUpdate(newCRD("g.example.com", "Widget", "widgets"))
		}()
		go func() {
			defer wg.Done()
			c.Lookup(schema.GroupKind{Group: "g.example.com", Kind: "Widget"})
		}()
	}
	wg.Wait()
}
