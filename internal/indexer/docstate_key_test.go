package indexer

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// oddDocIDs are document ids that are not valid JetStream KV keys as they
// stand, or that collide with the encoded form if encoding were careless.
var oddDocIDs = []string{
	"a b",
	"pod.default",
	".leading",
	"trailing.",
	"a..b",
	"..",
	"a/b",
	"a/b/",
	"=",
	"=YQ",
	"a=b",
	"a*b>c",
	"ünïcödé",
	"tab\there",
	strings.Repeat("x", 511) + ".",
}

// natsKeyRe and validNATSKey restate the JetStream KV key rules, plus the
// subject rule that no token between dots may be empty.
var natsKeyRe = regexp.MustCompile(`^[-/_=\.a-zA-Z0-9]+$`)

func validNATSKey(key string) bool {
	if key == "" || key[0] == '.' || key[len(key)-1] == '.' || strings.Contains(key, "..") {
		return false
	}
	return natsKeyRe.MatchString(key)
}

func TestDocStateKey_ObjectUIDsAreUnchanged(t *testing.T) {
	uid := "6f1c2a9e-3b7d-4c55-9a0e-2d8b1f4e7c63"
	assert.Equal(t, "proxies/"+uid, docStateKey("proxies", uid),
		"records written before key encoding must still be found")
	assert.Equal(t, "my_index-2/Some_ID-9", docStateKey("my_index-2", "Some_ID-9"))
}

func TestDocStateKey_AnyIDIsAValidKey(t *testing.T) {
	for _, id := range append([]string{"", "plain"}, oddDocIDs...) {
		key := docStateKey("proxies", id)
		assert.True(t, validNATSKey(key), "document id %q gave invalid key %q", id, key)
	}
	assert.True(t, validNATSKey(docStateKey("odd.index", "x")))
}

func TestDocStateKey_IsInjective(t *testing.T) {
	ids := append([]string{"", "a", "YQ", "PVlR"}, oddDocIDs...)
	indexes := []string{"proxies", "proxies/a", "=cHJveGllcw", "a.b"}

	seen := make(map[string][2]string)
	for _, index := range indexes {
		for _, id := range ids {
			key := docStateKey(index, id)
			if prev, ok := seen[key]; ok && prev != [2]string{index, id} {
				t.Errorf("index %q id %q and index %q id %q share key %q", index, id, prev[0], prev[1], key)
			}
			seen[key] = [2]string{index, id}
		}
	}
	assert.Equal(t, docStateKey("proxies", "a b"), docStateKey("proxies", "a b"), "the key must be deterministic")
}
