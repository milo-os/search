# milo-search

The search plugin for `datumctl`. It presents the `search.miloapis.com/v1alpha1`
API as a small, question-shaped, read-only command surface, turning the common
search workflows — ask a question, scope it to kinds, discover what is
searchable, page through results — into single commands instead of hand-authored
`ResourceSearchQuery` YAML and `jq` spelunking.

See the full enhancement at [`docs/enhancements/cli-plugin.md`](enhancements/cli-plugin.md).

## Install

```bash
datumctl plugin install search
```

From then on, search is a verb in your everyday `datumctl` vocabulary
(`datumctl search "payments"`). The plugin reuses your active `datumctl` login
and org/project context; there is no second sign-in and it holds no long-lived
credential of its own.

## Command surface

```text
# Searching (ResourceSearchQuery — one synchronous create per page)
datumctl search query "<text>" [--kind <Kind[.group][/version]>]... [--limit n] [--all] [--continue <token>] [--strict] [-o table|wide|json|yaml|name]
datumctl search "<text>"        # sugar: bare args that don't name a subcommand are the query; `--` forces literal terms

# Discoverability (consumer view over ResourceIndexPolicy)
datumctl search kinds [-o table|wide|json|yaml|name]
```

Alias: `q` → `query`.

### Asking a question

```bash
datumctl search "payments"
datumctl search "checkout" --kind Workload --kind HTTPRoute --limit 25
datumctl search query "" --kind Workload         # empty query = browse mode
```

`--kind` speaks in kinds, not GVK triples: type `Workload`, or
`Workload.compute.datum.net` to pin a group, or append `/v1alpha1` to pin a
version. The plugin resolves the rest against the indexed-kinds list and, on an
ambiguous or unknown kind, stops and suggests candidates rather than guessing.

### Knowing what's searchable

```bash
datumctl search kinds          # KIND / GROUP / VERSION / READY / AGE
datumctl search kinds -o wide  # adds the backing POLICY and INDEX name
```

Only kinds with a ready index policy are searchable. When a query requests a
kind that isn't searchable, the API reports it in `deniedTargetResources`; the
plugin turns that into a stderr warning and keeps going. `--strict` makes any
denied kind fail the command (exit 7) so automation can insist that zero results
means zero matches, not zero coverage.

### Paging

The API paginates with an opaque continue token:

- At a terminal, a stderr footer names the exact next-page command, token
  included.
- `--all` follows the tokens for you and returns every page merged (under
  `-o json|yaml`, one object with the concatenated results and an empty
  continue).
- Scripts can read `.status.continue` from `-o json` and pass it back via
  `--continue`.

Continue tokens are **bound** to the query text, `--kind` set, and `--limit`
they were issued for; changing any of those mid-pagination starts a different
question and the token is rejected (exit 6).

## Output and exit codes

- Default output is a human table. `-o json|yaml` is a stable contract — the
  real `search.miloapis.com/v1alpha1` objects, with data on **stdout** and all
  warnings/footers/errors on **stderr**, so `... -o json > out.json` is clean.
- `-o wide` adds columns; `-o name` emits `<kind>.<group>/<name>` per line
  (core-group kinds omit the group) for `xargs`/command substitution;
  `--quiet` drops the headline and pagination footer.
- Color auto-disables off a TTY, honors `NO_COLOR`, and obeys
  `--color=auto|always|never`. Color is never the sole signal (readiness shows
  `True`/`False`; scores are numeric).
- Errors follow `datumctl` core's frame: a lowercase red `error:` line, then an
  unlabeled, dimmed advice line with the next command. The process exit code is
  the machine contract; the `exit status N   # SEARCH_SYMBOL` trailer is a
  diagnostic that prints only under `--verbose`/`--debug`.

Exit codes are a contract (the symbolic name shows in the `--verbose` trailer):

| Code | Name                    | Meaning                                                        |
|------|-------------------------|----------------------------------------------------------------|
| 0    | OK                      | success, including a query with zero matches                   |
| 1    | SEARCH_ERROR            | unexpected / uncategorized failure                             |
| 2    | SEARCH_USAGE            | invalid flags or arguments, including reserved roadmap flags   |
| 3    | SEARCH_FORBIDDEN        | not authorized, or search not enabled for the project          |
| 4    | SEARCH_NOT_FOUND        | a named thing doesn't exist — notably an unknown kind          |
| 6    | SEARCH_INVALID          | request rejected — notably a bad or mismatched continue token  |
| 7    | SEARCH_PARTIAL_COVERAGE | `--strict` only: requested kinds were not searchable           |
| 8    | SEARCH_UNAVAILABLE      | search API unreachable or session expired                      |

Codes 5 and 9 are reserved to keep numbering aligned across `datumctl` plugins.
Stack traces are suppressed by default and available under `--verbose`/`--debug`.

## Access model

Search covers indexed, non-sensitive resources and is gated at the query (the
`create` verb on `ResourceSearchQuery`), not filtered per object. Tenancy is
inherited from the request path: a query sent through a project control plane is
automatically scoped to that tenant. Every result row carries its tenant, and
each search echoes the resolved org/project in its headline.

## Roadmap flags

`--filter`, `--facet`, and `--sort` are reserved for search-API capabilities
that `v1alpha1` does not yet expose. They are registered but rejected with a
usage error (exit 2) naming them as planned, rather than emulated client-side.

## Reserved flags and dev mode

Two flags exist for development and end-to-end testing only and are not part of
the everyday, context-inheriting workflow:

- `--kubeconfig` targets a dev/e2e cluster directly (and forces kubeconfig
  transport).
- `-n`/`--namespace` is a raw escape hatch; the search resources are
  cluster-scoped, so it has no effect on them.

Against the dev kind cluster (see the repo `Taskfile.yaml` and
`task test-infra:cluster-up`), with its kubeconfig active:

```bash
go build -o bin/milo-search ./cmd/milo-search
export KUBECONFIG=$HOME/.kube/config      # kind-test-infra context
bin/milo-search kinds --kubeconfig "$KUBECONFIG"
bin/milo-search "payments" --kubeconfig "$KUBECONFIG"
```

No `DATUM_*` variables are required for kubeconfig mode.

## Build and test

```bash
go build -o bin/milo-search ./cmd/milo-search   # or: task build:plugin
go test ./cmd/milo-search/...
```

## Plugin manifest

`datumctl` discovers the plugin via the manifest:

```bash
bin/milo-search --plugin-manifest
```
