---
status: provisional
stage: alpha
latest-milestone: "v0.x"
---

# A Search Plugin for datumctl

- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
  - [Notes/Constraints/Caveats](#notesconstraintscaveats)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Command surface](#command-surface)
  - [Identity and context](#identity-and-context)
  - [Asking a question, getting an answer](#asking-a-question-getting-an-answer)
  - [Knowing what's searchable](#knowing-whats-searchable)
  - [Paging through results](#paging-through-results)
  - [Human-first output, script-friendly on demand](#human-first-output-script-friendly-on-demand)
  - [Errors that name the fix](#errors-that-name-the-fix)
  - [Read-only by design](#read-only-by-design)
  - [Discoverability: help, completion, and suggestions](#discoverability-help-completion-and-suggestions)
  - [Distribution through the plugin catalog](#distribution-through-the-plugin-catalog)
  - [Roadmap surfaces: filters, facets, and sort](#roadmap-surfaces-filters-facets-and-sort)
- [Production Readiness Review Questionnaire](#production-readiness-review-questionnaire)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Infrastructure Needed](#infrastructure-needed)

## Summary

The search service gives Datum fast, relevance-ranked, multi-tenant search over
platform resources: a user submits a `ResourceSearchQuery` and gets scored
results back **synchronously in the create response** — no polling, no index to
manage, tenancy enforced by the platform. Today the only way to ask that
question is raw `kubectl`: hand-authored YAML for every query,
`kubectl create -f query.yaml -o json | jq` to dig the hits out of the create
response's status, and an opaque continue token pasted back into YAML for every
next page. Worse, the resource is create-only — nothing is persisted, so a
search can never be `kubectl get`-ed back afterwards — which means `kubectl`'s
entire create-then-inspect mental model is actively misleading here. The power
is there; the experience is not.

**`datumctl search`** is a `datumctl` plugin that gives search a first-class
command-line experience, distributed through the same plugin catalog users
already use to extend the CLI. It turns the most common search workflows — find
a resource by a name fragment, scope a search to specific kinds, discover what
is searchable at all, page through results — into short, memorable commands
that read like the user's mental model ("what mentions payments?") rather than
the service's wire format. It inherits the user's
existing `datumctl` login and active org/project context, so there is no second
authentication and no place to leak credentials — and because search derives
tenancy from the request path itself, inherited context *is* tenant scoping.
And it follows the conventions developers already expect from `kubectl`, `gh`,
and `docker`: a rich human-readable default, a stable `-o json|yaml` contract
for automation, actionable errors, and shell completion.

The result is that finding things on Datum feels like asking a question —
`datumctl search "payments"` returns a relevance-ordered table of what matched
and where — instead of feeling like filing paperwork with a database.

## Motivation

Search is the interaction that humans reach for constantly: a developer hunting
for the workload behind an alert, a new team member orienting themselves in an
unfamiliar project, a CI job auditing that nothing still references a retired
service. Every one of those interactions today goes through `kubectl` against
the `search.miloapis.com/v1alpha1` API, and `kubectl` is a generic tool that
knows nothing about questions. That generality is the problem:

- **Every question is hand-authored YAML.** Asking "which resources mention
  payments?" means writing a `ResourceSearchQuery` manifest with `apiVersion`,
  `kind`, `metadata`, and `spec.query`, running `kubectl create -f query.yaml
  -o json`, and extracting the hits with
  `jq '.status.results[].resource.metadata.name'`. The service answers
  synchronously; the tooling makes the answer feel buried.
- **The create-then-inspect model is a lie here.** `ResourceSearchQuery` is
  create-only: the answer lives in the *create response's* status, nothing is
  persisted, and the search can never be gotten back afterwards. Every reflex
  `kubectl` trains — create it, then `get` it, then `describe` it — fails
  against this API. A query is a question, not a resource creation, and the
  tooling should make it feel like one.
- **Pagination is copy-paste surgery.** More results means copying an opaque
  continue token out of one response, pasting it into `spec.continue` in the
  YAML, and resubmitting — for every page.
- **Results are at the wrong altitude.** The API returns each match as the full
  resource object. The human question is "what matched, and where?" — a kind, a
  name, a tenant, a score — and answering it means `jq` spelunking through
  unstructured objects.
- **Coverage is invisible.** Only kinds with a ready `ResourceIndexPolicy` are
  searchable at all. There is no way to see that list short of reading policy
  status objects by hand, and when a query requests a kind that isn't indexed,
  the API's `deniedTargetResources` signal never surfaces anywhere a human
  would look — the query quietly succeeds on a subset.

None of this is a gap in the service — the API is deliberately synchronous,
stateless, and multi-tenant. It is a gap in the *experience*. A small, focused
CLI that treats a search as a question turns these interactions from YAML
authoring and `jq` spelunking into single commands, and does so without
inventing any new server-side surface: the plugin is a thin, well-mannered
client over the existing API.

### Goals

- **Make asking a question one command.** Searching everything, scoping to
  kinds, and paging should each be one short, memorable invocation — no
  hand-authored YAML for the everyday path.
- **Make coverage legible.** Surface the two facts `kubectl` hides — *what is
  searchable* (which kinds have a ready index policy) and *what was skipped*
  (`deniedTargetResources`) — as first-class, at-a-glance output.
- **Inherit the user's identity and context.** The plugin reuses the existing
  `datumctl` login and active org/project; there is no second sign-in and the
  plugin never handles a long-lived credential. Because search tenancy is
  derived from the request path, inherited context is also the tenancy
  boundary.
- **Honor the conventions developers already know.** A human table by default
  with a stable `-o json|yaml` contract for scripts, actionable errors with
  documented exit codes, and shell completion — matching
  `kubectl`/`gh`/`docker`.
- **Ship as a catalog plugin, not a core command.** Deliver through the
  `datumctl` plugin mechanism so search tooling can evolve on its own release
  cadence and be installed (or not) per user.
- **Stay a thin client.** Introduce no new server-side API, type, or behavior;
  the plugin is a presentation layer over the existing
  `search.miloapis.com/v1alpha1` API.

### Non-Goals

- **Replacing `kubectl` or the API.** Power users and pipelines keep full
  access to the raw API and declarative YAML. The plugin is the friendly path
  for the common case, not the only path.
- **Changing search semantics.** Relevance ranking, what gets indexed, tenancy
  scoping, and the create-only query model are defined by the service and are
  out of scope here. The plugin presents them; it does not redefine them.
- **Adding result-level access control.** Search deliberately indexes
  non-sensitive resources and gates access at the query, not per object. The
  plugin does not layer its own filtering on top — it renders what the service
  returns and is honest about the model.
- **Managing index policies.** The `ResourceIndexPolicy` lifecycle — defining
  what gets indexed — is platform-operator tooling, and folding it in here
  would confront consumers with machinery they can't use. It belongs in a
  separate, operator-focused plugin (or in `kubectl`/GitOps today); keeping it
  out keeps this plugin's surface unambiguous: every command is one a consumer
  can run.
- **Defining the plugin distribution mechanism itself.** How catalogs and the
  marketplace work is specified by the
  [datumctl plugin marketplace enhancement][marketplace]; we assume that
  mechanism and describe the search plugin that ships through it.
- **Building a graphical UI or web console.** This is a terminal experience.
- **Implementation sequencing.** The focus here is the intended user
  experience; engineering order is out of scope.

## Proposal

Introduce `datumctl search`, a `datumctl` plugin that presents the search
service as a small set of commands built around asking questions. It reuses the
user's active `datumctl` session and org/project context to call the
`search.miloapis.com/v1alpha1` API, and installs once from the plugin catalog:

```console
$ datumctl plugin install search
```

From then on, search is a verb in their everyday vocabulary. The surface is
deliberately consumer-only — two commands, both read-only:

- **`query`** (with `datumctl search "<text>"` as sugar) — ask a question and
  get a relevance-ordered answer (the `ResourceSearchQuery` resource, one
  synchronous create per page).
- **`kinds`** — see what is searchable right now (a consumer-friendly view over
  `ResourceIndexPolicy` readiness).

Roadmap capabilities — **filters**, **facets**, and **sort** — slot into the
same grammar when the API grows them (see
[Roadmap surfaces](#roadmap-surfaces-filters-facets-and-sort)).

The full experience — the command surface, how context and auth are inherited,
the query flow, coverage and paging, output modes, error design, and
distribution — is detailed in [Design Details](#design-details). The stories
below make it concrete through the people it serves.

### User Stories

#### Story 1: A developer finds a resource without writing YAML

Priya is on call and needs everything related to "payments" — she doesn't know
whether the thing she's hunting is a workload, a route, or a config. Today that
means authoring a query manifest and digging through the create response:

```console
$ cat > query.yaml <<EOF
apiVersion: search.miloapis.com/v1alpha1
kind: ResourceSearchQuery
metadata:
  name: find-payments
spec:
  query: "payments"
EOF
$ kubectl create -f query.yaml -o json \
    | jq -r '.status.results[].resource | .kind + "/" + .metadata.name'
```

With the plugin, the question is the command:

```console
$ datumctl search "payments"
10 results for "payments" in acme / net-core  (4 kinds searched)

KIND        NAME               NAMESPACE   TENANT     SCORE   AGE
Workload    payments-api       default     net-core   0.98    41d
HTTPRoute   payments-ingress   default     net-core   0.95    41d
Workload    payments-worker    batch       net-core   0.93    41d
ConfigMap   payments-config    default     net-core   0.91    12d
Project     payments-sandbox   ─           platform   0.88    130d
...

More results available. Next page:
  datumctl search "payments" --continue eyJxIjoicGF5bWVudHMiLCJzaWciOiJ...
```

What matched and where — kind, name, tenant, relevance — is the headline, in
descending relevance order, with the resolved org/project echoed so there is
never a question of which tenant was searched. To script the same thing, Priya
adds `-o json` and gets the real `ResourceSearchQuery` object back, full
resource payloads included.

#### Story 2: A developer scopes a search and learns what was skipped

Jonas knows he's looking for a workload or a route, so he scopes the search —
and one of the kinds he asks for turns out not to be indexed. The plugin says
so on stderr, names the fix, and still answers for the rest:

```console
$ datumctl search "checkout" --kind Workload --kind HTTPRoute
Warning: 1 of the 2 kinds you asked for isn't ready to search yet, so it was skipped:
           HTTPRoute (gateway.networking.k8s.io/v1)
         Run "datumctl search kinds" to see what's ready.

4 results for "checkout" in acme / net-core  (1 kind searched)

KIND       NAME               NAMESPACE   TENANT     SCORE   AGE
Workload   checkout-api       default     net-core   0.97    88d
Workload   checkout-worker    batch       net-core   0.90    88d
Workload   checkout-replay    batch       net-core   0.71    30d
Workload   cart-checkout-v2   default     net-core   0.64    9d
```

The query succeeded, so the exit code is `0` and the results are usable — but
the partial coverage that the raw API buries in `deniedTargetResources` is
impossible to miss, and the warning lives on stderr where it can't corrupt a
pipe.

#### Story 3: A CI job refuses to trust a half-covered answer

A nightly job verifies that no workload still references the retired
`legacy-billing` service. Zero hits must mean *really zero* — not "the kind
wasn't searched." The job runs in script mode with `--strict`:

```console
$ datumctl search query "legacy-billing" --kind Workload \
    --all --strict -o json --quiet > hits.json
$ jq -r '.status.results[].resource.metadata.name' hits.json
```

`--all` follows continue tokens until the last page, so the JSON is the
complete answer, and `-o json` is a stable contract with data on stdout and
diagnostics on stderr. If `Workload` ever loses its index policy, the same
invocation that would otherwise report zero matches instead fails the build
with a dedicated, documented exit code:

```console
error: stopping — some of the kinds you asked for aren't ready to search yet:
  Workload (compute.datum.net/v1alpha1)

run "datumctl search kinds" to see what's ready, or drop --strict to search the rest anyway
exit status 7   # SEARCH_PARTIAL_COVERAGE
```

The pipeline distinguishes "not fully searched" from "nothing found" and from
"auth failed" without scraping text.

#### Story 4: A new team member asks "what can I even search?"

Amara joined the team this week. Before she trusts search to answer anything,
she wants to know its boundaries. One command, no policy spelunking:

```console
$ datumctl search kinds
KIND        GROUP                          VERSION    READY   AGE
ConfigMap   (core)                         v1         True    201d
HTTPRoute   gateway.networking.k8s.io      v1         False   2d
Project     resourcemanager.miloapis.com   v1alpha1   True    201d
Workload    compute.datum.net              v1alpha1   True    88d
```

Every searchable kind and its readiness, at a glance. `-o wide` adds the
backing policy and index name — the details that make a "why isn't HTTPRoute
ready yet?" conversation with the platform team precise.

### Notes/Constraints/Caveats

- **Query-as-create is the wire truth; the plugin presents a question.** Every
  search is a create of a `ResourceSearchQuery` whose answer arrives in the
  create response's status — nothing is persisted and nothing can be fetched
  back later. The plugin embraces that: a search looks and feels like asking a
  question, and `-o yaml` always shows the real `ResourceSearchQuery` resource
  for users who want the wire form.
- **Results are read-only projections.** The plugin renders matched resources;
  it never mutates them. Acting on a result is a hand-off — `-o name` emits
  identifiers shaped for piping into other tools.
- **Multi-tenancy is inherited, never re-implemented.** The search service
  derives tenant scope from the request URL and identity — a query sent through
  a project control plane is automatically filtered to that tenant — and that
  is exactly the scope `datumctl` already encodes when it brokers a plugin's
  calls. The plugin adds no tenancy logic of its own; it surfaces the resolved
  scope and lets the platform enforce it.
- **Search covers indexed, non-sensitive resources — and says so.** Only kinds
  with a ready `ResourceIndexPolicy` are searchable, and results are not
  filtered by per-object RBAC; access is gated at the query (the `create` verb
  on `ResourceSearchQuery`). The plugin states this plainly in `--help` and
  never implies search is a complete or per-object-authorized inventory.
- **The Go types are the source of truth for what ships.** The service's API
  design document describes filters, facets (`ResourceFacetQuery`), and sort
  ordering that the `v1alpha1` types do not yet expose. The plugin treats those
  as roadmap (see
  [Roadmap surfaces](#roadmap-surfaces-filters-facets-and-sort)) and ships
  nothing speculative.

### Risks and Mitigations

#### Risk: A friendly search implies completeness and authority it doesn't have

A one-line search that answers instantly invites users to treat it as a full
inventory — assuming every kind is indexed and every result is RBAC-filtered
per object, neither of which is true.

*Mitigations:* `datumctl search kinds` makes the coverage boundary a
first-class, one-command answer. Partial coverage is loudly surfaced (the
denied-kinds warning, and `--strict` for automation that must not tolerate it).
Help text states the indexing and access model explicitly. Every result row
carries its tenant, so provenance is visible rather than assumed.

#### Risk: Output that looks human-friendly breaks scripts

Color, footers, and tables corrupt piped data if emitted unconditionally.

*Mitigations:* The plugin detects TTY vs pipe and disables color and
interactive footers when not attached to a terminal, honors `NO_COLOR`, and
treats `-o json|yaml` as a stable contract with data on stdout and all
diagnostics — including coverage warnings — on stderr. Machine output is never
decorated.

#### Risk: The plugin becomes a second, divergent definition of search behavior

A client that re-ranks results, re-filters coverage, or re-implements tenancy
would drift from the service and mislead users.

*Mitigations:* The plugin performs no ranking, filtering, or tenancy math —
it submits queries and renders responses. Relevance order is the server's
order; coverage is the server's `deniedTargetResources`; scope is whatever the
platform derived from the request. The plugin only presents them.

#### Risk: Credential and context confusion across tenants

A user could search the wrong org/project without realizing it and act on the
answer.

*Mitigations:* The plugin inherits the active context from `datumctl` (never
its own auth), echoes the resolved org/project in the results headline of every
search, and accepts `--org`/`--project` to override per invocation with the
same precedence (flags > env > config) the rest of `datumctl` uses.

#### Risk: UX and security review

The plugin changes how users authenticate to the search service and introduces
new terminal flows.

*Mitigations:* The credential-handling path (reuse of the `datumctl`
credentials helper, no long-lived token in the plugin) and the help text
describing the access model should be reviewed jointly by the CLI maintainers
and a security-minded reviewer; the query, coverage, and error flows should be
validated with real users — at minimum a developer hunting a resource, a new
team member discovering coverage, and a pipeline author scripting against
`-o json`.

## Design Details

This section describes the product experience: what users and scripts see and
do. It assumes the `datumctl` plugin contract and the catalog described in the
[marketplace enhancement][marketplace].

### Command surface

The plugin keeps the surface small — one primary question-shaped command and
one discovery view, capped at two levels under `datumctl`:

```console
# Searching (ResourceSearchQuery — one synchronous create per page)
datumctl search query "<text>" [--kind <Kind[.group][/version]>]... [--limit <n>] [--all] [--continue <token>] [--strict] [-o table|wide|json|yaml|name]
datumctl search "<text>"      # sugar: bare arguments that don't name a subcommand are the query; `--` forces literal terms

# Discoverability (consumer view over ResourceIndexPolicy)
datumctl search kinds [-o table|wide|json|yaml|name]
```

Global flags apply to every command: `-o table|wide|json|yaml|name` selects the
output format (table by default), `--quiet`, `--verbose`, and `--color` behave
as described below, and `--org`/`--project` override the active context. Two flags exist for development and end-to-end testing only
— `--kubeconfig` to point at a dev/e2e cluster directly and `-n`/`--namespace`
as a raw escape hatch — and are not part of the everyday, context-inheriting
workflow.

Design choices and their rationale:

- **The question is the positional.** `datumctl search "payments"` is the
  gesture users will make fifty times a day, so bare arguments that don't name
  a subcommand are treated as the query, and `--` forces literal terms for the
  rare query that collides with a subcommand name (`datumctl search -- kinds`
  searches for the word "kinds"). `datumctl search query` is the canonical
  spelling that docs and scripts use; the sugar exists for fingers, not for
  pipelines.
- **`kinds` is a view, not a resource.** The searchable-kinds answer is
  assembled from `ResourceIndexPolicy` objects, but consumers should never
  need to know that resource exists — the command is named for the question it
  answers, not for the machinery behind it. What grammar the plugin does have
  — output modes, flag names, context flags — matches the rest of `datumctl`,
  so learning one plugin teaches the others.

### Identity and context

The plugin reuses the user's existing `datumctl` session: there is no second
login, and the plugin holds no long-lived credential of its own. Every call is
scoped to the active org/project, so a user searches only their own tenant —
exactly as the service enforces.

Search makes this inheritance unusually clean. The service derives tenant scope
from the request itself: a query sent through a project control-plane URL is
automatically filtered to that tenant by the platform, with no tenancy field in
the query spec at all. Scoping a plugin call to the active project — which is
precisely what `datumctl`'s credential brokering does by encoding scope in the
control-plane URL path — is therefore not merely compatible with search
tenancy; it is the *same mechanism*. The plugin never re-derives or
second-guesses scope; it echoes the resolved org/project in every results
headline so it is always clear which tenant answered, and `--org`/`--project`
override per invocation with the standard precedence (flags > env > config).
Switching context is the same `datumctl` operation users already know. The
underlying contract — how `datumctl` hands a plugin its context and brokers
short-lived tokens — belongs to the [marketplace enhancement][marketplace];
this plugin simply consumes it.

### Asking a question, getting an answer

Searching is the defining action, and the plugin makes it a single command
that returns ranked results synchronously — exactly mirroring the API's
one-create-per-question model:

```console
$ datumctl search "payments" --kind Workload
6 results for "payments" in acme / net-core  (1 kind searched)

KIND       NAME               NAMESPACE   TENANT     SCORE   AGE
Workload   payments-api       default     net-core   0.98    41d
Workload   payments-worker    batch       net-core   0.93    41d
...
```

- **The match list is the headline.** Results arrive in the server's relevance
  order with the score visible, and the headline echoes the resolved
  org/project and how many kinds were actually searched — the two facts that
  determine whether this answer is the answer the user thinks it is. The
  headline counts what is shown; when more pages exist it says so rather than
  inventing a total the API does not report.
- **Default columns answer "what matched, and where."** `KIND`, `NAME`,
  `NAMESPACE`, `TENANT`, `SCORE`, `AGE` — the projection a human wants, lifted
  out of the full resource objects the API returns. `-o wide` adds the API
  group/version and the tenant type (`platform` vs `project`); `-o json|yaml`
  returns the real `ResourceSearchQuery` object with the complete unstructured
  resources in `.status.results` for scripts that need everything.
- **`-o name` is built for hand-offs.** It emits one
  `<kind>.<group>/<name>` per line (`workload.compute.datum.net/payments-api`;
  core-group kinds omit the group), shaped for `xargs`, command substitution,
  and follow-up `kubectl` invocations — search finds, other tools act.
- **`--kind` speaks in kinds, not GVK triples.** Users type `--kind Workload`,
  or `--kind Workload.compute.datum.net` to pin a group, or append `/v1alpha1`
  to pin a version. The plugin resolves the rest against the indexed-kinds list
  (the same data behind `datumctl search kinds`), and on ambiguity it stops and
  suggests the candidates rather than guessing. Nobody hand-writes
  `{group, version, kind}` objects.
- **An empty query is browse mode.** The API defines an empty query string as
  matching everything, so `datumctl search query "" --kind Workload` lists
  every indexed workload — useful for eyeballing what an index actually
  contains. The plugin documents this rather than hiding it.
- **`--limit` maps straight to the API.** Page size defaults to the server's
  (10) and is capped at the server's maximum (100); the plugin does not
  fabricate larger pages.

Under the hood a search creates a `ResourceSearchQuery` and reads back
`.status.results`, `.status.deniedTargetResources`, and `.status.continue`;
`-o yaml` shows the real resource for anyone who wants it.

### Knowing what's searchable

Search is only as trustworthy as its coverage is visible, so the plugin makes
coverage a first-class surface in both directions:

- **`kinds` answers "what can I search?"** It renders each kind that has a
  `ResourceIndexPolicy`, with its group, version, and readiness, assembled from
  policy status. Consumers get the boundary of the searchable world in one
  command without ever learning that index policies exist. When a kind is
  missing or not ready, the fix lives with the platform operators who own
  index policies — `kubectl`/GitOps today, an operator-focused plugin tomorrow
  — and this plugin's job is to make that conversation precise: `-o wide` (and
  `--verbose`) adds the backing policy and index name, so the request reads
  "policy X for kind Y isn't ready," not "search seems broken."
- **Denied coverage is loud, but not fatal.** When a query requests kinds the
  service can't search, the API still answers for the rest and reports the
  skipped kinds in `status.deniedTargetResources`. The plugin turns that into a
  stderr warning naming each skipped kind and pointing at
  `datumctl search kinds` (Story 2). The exit code stays `0` — the query
  succeeded — because a human iterating at a terminal should get their partial
  answer, not a failure.
- **`--strict` is for automation that can't accept "partial."** Under
  `--strict`, any denied kind fails the command with the dedicated
  `SEARCH_PARTIAL_COVERAGE` exit code (Story 3), so CI can insist that zero
  results means zero matches, not zero coverage. The strictness is opt-in
  because it changes what "success" means; the default favors the interactive
  user.

### Paging through results

The API paginates with an opaque continue token in `.status.continue`; the
plugin turns that into three honest affordances:

- **At a terminal, the next page is a copy-paste.** When more results exist,
  a footer names the exact next command, token included (Story 1). No YAML
  surgery, no guessing at flag order.
- **`--all` follows the tokens for you.** It fetches page after page — each a
  fresh create, bounded by the server's 100-per-page maximum — until
  `.status.continue` comes back empty, then renders the union. Under
  `-o json|yaml` it emits a single `ResourceSearchQuery` object whose
  `.status.results` is the concatenation of every page and whose
  `.status.continue` is empty, so scripts see one complete answer.
- **Scripts can drive the cursor themselves.** `-o json` exposes
  `.status.continue` verbatim; a script passes it back via `--continue` to
  fetch the next page on its own schedule.

One property deserves calling out because it will surprise people the first
time: continue tokens are **bound to the query they were issued for**. The
server signs each token against the query text, `--limit`, and `--kind` set,
and rejects a token replayed with different parameters — changing any flag
mid-pagination is starting a different question. The plugin surfaces that
rejection as a clear, named error (see below) instead of a generic 4xx, and
`--help` for `--continue` states the binding up front.

### Human-first output, script-friendly on demand

The plugin is built for a human at a terminal first and a script second, with
the machine path explicit and stable:

- **Default:** an aligned, color-coded table sized to the terminal. Color
  reinforces meaning but is never the *only* signal — readiness shows
  `True`/`False` as text, scores are printed numerically — so meaning survives
  monochrome terminals, screen readers, and color-blind users.
- **`-o json|yaml`:** a stable, versioned contract for automation — the real
  `search.miloapis.com/v1alpha1` objects, not a bespoke schema. Field names and
  shapes don't change without a deprecation path; data goes to stdout, all
  logs, warnings, and pagination footers go to stderr, so `... -o json >
  out.json` is always clean.
- **`-o wide` / `-o name` / `--quiet`:** progressive density — extra columns
  for humans who want them, bare identifiers for `xargs`/command substitution.
- **TTY awareness and `NO_COLOR`:** color and interactive footers auto-disable
  when stdout is piped or `NO_COLOR` is set; `--color=auto|always|never`
  overrides.
- **Exit codes are a contract.** `0` on success — including a query that finds
  nothing, because "no matches" is an answer, not an error — and distinct,
  documented non-zero codes for distinct failure classes (see below), never
  `0` on a failure.

### Errors that name the fix

The signature search failures get first-class treatment instead of being
flattened into generic Kubernetes errors. Every error states what happened,
why, and a concrete next action. The plugin matches `datumctl` core's error
frame rather than inventing its own: a lowercase red `error:` line, then — after
a blank line — an unlabeled, dimmed advice line carrying the next command. The
`exit status N   # SYMBOL` trailer shown below each example is a `--verbose`-only
diagnostic; by default only the two lines above it print. The process exit code
is the machine contract, and it is unchanged.

- **Unknown or unindexed kind:** names the nearest matching kind and points at
  the coverage command.

  ```console
  error: "Wrkload" isn't a searchable kind — did you mean Workload (compute.datum.net)?

  run "datumctl search kinds" to see everything you can search
  exit status 4   # SEARCH_NOT_FOUND
  ```

- **Expired or mismatched continue token:** tells a token that has aged out
  apart from one reused with different parameters, so the fix is never a guess.

  ```console
  error: that continue token has expired or isn't valid anymore — they only work for a limited time

  rerun the search without --continue to start over
  exit status 6   # SEARCH_INVALID
  ```

- **Forbidden:** names the role that grants access and distinguishes "you
  aren't authorized" from "search isn't enabled here."

  ```console
  error: you don't have permission to search in net-core — searching needs the "search.miloapis.com-searcher" role

  ask an admin for that role, or check that search is turned on for this project
  exit status 3   # SEARCH_FORBIDDEN
  ```

- **Service unreachable or expired session:** an unreachable service tells you
  to retry; an expired session (same exit code) tells you to sign back in.

  ```console
  error: couldn't reach the search service for acme / net-core

  check your connection and try again in a moment; the service may be briefly unavailable
  ```
  ```console
  error: your session has expired or was signed out

  run "datumctl login", then retry
  exit status 8   # SEARCH_UNAVAILABLE
  ```

The full exit-code vocabulary is small, stable, and documented, so automation
branches on numbers, never on message text:

| Code | Symbol                    | Meaning                                                        |
|------|---------------------------|----------------------------------------------------------------|
| 0    | `OK`                      | Success — including a query with zero matches.                 |
| 1    | `SEARCH_ERROR`            | Unexpected or uncategorized failure.                           |
| 2    | `SEARCH_USAGE`            | Invalid flags/arguments, including reserved roadmap flags.     |
| 3    | `SEARCH_FORBIDDEN`        | Not authorized, or search not enabled for the project.         |
| 4    | `SEARCH_NOT_FOUND`        | A named thing doesn't exist — notably an unknown kind.         |
| 6    | `SEARCH_INVALID`          | Request rejected — notably a bad or mismatched continue token. |
| 7    | `SEARCH_PARTIAL_COVERAGE` | Under `--strict` only: requested kinds were not searchable.    |
| 8    | `SEARCH_UNAVAILABLE`      | Search API unreachable or session expired.                     |

(Codes 5 and 9 are reserved to keep numbering aligned across `datumctl`
plugins.)
`SEARCH_PARTIAL_COVERAGE` is the domain-signature code — the search analogue of
IPAM's pool-exhausted `7` — because "the answer is incomplete" is the failure
mode unique to this service that automation most needs to catch. Stack traces
are suppressed by default and available under `--verbose`/`--debug`.

### Read-only by design

Every command in this plugin is read-only, and that is a deliberate product
decision, not an accident of scope. A search allocates nothing, persists
nothing, and can be rerun freely; `kinds` is a bounded read rendered
client-side. Nothing prompts, nothing confirms, nothing needs a `--dry-run`,
and there is no flag whose absence can hang a script on a question nobody is
there to answer. The plugin is safe to hand to anyone with search access, safe
to run in a watch loop, and safe to alias without a second thought.

Acting on what search finds is deliberately a hand-off, not a feature:
`-o name` emits identifiers shaped for `xargs`, command substitution, and
follow-up `kubectl` invocations. Search finds; other tools — with their own
confirmations and their own audit trails — act.

### Discoverability: help, completion, and suggestions

- **Example-led help.** Every command answers `-h`/`--help` and bare invocation
  with a one-line description, a usage synopsis, and runnable examples — the
  terminal is the documentation. The top-level help states the access model in
  one honest sentence: search covers indexed, non-sensitive resources and is
  scoped by tenant, not filtered per object.
- **"Did you mean?"** Unknown subcommands and flags suggest the nearest valid
  one; unknown kinds suggest the nearest searchable kind (the
  `SEARCH_NOT_FOUND` example in
  [Errors that name the fix](#errors-that-name-the-fix)).
- **Shell completion** for bash/zsh/fish/powershell, including *dynamic*
  completion of `--kind` values by querying the API — the indexed-kinds list
  is small and cheap, and completing it eliminates the GVK guesswork entirely.
- **Progressive disclosure.** Top-level `search --help` shows the question
  (`query`, the sugar form, `kinds`); niche flags (`--continue`, `--strict`)
  live under the specific subcommand's help.

### Distribution through the plugin catalog

The plugin ships through the `datumctl` plugin catalog, so it inherits that
ecosystem wholesale: users install it with `datumctl plugin install search`,
the download is integrity-checked, it carries the catalog's **official** trust
badge, it versions on its own cadence independent of `datumctl` core releases,
and users who never search simply never install it. The catalog format, install
flow, trust model, and version-compatibility checks are all defined by the
[marketplace enhancement][marketplace]; the search plugin simply joins that
ecosystem rather than restating it.

### Roadmap surfaces: filters, facets, and sort

The service's API design describes a richer query surface than `v1alpha1`
ships today: structured **filters** over fields marked filterable, **facet**
counts via a `ResourceFacetQuery` resource, and explicit **sort** ordering.
None of these exist in the current Go types — full-text relevance is the whole
of what the API exposes — and the plugin ships nothing the API cannot honor.

But the grammar is reserved now so the experience stays coherent when the API
grows:

```console
datumctl search "payments" --filter 'spec.location == "us-west"'   # reserved
datumctl search "payments" --sort metadata.creationTimestamp:desc  # reserved
datumctl search facets --kind Workload                             # reserved
```

Until the corresponding API surface lands, the plugin rejects these flags with
a clear usage error (`SEARCH_USAGE`) that names them as planned capabilities of
the search API — rather than silently dropping the intent or emulating them
client-side, which would create exactly the divergent-behavior risk this
design rules out. When `ResourceFacetQuery` and the filter/sort fields ship,
these become the natural extension of the surface already established.

## Production Readiness Review Questionnaire

The plugin is **entirely client-side**. It introduces no search control-plane
components, no API types, and no server-side behavior, so the cluster-oriented
portions of the standard PRR questionnaire do not apply. The relevant readiness
considerations are captured below.

### Feature enablement and rollback

- The capability ships as an optional plugin installed via
  `datumctl plugin install search`. Users who never install it see no change.
- The plugin is purely additive over the existing API; `kubectl` and raw YAML
  continue to work unchanged for every workflow.
- Rolling back is uninstalling the plugin (`datumctl plugin remove search`) or
  pinning a prior version. The plugin holds no state at all and mutates
  nothing — searches persist nothing on the server or the client — so removing
  it leaves nothing behind.

### Monitoring and supportability

- Users can always confirm what the plugin did: `-o yaml` on any search shows
  the exact `ResourceSearchQuery` exchanged with the server, and since the
  plugin issues nothing but queries and bounded reads, that is the whole
  story.
- `--verbose` surfaces the resolved org/project, the API host, and the exact
  API calls made, which is the primary support surface for "why did I get this
  result?"
- The plugin degrades gracefully: an unreachable API or an expired session
  produces a clear, actionable message (re-run `datumctl login`) rather than a
  stack trace.

### Dependencies

- The plugin depends on a working `datumctl` installation (for dispatch,
  credentials, and active context) and on reachability of the search API for
  the active org/project. A search outage affects the plugin exactly as it
  affects `kubectl` against the same API; the plugin adds no new dependency.

### Scalability

- The plugin issues the same API calls a user would make through `kubectl`:
  creates of `ResourceSearchQuery`, plus a single bounded list of index
  policies to back `kinds` and `--kind` resolution, rendered client-side. It
  adds no new server load beyond what the equivalent manual workflow
  generates. `--all` is strictly sequential — one page in flight at a time,
  each bounded by the server's 100-result maximum.

### Security

- The reuse of `datumctl` credentials, and the help text describing search's
  access model (query-gated, tenant-scoped, not per-object RBAC-filtered),
  should receive a joint CLI + security review before the plugin stabilizes
  (see [Risks and Mitigations](#risks-and-mitigations)).

## Implementation History

- (provisional) Enhancement drafted, focusing on the product experience for a
  `datumctl` search plugin: a question-shaped, deliberately consumer-only and
  read-only command surface over the existing `search.miloapis.com/v1alpha1`
  API, inherited identity/context, first-class coverage and pagination,
  human-first/script-friendly output, and catalog distribution.

## Drawbacks

- **It adds a surface to maintain.** A purpose-built CLI must track the search
  API as it evolves (notably the arrival of filters, facets, and sort). This is
  mitigated by keeping the plugin a thin presentation layer with no ranking,
  filtering, or tenancy logic to drift, and by shipping it on its own cadence
  through the catalog.
- **It introduces a second way to do things.** Some users will use the plugin
  and some raw `kubectl`/YAML, which can fragment documentation and muscle
  memory. This is mitigated by `-o yaml` always exposing the real resources (so
  the plugin is a bridge to, not a replacement for, the API) and by positioning
  the plugin as the friendly path for the common case, not a parallel universe.
- **A friendlier front door invites misplaced trust.** Making search effortless
  increases the odds that a half-covered or tenant-scoped answer is mistaken
  for a complete inventory. This is mitigated by the coverage surfaces —
  `kinds`, the denied-kinds warning, `--strict`, the tenant column, and honest
  help text — but the ease itself is a real change in how the service gets
  used.

## Alternatives

- **Do nothing; keep using `kubectl`.** Zero new surface, but it leaves every
  question as hand-authored YAML against a create-only resource that `kubectl`'s
  mental model actively misrepresents, pagination as token surgery, and
  coverage invisible — exactly the gaps the plugin closes. The raw API remains
  available as the power-user path regardless.
- **Build search commands into `datumctl` core instead of a plugin.** This
  would put search front and center but couples its release cadence to the
  core CLI, bloats the binary for users who never search, and bypasses the
  catalog's trust and versioning model. The plugin path gets the same UX with
  independent evolution.
- **Ship a `kubectl` plugin (`kubectl search`) instead.** This would serve
  `kubectl`-native users but would not inherit `datumctl`'s identity, active
  org/project context, or the Datum plugin catalog — and since the search
  service keys tenancy off exactly the context `datumctl` supplies, this
  reintroduces the auth and scoping problems the `datumctl` plugin model
  solves for free.
- **Generate a generic CLI from the API (CRUD over every field).** A mechanical
  generator would produce exactly the experience we're trying to escape — a
  `create` command for a resource that is really a question, raw unstructured
  results instead of a match table, no kind resolution, no coverage warnings,
  no pagination affordances. The value here is the search-specific UX, not
  generic CRUD.
- **Wrap the API in shell aliases or a `jq` cookbook.** Cheap to start, but it
  can't deliver completion, stable machine output, TTY-aware rendering, kind
  resolution, or the coverage and pagination surfaces, and it has no
  distribution or trust story. The catalog plugin provides all of these.

## Infrastructure Needed

- A repository and release pipeline that publishes the plugin to the `datumctl`
  plugin catalog (per the [marketplace enhancement][marketplace]), and an entry
  in the curated catalog so it installs via `datumctl plugin install search`
  with the **official** trust badge.

[marketplace]: https://github.com/datum-cloud/datumctl/blob/main/docs/proposals/datumctl-plugin-marketplace/README.md
