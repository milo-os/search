# API Design

The Search service provides APIs for searching resources and managing the
resources available to search. Platform operators can quickly use this service
to search for resources they need across the platform.

## Motivation

The platform supports quick and easy registration of resources though Custom
Resource Definitions and API aggregation. Given this, it's important our search
service can adjust dynamically at runtime to resources being indexed and
searchable so it doesn't require software changes to index new resources.

## Goals

- Introduce a dynamic method of configuring which resources in the platform
  should be indexed and searchable
- Enable platform operators to quickly search across resource types while also
  allowing field based selection to narrow the scope of the search
- Support full-text search capabilities configurable through dynamic index
  policies

## Proposal

The Search API will initially offer these new API resources to end-users:

- **ResourceIndexPolicy** configures which resources in the platform should be
  indexed and which fields within the resource are applicable to indexing
- **ResourceResourceSearchQuery** allows users to execute field filtering and full-text
  searching capabilities across all indexed resources
- **ResourceFacetQuery** allows users to retrieve facet counts for building
  filter UIs, independent of search results

> [!IMPORTANT]
>
> The system does not support AuthZ based policies to ensure users have access
> to results returned by search queries. This API should only be used for
> non-sensitive resources and for internal-use only.
>
> This service will be made multi-tenant friendly in the future.

## Design Details

> [!NOTE]
>
> The search service is eventually consistent. Resources are indexed
> asynchronously, so search results may not immediately reflect changes made in
> the control plane. Recently created, updated, or deleted resources may take a
> short time to appear in or disappear from search results.

### Resource index policies

The `ResourceIndexPolicy` resource will be managed by platform operators to
control which resources in the platform are made available to users. The search
service will begin indexing a policies immediately after a policy is created.

The resource index policy will allow users to define the resource applicable to
the index policy and filter resources using CEL expressions. Index policies will
also allow users to configure which fields from the resource are indexed and how
they're indexed (field filtering, full-text search, etc.)

Here's an example index policy that would configure search service to index all
active organizations.

```yaml
apiVersion: search.miloapis.com/v1alpha1
kind: ResourceIndexPolicy
metadata:
  name: organizations
spec:
  # Identifies the resource type this policy applies to. Uses a versioned
  # reference since field paths may differ between API versions.
  targetResource:
    group: resourcemanager.miloapis.com
    version: v1alpha1
    kind: Organization

  # CEL expressions that filter which resources are indexed. Multiple
  # conditions can be specified and are evaluated with OR semantics - a
  # resource is indexed if it satisfies ANY condition. Use && within a
  # single expression to require multiple criteria together.
  #
  # Each condition has:
  # - name: A unique identifier for the condition, used in status reporting
  #   and debugging to identify which condition(s) matched a resource.
  # - expression: A CEL expression that must evaluate to a boolean. The
  #   resource is available as the root object in the expression context.
  #
  # Available CEL operations:
  # - Field access: spec.replicas, metadata.name, status.phase
  # - Map access: metadata.labels["app"], metadata.annotations["key"]
  # - Comparisons: ==, !=, <, <=, >, >=
  # - Logical operators: &&, ||, !
  # - String functions: contains(), startsWith(), endsWith(), matches()
  # - List functions: exists(), all(), size(), map(), filter()
  # - Membership: "value" in list, "key" in map
  # - Ternary: condition ? trueValue : falseValue
  conditions:
    # Index resources that are ready and not being deleted
    - name: active-resources
      expression: |
        status.conditions.exists(c, c.type == 'Ready' && c.status == 'True')
        && !has(metadata.deletionTimestamp)
    # Also index resources in production namespaces regardless of status
    - name: production-resources
      expression: metadata.namespace.startsWith("prod-")

  # Defines which fields from the resource are indexed and how they behave
  # in search operations.
  fields:
    # The JSONPath to the field value in the resource. Supports nested paths
    # and map key access using bracket notation.
    - path: metadata.name
      # When true, the field value is included in full-text search operations.
      # The value is tokenized and analyzed for relevance-based matching.
      searchable: true
      # When true, the field can be used in filter expressions for exact
      # matching, range queries, and other structured filtering operations.
      filterable: true
      # When true, the search service will return aggregated counts of unique
      # values for this field. Enables clients to discover available filter
      # values and build faceted navigation interfaces.
      facetable: true

    - path: metadata.annotations["kubernetes.io/description"]
      searchable: true
      filterable: true
      facetable: false
```

The index policy will used a versioned reference to resources since the field
paths for resources may be different between versions. The system should monitor
for deprecated resource versions being referenced in index policies.

> [!NOTE]
>
> The `filterable` and `facteable` options are forward looking options that will
> be added to the search service in a future release. The primary functionality
> we will target for launch is **full-text searching**.

#### Condition evaluation

Conditions provide fine-grained control over which resource instances are
indexed. When multiple conditions are specified, they are evaluated using OR
semantics - a resource is indexed if it satisfies ANY condition. This allows
defining multiple independent criteria for inclusion.

Use `&&` within a single CEL expression when you need AND semantics:

```yaml
conditions:
  - name: ready-in-production
    expression: |
      status.conditions.exists(c, c.type == 'Ready' && c.status == 'True')
      && metadata.namespace.startsWith("prod-")
```

Conditions are re-evaluated when resources change. If a resource no longer
satisfies any condition (e.g., it transitions from Ready to NotReady), it will
be removed from the search index. Similarly, resources that begin satisfying
a condition after an update will be added to the index.

### Resource search queries

> [!NOTE]
>
> Field filtering and facets are advanced functionality and represent a
> forward-looking design. The initial release of the search service will target
> full-text searching only.

The `ResourceResourceSearchQuery` resource allows users to execute searches across all
indexed resources, combining full-text search with field-based filtering.

**Full-text search** (the `query` field) performs relevance-based matching across
all fields marked as `searchable` in the applicable index policies. The search
service tokenizes and analyzes both the query string and field values, enabling
matches even when terms appear in different order or with slight variations.
Results are ranked by relevance—resources that better match the query appear
first, and each result includes a `relevanceScore` (0 to 1) indicating match
quality. For example, searching "nginx frontend" would match resources containing
both terms, with resources where these terms appear prominently ranked higher.

**Filters** (the `filters` field) perform exact, structured matching on fields
marked as `filterable`. Unlike full-text search, filters are binary—a resource
either matches or it doesn't. Filters don't affect relevance ranking; they simply
include or exclude resources from the result set. Use filters for precise
criteria like `metadata.namespace == "production"` or `spec.replicas >= 3`.

When both `query` and `filters` are specified, the search service first applies
filters to narrow the candidate set, then performs full-text search within that
set. This allows queries like "find resources matching 'nginx' that are in
production with at least 2 replicas."

```yaml
apiVersion: search.miloapis.com/v1alpha1
kind: ResourceResourceSearchQuery
metadata:
  name: find-production-deployments
spec:
  # Full-text search string. Searches all fields marked as searchable in the
  # applicable index policies.
  query: "nginx frontend"

  # CEL expressions for field-based filtering. Only fields marked as filterable
  # can be used. Each expression supports full boolean logic (&&, ||). Multiple
  # filters are combined with OR logic.
  filters:
    - # Identifier for the filter. Used in error messages and for documentation.
      name: prod-high-replica
      # CEL expression that must evaluate to a boolean.
      expression: 'metadata.namespace == "production" && spec.replicas >= 2'
    - name: staging-any
      expression: 'metadata.namespace == "staging"'

  # Limit search to specific resource types. When empty, searches all indexed
  # resource types but only full-text search is allowed (no filters or sort).
  # When specified, filters and sort fields must be configured in all applicable
  # index policies.
  resourceTypes:
    - group: apps
      kind: Deployment

  # Maximum results per page (default: 10, max: 100).
  limit: 25

  # Pagination cursor from a previous query response.
  continue: ""

  # Explicit sort ordering. When omitted, results are ordered by relevance
  # score for full-text queries.
  sort:
    # Field path to sort by (must be marked as filterable).
    field: metadata.creationTimestamp
    # Sort direction, either "asc" or "desc".
    order: desc

status:
  # Array of matched resources. Each result contains the full resource object
  # as it exists in the cluster.
  results:
    - # The full matched resource object.
      resource:
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: nginx-frontend
          namespace: production
        # ... full resource
      # Relevance score from 0 to 1, where higher values indicate better matches.
      # Only present when query is specified. Results are sorted by this score
      # descending unless explicit sort ordering is specified.
      relevanceScore: 0.92

  # Pagination cursor for the next page. Present only when more results exist.
  # Pass this value to the continue field in a subsequent query to fetch the
  # next page.
  continue: "eyJvZmZzZXQiOjI1fQ=="

  # Approximate total number of matching resources across all pages. This is
  # an estimate and may change as resources are added or removed.
  estimatedTotalHits: 142
```

#### Field validation

Fields used in filters, sorting, and facets are validated against the
`ResourceIndexPolicy` for each requested resource type:

| Query Scope | Validation Behavior |
|-------------|---------------------|
| All resources (`resourceTypes` empty) | Full-text search only; filters, sort, and facets are rejected |
| Explicit resource types | Fields must be configured in **all** applicable policies |

When multiple resource types are specified, field validation uses the
**intersection** of field configurations. A field must be marked appropriately
(filterable, sortable, or facetable) in every policy to be usable:

```
resourceTypes: [Deployments, Services]

Policy for Deployments: metadata.name (filterable), spec.replicas (filterable)
Policy for Services:    metadata.name (filterable), spec.type (filterable)

Allowed filter fields: metadata.name (common to both)
Rejected: spec.replicas, spec.type (not in all policies)
```

This ensures filters apply uniformly to all results rather than being silently
ignored for some resource types. To filter on resource-specific fields, query
that resource type individually.

#### Supported filter operations

Each filter expression accepts CEL syntax with the following supported
operations:

| Operation | Example |
|-----------|---------|
| Equality | `metadata.namespace == "production"` |
| Inequality | `metadata.namespace != "default"` |
| Comparison | `spec.replicas >= 2` |
| List membership | `metadata.namespace in ["prod", "staging"]` |
| Prefix | `metadata.name.startsWith("api-")` |
| Substring | `metadata.name.contains("api")` |
| Field existence | `has(metadata.annotations["description"])` |
| AND | `expr1 && expr2` |
| OR | `expr1 \|\| expr2` |
| Grouping | `(expr1 \|\| expr2) && expr3` |

### Resource facet queries

Facets provide aggregated counts of unique values for specified fields across
matching resources. When you request a facet on a field like `metadata.namespace`,
the search service returns each unique namespace along with the number of
resources in that namespace. For example, faceting on `metadata.namespace` and
`kind` might return:

```
Namespace                Kind
├── production (89)      ├── Deployment (78)
├── staging (42)         ├── Service (64)
└── development (11)     └── ConfigMap (23)
```

This enables building dynamic filter interfaces that show users what options
exist, how many results each option returns, and which filters would return no
results. Facets are computed against the filtered result set—if a user has
already filtered to `kind = Deployment`, the namespace facet counts will reflect
only Deployments.

The `ResourceFacetQuery` resource retrieves facet counts independently from
search results. This separation allows populating filter UIs before a user has
entered a search query, building browse interfaces that show category breakdowns
without individual results, or fetching facets separately for performance.

```yaml
apiVersion: search.miloapis.com/v1alpha1
kind: ResourceFacetQuery
metadata:
  name: explore-deployments
spec:
  # CEL expressions for scoping facet computation. Only fields marked as
  # filterable can be used. Each expression supports full boolean logic (&&, ||).
  # Multiple filters are combined with OR logic.
  filters:
    - # CEL expression that must evaluate to a boolean.
      expression: 'metadata.namespace == "production"'

  # Scope facet computation to specific resource types. Required when using
  # filters or facets on fields other than built-in metadata. When specified,
  # filter and facet fields must be configured in all applicable index policies.
  resourceTypes:
    - group: apps
      kind: Deployment

  # Fields to compute facets for. Only fields marked as facetable in the
  # applicable index policies can be used.
  facets:
    - # The field path to aggregate on.
      field: metadata.namespace
    - field: metadata.labels["app"]
      # Maximum number of unique values to return (default: 10).
      limit: 20
    - field: kind

status:
  # Array of facet results, one per requested facet field. Order matches the
  # request. Each facet contains value/count pairs sorted by count descending.
  # The response includes only the top N values per field (controlled by limit).
  facets:
    - # The field path that was aggregated.
      field: metadata.namespace
      # Array of unique values and their counts.
      values:
        - # The unique field value.
          value: production
          # Number of matching resources with this value.
          count: 89
        - value: staging
          count: 42
        - value: development
          count: 11
    - field: metadata.labels["app"]
      values:
        - value: nginx
          count: 34
        - value: redis
          count: 28
    - field: kind
      values:
        - value: Deployment
          count: 78
        - value: Service
          count: 64
```

### Future considerations

The following features may be added in future versions:

- **Highlighting**: Return matched terms with highlight markers for display
- **Disjunctive facets**: Option in `ResourceFacetQuery` to compute facet counts
  as if that facet's filter was not applied, enabling multi-select filter UIs
