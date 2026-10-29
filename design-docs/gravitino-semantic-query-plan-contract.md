<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Design of the Semantic Query Plan Contract

Status: draft for review. This document describes a contract, not an implementation.

## Background

A Semantic Model is a governed definition: datasets, relationships, dimensions and metrics. It
cannot answer a question about itself, so every consumer rebuilds the same pipeline — resolve
metrics and dimensions, resolve join paths, render engine-specific SQL. Two consumers can return
different numbers for the same question.

The current prototype closes that gap by compiling the query into Spark SQL text on the server. That
makes Gravitino own SQL assembly, identifier quoting, literal rendering and a dialect matrix, which
contradicts the non-goal in [the Semantic Model design](./gravitino-semantic-model-design.md)
("Query compilation and execution ... are separate work"). It also forces the server to choose the
execution engine for the caller, so a consumer that federates across catalogs cannot use the result
as-is.

This design keeps the seam but moves it: **Gravitino resolves a semantic query into a
dialect-neutral logical plan; the engine that will execute the query materializes that plan.**

## Goals

1. **Dialect neutrality.** Neither the request nor the response carries a dialect or SQL text.
   Gravitino never assembles a statement, never quotes an identifier, never renders a literal.
2. **Resolve once.** Everything that needs model knowledge is decided once, on the server: dataset
   ownership, join graph, ambiguity detection, aggregate classification, grouping set, filters.
3. **A stable, executable wire contract.** The plan must be sufficient for a third-party engine to
   generate its own plan without reading the Semantic Model definition.
4. **Materialization as an SPI.** Engines own translation; Gravitino ships at most a reference
   converter that is not part of the server runtime.

## Non-Goals

1. **SQL generation or execution on the server.** No statement text in any response.
2. **Parsing Semantic Model expressions.** Model expressions stay opaque per dialect; see
   [The expression boundary](#the-expression-boundary).
3. **Fan-out and cardinality rules.** `Relationship` carries no cardinality today, so the plan
   exposes the join graph but does not claim fan-out safety.
4. **Optimization.** No projection pushdown, predicate pushdown, join reordering or cost model.
   The consuming engine does that on its own logical plan.
5. **Version pinning.** The current definition is planned; see the open questions.
6. **A recursive plan tree.** Single-block metric queries do not need one; see
   [Alternatives considered](#alternatives-considered).

## The expression boundary

This is the constraint that decides the shape of the contract. In the Ossie-compatible model a
metric or field expression is **a piece of SQL text per dialect**:

```java
Expression -> DialectExpression[]   // e.g. {ANSI_SQL: "SUM(orders.amount)", DATABRICKS: "..."}
```

Therefore Gravitino cannot emit a fully structured expression AST without also owning a SQL parser
per dialect — a larger liability than the SQL generator this design removes. The contract is
therefore:

- **the relational layer is fully neutral** (sources, joins, selections, filters, grouping, limit),
  and
- **the expression layer is neutral about *which* dialect is chosen**: every expression travels as
  the model's complete dialect map, and the converter picks the representation it understands and
  rewrites dataset references to physical sources.

That is still the whole win. Gravitino no longer decides dialects, aliases, quoting, literal
syntax, aggregate-function classification or statement shape; it only says *what* is computed and
*over which datasets*.

## Where the boundary is

| Concern | Gravitino (resolver) | Converter (engine) |
| --- | --- | --- |
| Request validation, authorization, model loading | yes | no |
| Metric / dimension / field resolution | yes | no |
| Ambiguity and missing-reference errors | yes | no |
| Join graph and join order | yes | no |
| Column-pair join conditions | yes (structure only) | renders them |
| Aggregate vs. group-by classification | yes (by role) | no |
| Grouping set | yes | no |
| Filter conditions | yes (field, operator, value) | renders literals |
| Row limit | yes | no |
| Expression dialect selection | no | yes |
| Dataset-to-physical-alias mapping | no (sources only) | yes |
| Identifier quoting, literal syntax, SQL shape | no | yes |
| Pushdown, join reordering, CBO | no | yes |
| Execution and result handling | no | yes |

## Contract

### 1. Request: `SemanticQuery`

The request is purely semantic. It names metrics, dimensions and filters; it does not name tables,
engines or dialects.

```java
@Evolving
public final class SemanticQuery {

  /** The selected metric names, in output order. */
  public List<String> metrics();

  /** The selected dimension field names, in output order. */
  public List<String> dimensions();

  /** The filters, ANDed together in request order. */
  public List<SemanticQueryFilter> filters();

  /** The maximum number of rows to return, or {@code null} for no limit. */
  @Nullable
  public Long limit();

  public static Builder builder();
}
```

`SemanticQueryFilter` keeps its current shape: `field`, `operator` (`EQ`, `NE`, `GT`, `GE`, `LT`,
`LE`) and `value` as text. The value is deliberately **not** typed or quoted: the converter renders
the literal according to the field's `DataType` and its own dialect.

Two changes against the prototype:

- `dialect` is removed. A caller that wants SQL asks a converter, not the server.
- `limit` moves from `PlanningContext` into the query. A row limit is semantics, not a planner
  property; `PlanningContext` then only identifies the model and disappears from the public
  contract.

### 2. Response: `SemanticQueryPlan`

The plan is a single relational block, not a tree of plan nodes.

```java
@Evolving
public final class SemanticQueryPlan {

  /** The complete identifier of the Semantic Model the plan was resolved against. */
  public NameIdentifier semanticModel();

  /** The Semantic Model version the plan was resolved against, or {@code null} if unknown. */
  @Nullable
  public String semanticModelVersion();

  /** The datasets the query needs, in join order. */
  public List<PlanSource> sources();

  /** The joins that connect the sources; each one joins to a dataset already in scope. */
  public List<PlanJoin> joins();

  /** The selected metrics then dimensions, in request order. */
  public List<PlanSelection> selections();

  /** The filters, ANDed together in request order. */
  public List<PlanFilter> filters();

  /** The selection names that form the grouping set; equals the non-aggregate selections. */
  public List<String> groupBy();

  /** The maximum number of rows to return, or {@code null} for no limit. */
  @Nullable
  public Long limit();

  /** Non-fatal warnings raised while resolving; empty when there are none. */
  public List<String> warnings();
}
```

Supporting values:

```java
/** A dataset the query needs, and the physical source that provides it. */
public final class PlanSource {
  /** The dataset name declared by the Semantic Model. */
  public String dataset();

  /** The physical source: catalog, schema and table (or fileset/topic) identifier. */
  public NameIdentifier source();
}

/** A join between two datasets of the plan. */
public final class PlanJoin {
  public enum Type { INNER }

  public Type type();

  /** The Semantic Model relationship this join came from, or {@code null} if it is implied. */
  @Nullable
  public String relationship();

  /** The dataset already in scope; its columns are the left side of the condition. */
  public String fromDataset();

  /** The physical columns of {@code fromDataset}. */
  public String[] fromColumns();

  /** The dataset this join brings into scope. */
  public String toDataset();

  /** The physical columns of {@code toDataset}, pairwise with {@code fromColumns}. */
  public String[] toColumns();
}

/** A reference to a field of a dataset, either a declared field or a physical column. */
public final class PlanFieldRef {
  /** The dataset that provides the field. */
  public String dataset();

  /** The declared field expression per dialect, or {@code null} for a physical column. */
  @Nullable
  public Expression expression();

  /** The physical column name, or {@code null} for a declared field. */
  @Nullable
  public String column();

  /** The logical data type, or {@code null} if the model declares none. */
  @Nullable
  public DataType datatype();
}

/** A selected metric or dimension. */
public final class PlanSelection {
  public enum Role { METRIC, DIMENSION }

  /** The metric or field name as requested; unique within the plan. */
  public String name();

  public Role role();

  /** Whether this selection aggregates; {@code true} for metrics, {@code false} for dimensions. */
  public boolean aggregate();

  public PlanFieldRef field();
}

/** A filter on a field. */
public final class PlanFilter {
  public PlanFieldRef field();

  public SemanticQueryFilter.Operator operator();

  /** The compared value as untyped text; the converter renders it for its dialect. */
  public String value();
}
```

Invariants, checked by the server and safe for converters to rely on:

1. `selections` is non-empty and `sources` contains every `PlanFieldRef.dataset`.
2. Exactly one of `PlanFieldRef.expression` and `PlanFieldRef.column` is non-null.
3. `PlanJoin.fromDataset` appears in `sources` before `toDataset`, so the plan is executable in
   order without reordering.
4. `fromColumns.length == toColumns.length` and the pairing is positional.
5. `groupBy` equals the names of the selections whose `aggregate` is false, in `selections` order;
   it is redundant with the flags and stated explicitly so that no consumer has to re-derive it.
6. Selection names are unique across metrics and dimensions; the server rejects duplicates rather
   than emitting an ambiguous plan.
7. `limit`, when present, is non-negative.

### 3. Materialization SPI: `SemanticQueryPlanConverter`

The converter is the only place where an engine dialect appears. It is intentionally generic: a
Trino converter produces a Trino `LogicalPlan` object, a Spark converter produces SQL text, an
Ossie-side consumer produces Ossie semantic nodes. Nothing forces a `String`. The SPI lives in the
`api` module so that both the server and engine adapters can depend on it; implementations live
outside the server runtime.

```java
@Evolving
public interface SemanticQueryPlanConverter<T> {

  /** The engine or dialect family this converter targets, for example {@code spark-sql}. */
  String engine();

  /**
   * The Semantic Model expression dialects this converter can read. The plan carries expressions
   * as a dialect map; the converter must find at least one entry it understands for every field.
   */
  Set<String> expressionDialects();

  /**
   * Translates a plan into the engine's own plan representation.
   *
   * @throws UnsupportedOperationException If the plan cannot be represented by this engine.
   */
  T materialize(SemanticQueryPlan plan, ConversionContext context);
}
```

`ConversionContext` carries engine-side configuration that the plan must not know about, such as a
Gravitino-catalog-to-engine-catalog mapping for federated sources:

```java
@Evolving
public final class ConversionContext {
  public static ConversionContext of(Map<String, String> properties);
  public Map<String, String> properties();
}
```

For text-producing converters there is a convenience subtype, which is also what a future optional
REST materialization would build on:

```java
/** A converter whose engine consumes statements as text. */
public interface SqlSemanticQueryPlanConverter extends SemanticQueryPlanConverter<String> {

  /** The SQL dialect identifier of the produced text. */
  String sqlDialect();
}
```

Fallback rule for expressions: the converter prefers the representation for its own dialect, then
`ANSI_SQL`, and fails when the model provides neither. This is a converter policy, not a contract
rule; a converter may be stricter.

### 4. Server-side removal

The following parts of the prototype do not survive this contract:

| Prototype | Decision |
| --- | --- |
| `SemanticQueryPlan.sql` / `dialect()` | removed |
| `SemanticQuery.dialect()` | removed |
| `SemanticQueryPlanner` SPI, `SemanticQueryPlannerFactory`, `gravitino.semanticModel.queryPlanner`, planner-class config | removed; resolution is Gravitino's own job and is not pluggable |
| `none` planner and the `501` response | removed; the endpoint always resolves |
| `plan.planner()` in the response | replaced by `semanticModel` / `semanticModelVersion` provenance |
| `plugins:semantic-query-planner` Spark planner | becomes a reference `SemanticQueryPlanConverter` in a library module, not wired into the server |
| `PlanningContext.limit` | moved into `SemanticQuery.limit` |

The REST path keeps its shape but changes meaning: `POST .../semantic-models/{semanticModel}/plan`
returns a plan, not SQL.

## Wire format

### Request

```json
{
  "metrics": ["Gross_Revenue"],
  "dimensions": ["Region_Name", "Calendar_Month"],
  "filters": [
    {"field": "Calendar_Month", "operator": "EQ", "value": "2026-10"}
  ],
  "limit": 1000
}
```

### Response

Resolved against a model with datasets `orders` (source `catalogA.dbB.order_table`) and `regions`
(source `catalogA.dbB.region_table`), and a relationship `orders_to_regions` on
`orders.region_id = regions.id`:

```json
{
  "code": 0,
  "plan": {
    "semanticModel": ["metalake", "catalogA", "sales", "sales_model"],
    "semanticModelVersion": "3",
    "sources": [
      {"dataset": "orders", "source": ["catalogA", "dbB", "order_table"]},
      {"dataset": "regions", "source": ["catalogA", "dbB", "region_table"]}
    ],
    "joins": [
      {
        "type": "INNER",
        "relationship": "orders_to_regions",
        "fromDataset": "orders",
        "fromColumns": ["region_id"],
        "toDataset": "regions",
        "toColumns": ["id"]
      }
    ],
    "selections": [
      {
        "name": "Gross_Revenue",
        "role": "METRIC",
        "aggregate": true,
        "field": {
          "dataset": "orders",
          "expression": {
            "ANSI_SQL": "SUM(orders.amount)",
            "DATABRICKS": "SUM(orders.amount)"
          },
          "datatype": "DECIMAL"
        }
      },
      {
        "name": "Region_Name",
        "role": "DIMENSION",
        "aggregate": false,
        "field": {
          "dataset": "regions",
          "expression": {"ANSI_SQL": "regions.name"},
          "datatype": "STRING"
        }
      },
      {
        "name": "Calendar_Month",
        "role": "DIMENSION",
        "aggregate": false,
        "field": {
          "dataset": "orders",
          "expression": {"ANSI_SQL": "orders.month"},
          "datatype": "STRING"
        }
      }
    ],
    "filters": [
      {
        "field": {
          "dataset": "orders",
          "expression": {"ANSI_SQL": "orders.month"},
          "datatype": "STRING"
        },
        "operator": "EQ",
        "value": "2026-10"
      }
    ],
    "groupBy": ["Region_Name", "Calendar_Month"],
    "limit": 1000,
    "warnings": []
  }
}
```

Note what is absent: no `sql`, no `dialect`, no aliases (`t0`), no quoting, no `WHERE` text, no
literal syntax. A Spark converter rewrites `orders.amount` to its own alias, quotes identifiers and
renders `'2026-10'`; a Trino converter builds a `LogicalPlan` instead.

Wire encoding notes:

- `NameIdentifier` is an array of namespace levels plus the name, as everywhere else in Gravitino.
- `Expression` is an object from dialect identifier to expression text; it is exactly the model's
  dialect map, not a parsed tree.
- `Role`, `Type`, `Operator` and `DataType` are the uppercase enum names.
- Nullable fields are omitted when absent; unknown enumeration values are rejected by DTO
  validation rather than ignored.

## Resolution rules the server guarantees

These rules are part of the contract because consumers and tests depend on them being deterministic.

1. **Metric lookup.** Every requested metric must exist in the model's current definition;
   otherwise `422` naming the metric and the model.
2. **Dataset ownership of a metric.** A metric belongs to the single dataset its expression
   qualifies (`orders.amount`). If it qualifies none, the dataset whose declared fields the
   expression mentions decides it; if that is still ambiguous, or the metric references more than
   one dataset, the query is rejected with `422` and the message asks for a qualified expression.
3. **Dimension and filter field lookup.** `<dataset>.<field>` names a field of that dataset; a bare
   name resolves when exactly one dataset declares it. A name that no dataset declares is treated as
   a physical column of the dataset that owns the first selection, which is how a filter can target
   a partition column that the model does not expose.
4. **Ambiguity.** A bare name declared by several datasets, or a physical column with no owning
   dataset, is rejected with `422` and a message naming the candidates.
5. **Join graph.** Starting from the dataset of the first selection, the server adds datasets in
   request order, each time by a shortest path over the model's relationships. Every hop is an
   `INNER` join, because a relationship carries no cardinality. If a required dataset is
   unreachable, the query is rejected with `422`.
6. **Aggregation.** A metric aggregates; a dimension does not. The server never inspects expression
   text for aggregate function names.
7. **Grouping.** `groupBy` is the ordered list of non-aggregate selections. A query with no metric
   has no aggregation; it returns detail rows, one per matching row, and this is documented rather
   than silently changed to `DISTINCT`.
8. **Filters.** Filters are `AND`ed in request order and apply to the joined relation. `null`
   comparisons and `IN` lists are out of scope for this contract.
9. **Limit.** A negative limit is a `400`; the plan echoes a non-negative limit unchanged.
10. **Determinism.** The same request against the same model definition produces the same plan,
    including list order and join order.

## Errors

| Status | Condition |
| --- | --- |
| `400` | Malformed request: blank names, unknown operator, negative limit, no metric or dimension |
| `403` | Caller lacks the Semantic Model load privilege (`OWNER`, or `USE_SEMANTIC_MODEL` / `MODIFY_SEMANTIC_MODEL` with `USE_CATALOG` and `USE_SCHEMA`) |
| `404` | Metalake, catalog, schema or Semantic Model does not exist |
| `422` | The model exists but cannot satisfy the query: unknown metric, ambiguous field, unjoinable dataset |
| `5XX` | Server error |

`501` disappears together with the pluggable planner: the endpoint is always available.

## Reference materialization (informative)

A SQL converter is expected to be small, because the plan already contains every decision:

1. assign one alias per `PlanSource`, in `sources` order;
2. for each selection and filter, pick the expression text for the converter's dialect and rewrite
   `<dataset>.<column>` references to `<alias>."<column>"`; use `PlanFieldRef.column` verbatim when
   the field is a physical column;
3. render filter literals from `PlanFieldRef.datatype` and the converter's dialect;
4. assemble `SELECT <selections> FROM <first source> JOIN ... WHERE ... GROUP BY ... LIMIT ...`.

Selections keep their plan order and each is aliased to its `PlanSelection.name`, so the result
schema is stable for callers. For the plan in [Wire format](#wire-format), a Spark converter would
produce:

```sql
SELECT SUM(`t0`.`amount`) AS `Gross_Revenue`,
       `t1`.`name` AS `Region_Name`,
       `t0`.`month` AS `Calendar_Month`
FROM `catalogA`.`dbB`.`order_table` AS `t0`
INNER JOIN `catalogA`.`dbB`.`region_table` AS `t1`
  ON `t0`.`region_id` = `t1`.`id`
WHERE `t0`.`month` = '2026-10'
GROUP BY `t1`.`name`, `t0`.`month`
LIMIT 1000
```

Steps 2 to 4 are the only dialect-specific code in the system, and they live in the engine adapter,
not in the Gravitino server. The prototype's Spark renderer (`SparkSqlDialect`,
`SparkSqlExpressions` and the rendering half of `SparkSqlSemanticQueryPlanner`) is the reference
implementation of this section.

## Security and governance

- The plan exposes physical source identifiers. This is not a new disclosure: the Semantic Model
  definition already carries `Dataset.source`, and the endpoint requires read access to the model.
- **Plan authorization is not execution authorization.** The converter and the engine execute
  outside Gravitino; they are responsible for their own credentials and row/column policies.
  The documentation and the endpoint description must say so.
- The plan carries the model identifier and, when available, its version, so an audit trail can
  attribute a produced statement to the definition it came from.

## Alternatives considered

**A. Server emits dialect SQL (the prototype).** Rejected: it puts SQL assembly, quoting, literal
rendering and the dialect matrix inside Gravitino, contradicts the Semantic Model non-goal, forces
one engine onto every caller, and makes any consumer that needs another dialect impossible without
a server-side plugin.

**B. A recursive `PlanNode` AST.** Rejected for now: metric queries are a single select block, so a
tree adds nesting to the wire format, complicates validation and versioning, and buys nothing. The
flat block can gain an optional nested field later without breaking consumers.

**C. Structured expression AST.** Rejected: the model stores expressions as per-dialect SQL text, so
structuring them requires a SQL parser per dialect — a larger liability than the renderer being
removed. Carrying the dialect map defers the choice to the party that knows its dialect.

**D. Plan plus optional server-side materialization.** Not in this contract. It is compatible with
it: a deployment that wants SQL can run a converter behind an optional flag, but the canonical
response stays dialect-neutral. Deferred so that the contract is settled first.

## Compatibility and evolution

- The plan is additive by design: new optional fields (for example `distinct`, `orderBy`, fan-out
  annotations) do not break existing converters, which ignore unknown fields.
- Enumerations (`role`, `type`, `operator`) are validated strictly; adding a value is a contract
  change and must be announced in the OpenAPI document.
- The converter SPI is `@Evolving`; `engine()` and `materialize()` are the stable surface.
- Proposed versioning: the plan carries `semanticModelVersion`; a future `.../versions/{version}/plan`
  path can pin it without changing the body.

## Mapping to the prototype

| Prototype area | Change |
| --- | --- |
| `api/.../semantic/SemanticQuery{Plan,Planner,Filter}.java` | reshape as above; delete `PlanningContext` from the public contract |
| `api/.../exceptions/SemanticQueryPlanException.java` | keep, still maps to `422` |
| `core/.../SemanticQueryPlannerFactory.java`, `Configs.SEMANTIC_MODEL_QUERY_PLANNER` | delete |
| `server/.../rest/SemanticModelOperations.java` | same path; resolve instead of compile |
| `common/.../dto/requests/PlanSemanticQueryRequest.java`, `responses/SemanticQueryPlanResponse.java` | re-shape; add plan sub-DTOs |
| `docs/open-api/semantic-models.yaml` | new schema, drop `501`, drop `dialect` |
| client Java / Python / MCP | mechanical: `planSemanticQuery` returns the plan |
| `plugins/semantic-query-planner` | becomes `plugins/semantic-query-converter` (reference Spark converter, library only) |

## Open questions for reviewers

1. **Version pinning.** Should the plan carry `semanticModelVersion` now (and fail if the model
   changes between plan and execute), or stay "current definition" until the API exposes versions?
2. **Physical sources in the response.** Expose `Dataset.source` (as drafted), or expose dataset
   names only and let the engine resolve them through its Gravitino connector? The latter is
   cleaner for governance but makes the plan unusable without a Gravitino connection.
3. **`limit` type.** `Long` (matches engine row limits) versus `int` (matches most APIs)?
4. **Filter operators.** Ship `EQ/NE/GT/GE/LT/LE` only, or add `IN`, `NOT_IN`, `IS_NULL`,
   `LIKE` in the first cut? The model has no nullability metadata today.
5. **Detail queries.** A query with dimensions and no metric currently returns detail rows. Should
   it instead be rejected, or grouped?
6. **Naming.** `SemanticQueryPlan` (chosen) versus `SemanticExecutePlan` or
   `SemanticQueryLogicalPlan`; and `plan` as the endpoint suffix.
7. **Converter location.** SPI in `api` (drafted) versus `clients/client-java`, given that engine
   adapters usually depend on a client library rather than the server API.
8. **Response envelope.** Gravitino responses are flat (`code` plus fields). This draft nests the
   plan under `plan` so a future materialized form can sit beside it. Keep the nesting, or flatten
   the plan fields to the top level for consistency with the existing response DTOs?