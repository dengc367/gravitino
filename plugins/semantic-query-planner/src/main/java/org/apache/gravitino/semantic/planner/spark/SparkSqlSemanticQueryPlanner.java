/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.semantic.planner.spark;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.SemanticQueryPlanException;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.Field;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.PlanningContext;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.semantic.SemanticQuery;
import org.apache.gravitino.semantic.SemanticQueryFilter;
import org.apache.gravitino.semantic.SemanticQueryPlan;
import org.apache.gravitino.semantic.SemanticQueryPlanner;

/**
 * A reference {@link SemanticQueryPlanner} that compiles a {@link SemanticQuery} into Spark SQL
 * text.
 *
 * <p>The planner only generates SQL. It never executes it and needs no Spark dependency, because
 * the Gravitino server returns the SQL to the caller instead of running it.
 *
 * <p>The generated statement has a deterministic shape so that callers can diff and test it:
 *
 * <ol>
 *   <li>selected metrics first, in the requested order, then selected dimensions in the requested
 *       order, each one aliased to its Semantic Model name;
 *   <li>one {@code FROM} entry for the dataset that owns the first selection, followed by one
 *       {@code INNER JOIN} per dataset the query also needs;
 *   <li>filters {@code AND}ed into one {@code WHERE} clause in the requested order;
 *   <li>one {@code GROUP BY} clause when any selected expression aggregates, listing every selected
 *       expression that does not aggregate;
 *   <li>an optional {@code LIMIT} taken from the {@value #LIMIT_PROPERTY} planning property.
 * </ol>
 *
 * <p>Datasets are aliased {@code t0}, {@code t1}, ... in the order they join, every identifier is
 * backtick-quoted, and each clause occupies its own line. A relationship carries no cardinality, so
 * every hop of the join tree is an inner join: a row whose joined dataset has no match does not
 * contribute to an aggregation.
 *
 * <p>Expressions are rewritten as follows. {@code <dataset>.<column>} references resolve to the
 * alias of that dataset, a bare or backtick-quoted identifier that names a field declared by the
 * owning dataset resolves to the alias of that dataset, and every other token is copied unchanged.
 * A metric belongs to the dataset its expression qualifies; when its expression qualifies nothing,
 * the planner uses the declared fields it mentions, and needs a qualified expression when that is
 * still ambiguous.
 *
 * <p>Column references that no dataset declares are treated as physical columns of a source table,
 * which is how a filter can compare a column the model does not expose as a field. An unqualified
 * reference resolves against the dataset that owns the first selection; a qualified reference names
 * the dataset before the dot. A dimension or filter field that several datasets declare must be
 * qualified as {@code <dataset>.<field>}.
 */
public final class SparkSqlSemanticQueryPlanner implements SemanticQueryPlanner {

  /** The planner name configured by {@code gravitino.semanticModel.queryPlanner}. */
  public static final String NAME = SparkSqlDialect.NAME;

  /**
   * The {@link PlanningContext} property that limits the number of returned rows.
   *
   * <p>{@link SemanticQuery} models no row limit, so the planner reads an optional non-negative
   * integer from the planning context instead and emits {@code LIMIT} only when it is set.
   */
  public static final String LIMIT_PROPERTY = "limit";

  private static final Map<SemanticQueryFilter.Operator, String> OPERATOR_SYMBOLS =
      operatorSymbols();

  /**
   * Creates a planner; the Gravitino server instantiates it through its no-argument constructor.
   */
  public SparkSqlSemanticQueryPlanner() {}

  @Override
  public String name() {
    return NAME;
  }

  /**
   * Compiles a semantic query into Spark SQL text.
   *
   * <p>The returned plan carries no warnings: a query either compiles completely or is rejected
   * with a {@link SemanticQueryPlanException}.
   *
   * @param model The resolved Semantic Model whose current definition is used.
   * @param query The validated semantic query.
   * @param context The planning context, which may select the row limit.
   * @return The generated plan.
   * @throws SemanticQueryPlanException If the query cannot be compiled for the requested dialect.
   */
  @Override
  public SemanticQueryPlan plan(SemanticModel model, SemanticQuery query, PlanningContext context) {
    Objects.requireNonNull(context, "context must not be null");
    CompiledQuery compiled = compile(model, query, context);
    return SemanticQueryPlan.of(compiled.sql, query.dialect(), NAME, Collections.emptyList());
  }

  /**
   * Returns the identifiers the plan for a query resolves to, in join order.
   *
   * <p>{@link SemanticQueryPlan} carries the SQL, the dialect, the planner name and warnings, but
   * no slot for resolved identifiers. The generated SQL already contains them, because every source
   * appears fully qualified next to its alias; this method exposes the same mapping
   * programmatically for callers that need to attribute the generated SQL to governed tables.
   *
   * @param model The resolved Semantic Model whose current definition is used.
   * @param query The semantic query to resolve.
   * @return An unmodifiable list of {@code <dataset> -> <source> AS <alias>} entries.
   * @throws SemanticQueryPlanException If the query cannot be compiled for the requested dialect.
   */
  public List<String> resolvedIdentifiers(SemanticModel model, SemanticQuery query) {
    return compile(model, query, null).provenance;
  }

  private CompiledQuery compile(
      SemanticModel model, SemanticQuery query, @Nullable PlanningContext context) {
    Objects.requireNonNull(model, "model must not be null");
    Objects.requireNonNull(query, "query must not be null");

    String dialect = query.dialect();
    SparkSqlDialect.checkSupported(dialect);

    SemanticModelDefinition definition = model.definition();
    Map<String, Dataset> datasets = indexDatasets(definition.datasets());
    Map<String, Metric> metrics = indexMetrics(definition.metrics());
    Relationship[] relationships =
        definition.relationships() == null ? new Relationship[0] : definition.relationships();
    Set<String> datasetNames = Collections.unmodifiableSet(new LinkedHashSet<>(datasets.keySet()));

    List<Selection> selections = new ArrayList<>();
    String primaryDataset = null;
    for (String metricName : query.metrics()) {
      Metric metric = metrics.get(metricName);
      if (metric == null) {
        throw new SemanticQueryPlanException(
            "Semantic Model '%s' defines no metric named '%s'", model.name(), metricName);
      }
      String expression = SparkSqlDialect.expressionFor(metric.expression(), dialect);
      String owner = owningDatasetOfMetric(model, metricName, expression, datasets);
      if (primaryDataset == null) {
        primaryDataset = owner;
      }
      selections.add(new Selection(owner, expression, metricName, metricName));
    }
    for (String dimensionName : query.dimensions()) {
      ResolvedColumn column =
          resolveColumn(dimensionName, datasets, "dimension", dialect, primaryDataset);
      if (primaryDataset == null) {
        primaryDataset = column.dataset;
      }
      selections.add(new Selection(column.dataset, column.expression, column.name, column.name));
    }

    List<ResolvedFilter> filters = new ArrayList<>();
    for (SemanticQueryFilter filter : query.filters()) {
      ResolvedColumn column =
          resolveColumn(filter.field(), datasets, "filter field", dialect, primaryDataset);
      filters.add(
          new ResolvedFilter(
              column.dataset,
              column.expression,
              column.name,
              filter.operator(),
              SparkSqlDialect.literal(filter.value(), column.datatype)));
    }

    List<String> required = requiredDatasets(selections, filters, datasetNames);
    if (required.isEmpty()) {
      throw new SemanticQueryPlanException(
          "Semantic query selects no metric or dimension, so there is nothing to plan");
    }

    Map<String, String> aliases = new LinkedHashMap<>();
    List<JoinLink> joins = new ArrayList<>();
    planJoins(model, required, datasetNames, relationships, aliases, joins);

    Function<String, String> aliasLookup =
        datasetName -> {
          String alias = aliases.get(datasetName);
          if (alias == null) {
            throw new SemanticQueryPlanException(
                "Dataset '%s' is not reachable from the planned join tree", datasetName);
          }
          return alias;
        };

    List<String> renderedSelections = new ArrayList<>();
    List<String> groupBy = new ArrayList<>();
    Set<String> distinctGroupBy = new LinkedHashSet<>();
    boolean aggregated = false;
    for (Selection selection : selections) {
      String rendered =
          renderColumn(
              selection.dataset,
              selection.expression,
              selection.column,
              datasets,
              datasetNames,
              aliasLookup);
      renderedSelections.add(rendered);
      boolean aggregate =
          selection.expression != null
              && SparkSqlExpressions.containsAggregate(selection.expression);
      if (aggregate) {
        aggregated = true;
      } else if (distinctGroupBy.add(rendered)) {
        groupBy.add(rendered);
      }
    }

    List<String> predicates = new ArrayList<>();
    for (ResolvedFilter filter : filters) {
      String rendered =
          renderColumn(
              filter.dataset,
              filter.expression,
              filter.column,
              datasets,
              datasetNames,
              aliasLookup);
      predicates.add(rendered + " " + operatorSymbol(filter.operator) + " " + filter.literal);
    }

    List<String> lines = new ArrayList<>();
    lines.add("SELECT");
    for (int index = 0; index < selections.size(); index++) {
      lines.add(
          "  "
              + renderedSelections.get(index)
              + " AS "
              + SparkSqlDialect.quote(selections.get(index).alias)
              + (index + 1 < selections.size() ? "," : ""));
    }
    String base = required.get(0);
    lines.add("FROM");
    lines.add(
        "  "
            + SparkSqlDialect.quote(datasets.get(base).source())
            + " AS "
            + SparkSqlDialect.quote(aliases.get(base)));
    for (JoinLink join : joins) {
      lines.add("INNER JOIN");
      lines.add(
          "  "
              + SparkSqlDialect.quote(datasets.get(join.dataset).source())
              + " AS "
              + SparkSqlDialect.quote(join.alias));
      lines.add("  ON " + join.condition);
    }
    if (!predicates.isEmpty()) {
      lines.add("WHERE");
      lines.add("  " + String.join(" AND ", predicates));
    }
    if (aggregated && !groupBy.isEmpty()) {
      lines.add("GROUP BY");
      for (int index = 0; index < groupBy.size(); index++) {
        lines.add("  " + groupBy.get(index) + (index + 1 < groupBy.size() ? "," : ""));
      }
    }
    String limit = limitValue(context);
    if (limit != null) {
      lines.add("LIMIT " + limit);
    }

    List<String> provenance = new ArrayList<>();
    for (Map.Entry<String, String> entry : aliases.entrySet()) {
      provenance.add(
          entry.getKey()
              + " -> "
              + dottedName(datasets.get(entry.getKey()).source())
              + " AS "
              + entry.getValue());
    }
    return new CompiledQuery(String.join("\n", lines), Collections.unmodifiableList(provenance));
  }

  private static List<String> requiredDatasets(
      List<Selection> selections, List<ResolvedFilter> filters, Set<String> datasetNames) {
    Set<String> required = new LinkedHashSet<>();
    for (Selection selection : selections) {
      required.add(selection.dataset);
    }
    for (ResolvedFilter filter : filters) {
      required.add(filter.dataset);
    }
    for (Selection selection : selections) {
      if (selection.expression != null) {
        required.addAll(SparkSqlExpressions.referencedDatasets(selection.expression, datasetNames));
      }
    }
    for (ResolvedFilter filter : filters) {
      if (filter.expression != null) {
        required.addAll(SparkSqlExpressions.referencedDatasets(filter.expression, datasetNames));
      }
    }
    return Collections.unmodifiableList(new ArrayList<>(required));
  }

  private static void planJoins(
      SemanticModel model,
      List<String> required,
      Set<String> datasetNames,
      Relationship[] relationships,
      Map<String, String> aliases,
      List<JoinLink> joins) {
    String base = required.get(0);
    aliases.put(base, "t0");
    Set<String> joined = new LinkedHashSet<>();
    joined.add(base);
    int aliasCounter = 1;
    for (String target : required) {
      if (aliases.containsKey(target)) {
        continue;
      }
      List<JoinStep> path = findPath(joined, target, datasetNames, relationships);
      if (path == null) {
        throw new SemanticQueryPlanException(
            "No relationship path connects dataset '%s' to dataset '%s' in Semantic Model '%s'",
            base, target, model.name());
      }
      for (JoinStep step : path) {
        if (aliases.containsKey(step.toDataset)) {
          continue;
        }
        String alias = "t" + aliasCounter++;
        aliases.put(step.toDataset, alias);
        joined.add(step.toDataset);
        joins.add(
            new JoinLink(
                step.toDataset,
                alias,
                aliases.get(step.fromDataset),
                step.fromColumns,
                step.toColumns));
      }
    }
  }

  @Nullable
  private static List<JoinStep> findPath(
      Set<String> sources, String target, Set<String> datasetNames, Relationship[] relationships) {
    if (sources.contains(target)) {
      return new ArrayList<>();
    }

    Map<String, JoinStep> parents = new LinkedHashMap<>();
    Set<String> visited = new LinkedHashSet<>(sources);
    Deque<String> queue = new ArrayDeque<>(sources);
    boolean found = false;
    while (!found && !queue.isEmpty()) {
      String node = queue.poll();
      for (Relationship relationship : relationships) {
        JoinStep step;
        if (relationship.from().equals(node)) {
          step =
              new JoinStep(
                  node, relationship.to(), relationship.fromColumns(), relationship.toColumns());
        } else if (relationship.to().equals(node)) {
          step =
              new JoinStep(
                  node, relationship.from(), relationship.toColumns(), relationship.fromColumns());
        } else {
          continue;
        }
        if (!datasetNames.contains(step.toDataset) || !visited.add(step.toDataset)) {
          continue;
        }
        parents.put(step.toDataset, step);
        if (step.toDataset.equals(target)) {
          found = true;
          break;
        }
        queue.add(step.toDataset);
      }
    }
    if (!found) {
      return null;
    }

    List<JoinStep> path = new ArrayList<>();
    String cursor = target;
    while (!sources.contains(cursor)) {
      JoinStep step = parents.get(cursor);
      if (step == null) {
        return null;
      }
      path.add(0, step);
      cursor = step.fromDataset;
    }
    return path;
  }

  private static String owningDatasetOfMetric(
      SemanticModel model, String metricName, String expression, Map<String, Dataset> datasets) {
    List<String> referenced = SparkSqlExpressions.referencedDatasets(expression, datasets.keySet());
    if (referenced.size() == 1) {
      return referenced.get(0);
    }
    if (referenced.size() > 1) {
      throw new SemanticQueryPlanException(
          "Metric '%s' of Semantic Model '%s' references more than one dataset (%s), but a metric"
              + " aggregates over a single dataset",
          metricName, model.name(), String.join(", ", referenced));
    }

    Set<String> identifiers = SparkSqlExpressions.identifiers(expression);
    List<String> candidates = new ArrayList<>();
    for (Dataset dataset : datasets.values()) {
      for (String fieldName : declaredFieldNames(dataset)) {
        if (identifiers.contains(fieldName)) {
          candidates.add(dataset.name());
          break;
        }
      }
    }
    if (candidates.size() == 1) {
      return candidates.get(0);
    }
    if (candidates.size() > 1) {
      throw new SemanticQueryPlanException(
          "Cannot determine the dataset of metric '%s' of Semantic Model '%s': datasets %s declare"
              + " the fields its expression mentions. Qualify the expression with a dataset name,"
              + " for example 'orders.amount'",
          metricName, model.name(), String.join(", ", candidates));
    }
    if (datasets.size() == 1) {
      return datasets.keySet().iterator().next();
    }
    throw new SemanticQueryPlanException(
        "Cannot determine the dataset of metric '%s' of Semantic Model '%s'. Qualify the expression"
            + " with a dataset name, for example 'orders.amount'",
        metricName, model.name());
  }

  private static ResolvedColumn resolveColumn(
      String name,
      Map<String, Dataset> datasets,
      String role,
      String dialect,
      @Nullable String primaryDataset) {
    int separator = name.indexOf('.');
    if (separator > 0 && separator < name.length() - 1) {
      String datasetName = name.substring(0, separator);
      String columnName = name.substring(separator + 1);
      Dataset dataset = datasets.get(datasetName);
      if (dataset == null) {
        throw new SemanticQueryPlanException(
            "Unknown %s '%s': the Semantic Model defines no dataset named '%s'",
            role, name, datasetName);
      }
      Field field = findField(dataset, columnName);
      if (field != null) {
        return new ResolvedColumn(
            datasetName,
            SparkSqlDialect.expressionFor(field.expression(), dialect),
            field.name(),
            field.datatype());
      }
      return new ResolvedColumn(datasetName, null, columnName, null);
    }

    List<Dataset> declaringDatasets = new ArrayList<>();
    Field declared = null;
    for (Dataset dataset : datasets.values()) {
      Field field = findField(dataset, name);
      if (field != null) {
        declaringDatasets.add(dataset);
        declared = field;
      }
    }
    if (declaringDatasets.size() > 1) {
      List<String> names = new ArrayList<>();
      for (Dataset dataset : declaringDatasets) {
        names.add(dataset.name());
      }
      throw new SemanticQueryPlanException(
          "Ambiguous %s '%s': datasets %s all declare it. Qualify it as '<dataset>.<field>'",
          role, name, String.join(", ", names));
    }
    if (declared != null) {
      Dataset dataset = declaringDatasets.get(0);
      return new ResolvedColumn(
          dataset.name(),
          SparkSqlDialect.expressionFor(declared.expression(), dialect),
          declared.name(),
          declared.datatype());
    }
    if (datasets.size() == 1) {
      String datasetName = datasets.keySet().iterator().next();
      return new ResolvedColumn(datasetName, null, name, null);
    }
    if (primaryDataset != null) {
      return new ResolvedColumn(primaryDataset, null, name, null);
    }
    throw new SemanticQueryPlanException(
        "Unknown %s '%s': no dataset of the Semantic Model declares that field. Qualify it as"
            + " '<dataset>.<column>' to name a column of a specific dataset",
        role, name);
  }

  @Nullable
  private static Field findField(Dataset dataset, String name) {
    Field[] fields = dataset.fields();
    if (fields == null) {
      return null;
    }
    for (Field field : fields) {
      if (field.name().equals(name)) {
        return field;
      }
    }
    return null;
  }

  private static Set<String> declaredFieldNames(Dataset dataset) {
    Set<String> names = new LinkedHashSet<>();
    Field[] fields = dataset.fields();
    if (fields != null) {
      for (Field field : fields) {
        names.add(field.name());
      }
    }
    return Collections.unmodifiableSet(names);
  }

  private static Map<String, Dataset> indexDatasets(Dataset[] datasets) {
    Map<String, Dataset> index = new LinkedHashMap<>();
    for (Dataset dataset : datasets) {
      if (index.put(dataset.name(), dataset) != null) {
        throw new SemanticQueryPlanException(
            "Semantic Model defines dataset '%s' more than once", dataset.name());
      }
    }
    return index;
  }

  private static Map<String, Metric> indexMetrics(@Nullable Metric[] metrics) {
    Map<String, Metric> index = new LinkedHashMap<>();
    if (metrics == null) {
      return index;
    }
    for (Metric metric : metrics) {
      if (index.put(metric.name(), metric) != null) {
        throw new SemanticQueryPlanException(
            "Semantic Model defines metric '%s' more than once", metric.name());
      }
    }
    return index;
  }

  @Nullable
  private static String limitValue(@Nullable PlanningContext context) {
    if (context == null) {
      return null;
    }
    String raw = context.properties().get(LIMIT_PROPERTY);
    if (raw == null) {
      return null;
    }
    String trimmed = raw.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    int limit;
    try {
      limit = Integer.parseInt(trimmed);
    } catch (NumberFormatException e) {
      throw new SemanticQueryPlanException(
          e,
          "Planning property '%s' must be a non-negative integer, but was '%s'",
          LIMIT_PROPERTY,
          raw);
    }
    if (limit < 0) {
      throw new SemanticQueryPlanException(
          "Planning property '%s' must be a non-negative integer, but was '%s'",
          LIMIT_PROPERTY, raw);
    }
    return Integer.toString(limit);
  }

  private static String renderColumn(
      String dataset,
      @Nullable String expression,
      String column,
      Map<String, Dataset> datasets,
      Set<String> datasetNames,
      Function<String, String> aliasLookup) {
    String alias = aliasLookup.apply(dataset);
    if (expression == null) {
      return SparkSqlDialect.quote(alias) + "." + SparkSqlDialect.quote(column);
    }
    return SparkSqlExpressions.rewrite(
        expression, declaredFieldNames(datasets.get(dataset)), alias, datasetNames, aliasLookup);
  }

  private static String operatorSymbol(SemanticQueryFilter.Operator operator) {
    String symbol = OPERATOR_SYMBOLS.get(operator);
    if (symbol == null) {
      throw new SemanticQueryPlanException("Unsupported filter operator '%s'", operator);
    }
    return symbol;
  }

  private static String dottedName(NameIdentifier identifier) {
    StringBuilder dotted = new StringBuilder();
    for (String level : identifier.namespace().levels()) {
      dotted.append(level).append('.');
    }
    return dotted.append(identifier.name()).toString();
  }

  private static Map<SemanticQueryFilter.Operator, String> operatorSymbols() {
    Map<SemanticQueryFilter.Operator, String> symbols =
        new EnumMap<>(SemanticQueryFilter.Operator.class);
    symbols.put(SemanticQueryFilter.Operator.EQ, "=");
    symbols.put(SemanticQueryFilter.Operator.NE, "<>");
    symbols.put(SemanticQueryFilter.Operator.GT, ">");
    symbols.put(SemanticQueryFilter.Operator.GE, ">=");
    symbols.put(SemanticQueryFilter.Operator.LT, "<");
    symbols.put(SemanticQueryFilter.Operator.LE, "<=");
    return Collections.unmodifiableMap(symbols);
  }

  /** A selected metric or dimension together with the dataset that owns it. */
  private static final class Selection {

    private final String dataset;
    private final String column;
    private final String alias;
    @Nullable private final String expression;

    private Selection(String dataset, @Nullable String expression, String column, String alias) {
      this.dataset = dataset;
      this.expression = expression;
      this.column = column;
      this.alias = alias;
    }
  }

  /**
   * A column a query or filter names, resolved to the dataset that provides it.
   *
   * <p>{@code expression} is the declared field expression to rewrite, or {@code null} when the
   * column is a physical column of the dataset's source table.
   */
  private static final class ResolvedColumn {

    private final String dataset;
    private final String name;
    @Nullable private final DataType datatype;
    @Nullable private final String expression;

    private ResolvedColumn(
        String dataset, @Nullable String expression, String name, @Nullable DataType datatype) {
      this.dataset = dataset;
      this.expression = expression;
      this.name = name;
      this.datatype = datatype;
    }
  }

  /** A filter whose field already resolved to a dataset. */
  private static final class ResolvedFilter {

    private final String dataset;
    private final String column;
    private final SemanticQueryFilter.Operator operator;
    private final String literal;
    @Nullable private final String expression;

    private ResolvedFilter(
        String dataset,
        @Nullable String expression,
        String column,
        SemanticQueryFilter.Operator operator,
        String literal) {
      this.dataset = dataset;
      this.expression = expression;
      this.column = column;
      this.operator = operator;
      this.literal = literal;
    }
  }

  /** One join of the join tree, with its rendered condition. */
  private static final class JoinLink {

    private final String dataset;
    private final String alias;
    private final String condition;

    private JoinLink(
        String dataset, String alias, String fromAlias, String[] fromColumns, String[] toColumns) {
      this.dataset = dataset;
      this.alias = alias;
      StringBuilder rendered = new StringBuilder();
      for (int index = 0; index < fromColumns.length; index++) {
        if (index > 0) {
          rendered.append(" AND ");
        }
        rendered
            .append(SparkSqlDialect.quote(fromAlias))
            .append('.')
            .append(SparkSqlDialect.quote(fromColumns[index]))
            .append(" = ")
            .append(SparkSqlDialect.quote(alias))
            .append('.')
            .append(SparkSqlDialect.quote(toColumns[index]));
      }
      this.condition = rendered.toString();
    }
  }

  /** One directed hop of a join path. */
  private static final class JoinStep {

    private final String fromDataset;
    private final String toDataset;
    private final String[] fromColumns;
    private final String[] toColumns;

    private JoinStep(
        String fromDataset, String toDataset, String[] fromColumns, String[] toColumns) {
      this.fromDataset = fromDataset;
      this.toDataset = toDataset;
      this.fromColumns = fromColumns;
      this.toColumns = toColumns;
    }
  }

  /** The SQL and provenance a compiled query produces. */
  private static final class CompiledQuery {

    private final String sql;
    private final List<String> provenance;

    private CompiledQuery(String sql, List<String> provenance) {
      this.sql = sql;
      this.provenance = provenance;
    }
  }
}
