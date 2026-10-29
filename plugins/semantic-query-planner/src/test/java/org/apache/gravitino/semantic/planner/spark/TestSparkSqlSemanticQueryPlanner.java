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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.SemanticQueryPlanException;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dialects;
import org.apache.gravitino.semantic.Expression;
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
import org.junit.jupiter.api.Test;

/** Tests the SQL text {@link SparkSqlSemanticQueryPlanner} generates. */
public class TestSparkSqlSemanticQueryPlanner {

  private static final NameIdentifier MODEL_IDENTIFIER =
      NameIdentifier.of("metalake", "catalog", "schema", "sales_model");

  private static final PlanningContext CONTEXT = PlanningContext.of(MODEL_IDENTIFIER);

  private static final String SPARK_SQL = "spark-sql";

  @Test
  public void testPlannerName() {
    assertEquals(SPARK_SQL, new SparkSqlSemanticQueryPlanner().name());
    assertEquals("spark-sql", SparkSqlSemanticQueryPlanner.NAME);
  }

  @Test
  public void testPlannerLoadsThroughTheConfiguredClassName() throws Exception {
    // The server loads a planner by class name and instantiates it through its no-arg constructor;
    // there is no ServiceLoader hook for planners.
    Class<?> plannerClass =
        Class.forName("org.apache.gravitino.semantic.planner.spark.SparkSqlSemanticQueryPlanner");

    assertTrue(SemanticQueryPlanner.class.isAssignableFrom(plannerClass));
    SemanticQueryPlanner planner =
        (SemanticQueryPlanner) plannerClass.getDeclaredConstructor().newInstance();
    assertEquals(SPARK_SQL, planner.name());
  }

  @Test
  public void testSingleDatasetMetricAndDimension() {
    SemanticQuery query =
        SemanticQuery.builder()
            .withMetrics(Arrays.asList("total_revenue"))
            .withDimensions(Arrays.asList("region"))
            .withFilters(Arrays.asList())
            .withDialect(SPARK_SQL)
            .build();

    SemanticQueryPlan plan = new SparkSqlSemanticQueryPlanner().plan(salesModel(), query, CONTEXT);

    assertEquals(
        sql(
            "SELECT",
            "  SUM(`t0`.`amount`) AS `total_revenue`,",
            "  `t0`.`region` AS `region`",
            "FROM",
            "  `sales`.`mart`.`orders` AS `t0`",
            "GROUP BY",
            "  `t0`.`region`"),
        plan.sql());
    assertEquals(SPARK_SQL, plan.dialect());
    assertEquals(SPARK_SQL, plan.planner());
    assertTrue(plan.warnings().isEmpty());
  }

  @Test
  public void testJoinBetweenTwoDatasets() {
    SemanticQuery query =
        query(Arrays.asList("total_revenue"), Arrays.asList("segment"), Arrays.asList(), SPARK_SQL);

    SemanticQueryPlan plan = new SparkSqlSemanticQueryPlanner().plan(salesModel(), query, CONTEXT);

    assertEquals(
        sql(
            "SELECT",
            "  SUM(`t0`.`amount`) AS `total_revenue`,",
            "  `t1`.`segment` AS `segment`",
            "FROM",
            "  `sales`.`mart`.`orders` AS `t0`",
            "INNER JOIN",
            "  `sales`.`mart`.`customers` AS `t1`",
            "  ON `t0`.`customer_id` = `t1`.`customer_id`",
            "GROUP BY",
            "  `t1`.`segment`"),
        plan.sql());
    assertEquals(
        Arrays.asList(
            "orders -> sales.mart.orders AS t0", "customers -> sales.mart.customers AS t1"),
        new SparkSqlSemanticQueryPlanner().resolvedIdentifiers(salesModel(), query));
  }

  @Test
  public void testJoinPathThroughIntermediateDataset() {
    SemanticQuery query =
        query(Arrays.asList("total_revenue"), Arrays.asList("country"), Arrays.asList(), SPARK_SQL);

    SemanticQueryPlan plan = new SparkSqlSemanticQueryPlanner().plan(salesModel(), query, CONTEXT);

    assertEquals(
        sql(
            "SELECT",
            "  SUM(`t0`.`amount`) AS `total_revenue`,",
            "  `t2`.`country` AS `country`",
            "FROM",
            "  `sales`.`mart`.`orders` AS `t0`",
            "INNER JOIN",
            "  `sales`.`mart`.`customers` AS `t1`",
            "  ON `t0`.`customer_id` = `t1`.`customer_id`",
            "INNER JOIN",
            "  `sales`.`mart`.`regions` AS `t2`",
            "  ON `t1`.`region_id` = `t2`.`region_id`",
            "GROUP BY",
            "  `t2`.`country`"),
        plan.sql());
  }

  @Test
  public void testUnjoinableDatasetsFail() {
    SemanticQuery query =
        query(Arrays.asList("total_revenue"), Arrays.asList("segment"), Arrays.asList(), SPARK_SQL);
    SemanticModel model = modelWithoutRelationships();
    SparkSqlSemanticQueryPlanner planner = new SparkSqlSemanticQueryPlanner();

    SemanticQueryPlanException exception =
        assertThrows(SemanticQueryPlanException.class, () -> planner.plan(model, query, CONTEXT));

    assertTrue(exception.getMessage().contains("orders"), exception.getMessage());
    assertTrue(exception.getMessage().contains("customers"), exception.getMessage());
    assertTrue(exception.getMessage().contains("No relationship path"), exception.getMessage());
  }

  @Test
  public void testFilterCombination() {
    SemanticQuery query =
        SemanticQuery.builder()
            .withMetrics(Arrays.asList("total_revenue"))
            .withDimensions(Arrays.asList("region"))
            .withFilters(
                Arrays.asList(
                    SemanticQueryFilter.of("region", SemanticQueryFilter.Operator.EQ, "EMEA"),
                    SemanticQueryFilter.of("amount", SemanticQueryFilter.Operator.GE, "100"),
                    SemanticQueryFilter.of("amount", SemanticQueryFilter.Operator.LT, "1000"),
                    SemanticQueryFilter.of("region", SemanticQueryFilter.Operator.NE, "O'Hare")))
            .withDialect(SPARK_SQL)
            .build();

    SemanticQueryPlan plan = new SparkSqlSemanticQueryPlanner().plan(salesModel(), query, CONTEXT);

    assertEquals(
        sql(
            "SELECT",
            "  SUM(`t0`.`amount`) AS `total_revenue`,",
            "  `t0`.`region` AS `region`",
            "FROM",
            "  `sales`.`mart`.`orders` AS `t0`",
            "WHERE",
            "  `t0`.`region` = 'EMEA' AND `t0`.`amount` >= 100 AND `t0`.`amount` < 1000 AND"
                + " `t0`.`region` <> 'O''Hare'",
            "GROUP BY",
            "  `t0`.`region`"),
        plan.sql());
  }

  @Test
  public void testFilterOnUndeclaredColumnUsesPrimaryDataset() {
    SemanticQuery query =
        query(
            Arrays.asList("total_revenue"),
            Arrays.asList(),
            Arrays.asList(
                SemanticQueryFilter.of("status", SemanticQueryFilter.Operator.EQ, "PAID")),
            SPARK_SQL);

    SemanticQueryPlan plan = new SparkSqlSemanticQueryPlanner().plan(salesModel(), query, CONTEXT);

    assertEquals(
        sql(
            "SELECT",
            "  SUM(`t0`.`amount`) AS `total_revenue`",
            "FROM",
            "  `sales`.`mart`.`orders` AS `t0`",
            "WHERE",
            "  `t0`.`status` = 'PAID'"),
        plan.sql());
  }

  @Test
  public void testAggregatedMetricWithoutDimensionHasNoGroupBy() {
    SemanticQuery query =
        query(Arrays.asList("total_revenue"), Arrays.asList(), Arrays.asList(), SPARK_SQL);

    SemanticQueryPlan plan = new SparkSqlSemanticQueryPlanner().plan(salesModel(), query, CONTEXT);

    assertEquals(
        sql(
            "SELECT",
            "  SUM(`t0`.`amount`) AS `total_revenue`",
            "FROM",
            "  `sales`.`mart`.`orders` AS `t0`"),
        plan.sql());
  }

  @Test
  public void testCountDistinctAndUnqualifiedMetricExpression() {
    SemanticQuery query =
        query(
            Arrays.asList("order_count", "average_amount"),
            Arrays.asList(),
            Arrays.asList(),
            SPARK_SQL);

    SemanticQueryPlan plan = new SparkSqlSemanticQueryPlanner().plan(salesModel(), query, CONTEXT);

    assertEquals(
        sql(
            "SELECT",
            "  COUNT(DISTINCT `t0`.`order_id`) AS `order_count`,",
            "  AVG(`t0`.`amount`) AS `average_amount`",
            "FROM",
            "  `sales`.`mart`.`orders` AS `t0`"),
        plan.sql());
  }

  @Test
  public void testDimensionOnlyQueryHasNoGroupBy() {
    SemanticQuery query =
        query(Arrays.asList(), Arrays.asList("region"), Arrays.asList(), SPARK_SQL);

    SemanticQueryPlan plan = new SparkSqlSemanticQueryPlanner().plan(salesModel(), query, CONTEXT);

    assertEquals(
        sql("SELECT", "  `t0`.`region` AS `region`", "FROM", "  `sales`.`mart`.`orders` AS `t0`"),
        plan.sql());
  }

  @Test
  public void testUnknownMetricFails() {
    SemanticQuery query =
        query(Arrays.asList("net_profit"), Arrays.asList(), Arrays.asList(), SPARK_SQL);
    SparkSqlSemanticQueryPlanner planner = new SparkSqlSemanticQueryPlanner();

    SemanticQueryPlanException exception =
        assertThrows(
            SemanticQueryPlanException.class, () -> planner.plan(salesModel(), query, CONTEXT));

    assertTrue(exception.getMessage().contains("net_profit"), exception.getMessage());
  }

  @Test
  public void testUnknownDimensionFails() {
    SemanticQuery query = query(Arrays.asList(), Arrays.asList("nope"), Arrays.asList(), SPARK_SQL);
    SparkSqlSemanticQueryPlanner planner = new SparkSqlSemanticQueryPlanner();

    SemanticQueryPlanException exception =
        assertThrows(
            SemanticQueryPlanException.class, () -> planner.plan(salesModel(), query, CONTEXT));

    assertTrue(exception.getMessage().contains("nope"), exception.getMessage());
  }

  @Test
  public void testAmbiguousFieldNeedsQualification() {
    SemanticModel model = ambiguousModel();
    SparkSqlSemanticQueryPlanner planner = new SparkSqlSemanticQueryPlanner();
    SemanticQuery ambiguous =
        query(Arrays.asList(), Arrays.asList("code"), Arrays.asList(), SPARK_SQL);

    SemanticQueryPlanException exception =
        assertThrows(
            SemanticQueryPlanException.class, () -> planner.plan(model, ambiguous, CONTEXT));

    assertTrue(exception.getMessage().contains("Ambiguous"), exception.getMessage());
    assertTrue(exception.getMessage().contains("orders"), exception.getMessage());
    assertTrue(exception.getMessage().contains("customers"), exception.getMessage());

    SemanticQuery qualified =
        query(Arrays.asList(), Arrays.asList("customers.code"), Arrays.asList(), SPARK_SQL);
    assertEquals(
        sql("SELECT", "  `t0`.`code` AS `code`", "FROM", "  `sales`.`mart`.`customers` AS `t0`"),
        planner.plan(model, qualified, CONTEXT).sql());
  }

  @Test
  public void testUnsupportedDialectIsRejected() {
    SemanticQuery query =
        query(Arrays.asList("total_revenue"), Arrays.asList(), Arrays.asList(), "trino");
    SparkSqlSemanticQueryPlanner planner = new SparkSqlSemanticQueryPlanner();

    SemanticQueryPlanException exception =
        assertThrows(
            SemanticQueryPlanException.class, () -> planner.plan(salesModel(), query, CONTEXT));

    assertTrue(exception.getMessage().contains("Unsupported dialect 'trino'"));
    assertTrue(exception.getMessage().contains("spark-sql"));
  }

  @Test
  public void testAnsiSqlDialectIsAcceptedAndPreserved() {
    SemanticQuery query =
        query(Arrays.asList("total_revenue"), Arrays.asList("region"), Arrays.asList(), "ANSI_SQL");

    SemanticQueryPlan plan = new SparkSqlSemanticQueryPlanner().plan(salesModel(), query, CONTEXT);

    assertEquals("ANSI_SQL", plan.dialect());
    assertEquals(
        sql(
            "SELECT",
            "  SUM(`t0`.`amount`) AS `total_revenue`,",
            "  `t0`.`region` AS `region`",
            "FROM",
            "  `sales`.`mart`.`orders` AS `t0`",
            "GROUP BY",
            "  `t0`.`region`"),
        plan.sql());
  }

  @Test
  public void testOutputIsDeterministic() {
    SemanticQuery query =
        query(
            Arrays.asList("total_revenue", "order_count"),
            Arrays.asList("segment", "region"),
            Arrays.asList(SemanticQueryFilter.of("amount", SemanticQueryFilter.Operator.GT, "0")),
            SPARK_SQL);

    String first = new SparkSqlSemanticQueryPlanner().plan(salesModel(), query, CONTEXT).sql();
    String second = new SparkSqlSemanticQueryPlanner().plan(salesModel(), query, CONTEXT).sql();
    String third =
        new SparkSqlSemanticQueryPlanner()
            .plan(
                salesModel(),
                query,
                PlanningContext.of(
                    NameIdentifier.of("metalake", "catalog", "schema", "sales_model")))
            .sql();

    assertEquals(first, second);
    assertEquals(first, third);
    assertEquals(
        sql(
            "SELECT",
            "  SUM(`t0`.`amount`) AS `total_revenue`,",
            "  COUNT(DISTINCT `t0`.`order_id`) AS `order_count`,",
            "  `t1`.`segment` AS `segment`,",
            "  `t0`.`region` AS `region`",
            "FROM",
            "  `sales`.`mart`.`orders` AS `t0`",
            "INNER JOIN",
            "  `sales`.`mart`.`customers` AS `t1`",
            "  ON `t0`.`customer_id` = `t1`.`customer_id`",
            "WHERE",
            "  `t0`.`amount` > 0",
            "GROUP BY",
            "  `t1`.`segment`,",
            "  `t0`.`region`"),
        first);
  }

  @Test
  public void testLimitFromPlanningContext() {
    SemanticQuery query =
        query(Arrays.asList("total_revenue"), Arrays.asList("region"), Arrays.asList(), SPARK_SQL);

    SemanticQueryPlan plan =
        new SparkSqlSemanticQueryPlanner()
            .plan(
                salesModel(), query, PlanningContext.of(MODEL_IDENTIFIER, Map.of("limit", " 25 ")));

    assertEquals(
        sql(
            "SELECT",
            "  SUM(`t0`.`amount`) AS `total_revenue`,",
            "  `t0`.`region` AS `region`",
            "FROM",
            "  `sales`.`mart`.`orders` AS `t0`",
            "GROUP BY",
            "  `t0`.`region`",
            "LIMIT 25"),
        plan.sql());
  }

  @Test
  public void testInvalidLimitIsRejected() {
    SemanticQuery query =
        query(Arrays.asList("total_revenue"), Arrays.asList(), Arrays.asList(), SPARK_SQL);
    SparkSqlSemanticQueryPlanner planner = new SparkSqlSemanticQueryPlanner();

    assertThrows(
        SemanticQueryPlanException.class,
        () ->
            planner.plan(
                salesModel(),
                query,
                PlanningContext.of(MODEL_IDENTIFIER, Map.of("limit", "many"))));

    assertThrows(
        SemanticQueryPlanException.class,
        () ->
            planner.plan(
                salesModel(), query, PlanningContext.of(MODEL_IDENTIFIER, Map.of("limit", "-1"))));
  }

  private static SemanticQuery query(
      List<String> metrics,
      List<String> dimensions,
      List<SemanticQueryFilter> filters,
      String dialect) {
    return SemanticQuery.builder()
        .withMetrics(metrics)
        .withDimensions(dimensions)
        .withFilters(filters)
        .withDialect(dialect)
        .build();
  }

  private static String sql(String... lines) {
    return String.join("\n", lines);
  }

  private static Expression expression(String text) {
    return Expression.builder()
        .withDialects(
            new DialectExpression[] {
              DialectExpression.builder()
                  .withDialect(Dialects.ANSI_SQL)
                  .withExpression(text)
                  .build()
            })
        .build();
  }

  private static Field field(String name, String text, DataType datatype) {
    return Field.builder()
        .withName(name)
        .withExpression(expression(text))
        .withDatatype(datatype)
        .build();
  }

  private static Dataset ordersDataset() {
    return Dataset.builder()
        .withName("orders")
        .withSource(NameIdentifier.of("sales", "mart", "orders"))
        .withFields(
            new Field[] {
              field("amount", "amount", DataType.DECIMAL),
              field("region", "region", DataType.STRING)
            })
        .build();
  }

  private static Dataset customersDataset() {
    return Dataset.builder()
        .withName("customers")
        .withSource(NameIdentifier.of("sales", "mart", "customers"))
        .withFields(new Field[] {field("segment", "segment", DataType.STRING)})
        .build();
  }

  private static Dataset regionsDataset() {
    return Dataset.builder()
        .withName("regions")
        .withSource(NameIdentifier.of("sales", "mart", "regions"))
        .withFields(new Field[] {field("country", "country", DataType.STRING)})
        .build();
  }

  private static Relationship ordersToCustomers() {
    return Relationship.builder()
        .withName("orders_to_customers")
        .withFrom("orders")
        .withTo("customers")
        .withFromColumns(new String[] {"customer_id"})
        .withToColumns(new String[] {"customer_id"})
        .build();
  }

  private static Relationship customersToRegions() {
    return Relationship.builder()
        .withName("customers_to_regions")
        .withFrom("customers")
        .withTo("regions")
        .withFromColumns(new String[] {"region_id"})
        .withToColumns(new String[] {"region_id"})
        .build();
  }

  private static Metric metric(String name, String text, DataType datatype) {
    return Metric.builder()
        .withName(name)
        .withExpression(expression(text))
        .withDatatype(datatype)
        .build();
  }

  private static SemanticModel salesModel() {
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {ordersDataset(), customersDataset(), regionsDataset()})
            .withRelationships(new Relationship[] {ordersToCustomers(), customersToRegions()})
            .withMetrics(
                new Metric[] {
                  metric("total_revenue", "SUM(orders.amount)", DataType.DECIMAL),
                  metric("order_count", "COUNT(DISTINCT orders.order_id)", DataType.INTEGER),
                  metric("average_amount", "AVG(amount)", DataType.DECIMAL)
                })
            .build();
    return semanticModel("sales_model", definition);
  }

  private static SemanticModel modelWithoutRelationships() {
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {ordersDataset(), customersDataset()})
            .withMetrics(
                new Metric[] {metric("total_revenue", "SUM(orders.amount)", DataType.DECIMAL)})
            .build();
    return semanticModel("sales_model", definition);
  }

  private static SemanticModel ambiguousModel() {
    Field ordersCode = field("code", "code", DataType.STRING);
    Field customersCode = field("code", "code", DataType.STRING);
    Dataset orders =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withFields(new Field[] {ordersCode})
            .build();
    Dataset customers =
        Dataset.builder()
            .withName("customers")
            .withSource(NameIdentifier.of("sales", "mart", "customers"))
            .withFields(new Field[] {customersCode})
            .build();
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder().withDatasets(new Dataset[] {orders, customers}).build();
    return semanticModel("ambiguous_model", definition);
  }

  private static SemanticModel semanticModel(String name, SemanticModelDefinition definition) {
    return new FixtureSemanticModel(name, definition, "fixture");
  }

  /** A Semantic Model backed by a fixture definition, without a storage layer. */
  private static final class FixtureSemanticModel implements SemanticModel {

    private final String name;
    private final SemanticModelDefinition definition;
    @Nullable private final String comment;

    private FixtureSemanticModel(
        String name, SemanticModelDefinition definition, @Nullable String comment) {
      this.name = name;
      this.definition = definition;
      this.comment = comment;
    }

    @Override
    public String name() {
      return name;
    }

    @Nullable
    @Override
    public String comment() {
      return comment;
    }

    @Override
    public SemanticModelDefinition definition() {
      return definition;
    }

    @Override
    public Audit auditInfo() {
      return new FixtureAudit();
    }
  }

  /** Audit information for the fixture Semantic Model. */
  private static final class FixtureAudit implements Audit {

    @Override
    public String creator() {
      return "fixture";
    }

    @Override
    public Instant createTime() {
      return Instant.EPOCH;
    }

    @Override
    public String lastModifier() {
      return "fixture";
    }

    @Override
    public Instant lastModifiedTime() {
      return Instant.EPOCH;
    }
  }
}
