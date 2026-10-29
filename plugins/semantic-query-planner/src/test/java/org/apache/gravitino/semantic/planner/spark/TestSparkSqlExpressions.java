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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Tests the expression rewriting rules of {@link SparkSqlExpressions}. */
public class TestSparkSqlExpressions {

  private static final Set<String> DATASETS =
      Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList("orders", "customers")));

  @Test
  public void testRewritesDatasetQualifiedReferences() {
    assertEquals("SUM(`t0`.`amount`)", rewrite("SUM(orders.amount)", Collections.emptySet()));
    assertEquals("SUM(`t1`.`segment`)", rewrite("SUM(customers.segment)", Collections.emptySet()));
  }

  @Test
  public void testRewritesDeclaredFieldsToTheOwningAlias() {
    assertEquals("`t0`.`amount`", rewrite("amount", Collections.singleton("amount")));
    assertEquals("`t0`.`amount` * 2", rewrite("amount * 2", Collections.singleton("amount")));
  }

  @Test
  public void testLeavesUndeclaredIdentifiersUntouched() {
    assertEquals("order_id", rewrite("order_id", Collections.emptySet()));
    assertEquals("SUM(order_id)", rewrite("SUM(order_id)", Collections.emptySet()));
  }

  @Test
  public void testPreservesKeywordsAndFunctionNames() {
    assertEquals(
        "CAST(`t0`.`amount` AS DECIMAL(10,2))",
        rewrite("CAST(amount AS DECIMAL(10,2))", Collections.singleton("amount")));
    assertEquals(
        "CASE WHEN `t0`.`region` = 'EMEA' THEN `t0`.`amount` ELSE 0 END",
        rewrite(
            "CASE WHEN region = 'EMEA' THEN amount ELSE 0 END",
            new LinkedHashSet<>(Arrays.asList("region", "amount"))));
    assertEquals("CURRENT_DATE", rewrite("CURRENT_DATE", Collections.emptySet()));
  }

  @Test
  public void testRewritesBacktickQuotedDeclaredFields() {
    assertEquals("`t0`.`amount`", rewrite("`amount`", Collections.singleton("amount")));
    assertEquals("`other`", rewrite("`other`", Collections.emptySet()));
  }

  @Test
  public void testQuotesUnknownQualifiedReferences() {
    assertEquals("`payload`.`user_id`", rewrite("payload.user_id", Collections.emptySet()));
  }

  @Test
  public void testKeepsStringLiteralsIntact() {
    assertEquals("'SUM(orders.amount)'", rewrite("'SUM(orders.amount)'", Collections.emptySet()));
    assertEquals("'O''Hare'", rewrite("'O''Hare'", Collections.emptySet()));
  }

  @Test
  public void testListsReferencedDatasets() {
    assertEquals(
        Arrays.asList("orders", "customers"),
        SparkSqlExpressions.referencedDatasets("SUM(orders.amount) + customers.segment", DATASETS));
    assertEquals(
        Collections.emptyList(), SparkSqlExpressions.referencedDatasets("SUM(amount)", DATASETS));
    assertEquals(
        Collections.emptyList(), SparkSqlExpressions.referencedDatasets("orders", DATASETS));
  }

  @Test
  public void testListsIdentifiersWithoutFunctionNames() {
    Set<String> identifiers = SparkSqlExpressions.identifiers("COUNT(DISTINCT order_id)");

    assertTrue(identifiers.contains("order_id"));
    assertFalse(identifiers.contains("COUNT"));
  }

  @Test
  public void testDetectsAggregateFunctions() {
    assertTrue(SparkSqlExpressions.containsAggregate("SUM(amount)"));
    assertTrue(SparkSqlExpressions.containsAggregate("COUNT(DISTINCT order_id)"));
    assertTrue(SparkSqlExpressions.containsAggregate("COALESCE(SUM(amount), 0)"));
    assertFalse(SparkSqlExpressions.containsAggregate("amount + 1"));
    assertFalse(SparkSqlExpressions.containsAggregate("amount"));
  }

  private static String rewrite(String expression, Set<String> fieldNames) {
    return SparkSqlExpressions.rewrite(
        expression, fieldNames, "t0", DATASETS, TestSparkSqlExpressions::alias);
  }

  private static String alias(String dataset) {
    return "orders".equals(dataset) ? "t0" : "t1";
  }

  @Test
  public void testPreservesWhitespaceAndPunctuation() {
    assertEquals("SUM( `t0`.`amount` )", rewrite("SUM( amount )", Collections.singleton("amount")));
  }
}
