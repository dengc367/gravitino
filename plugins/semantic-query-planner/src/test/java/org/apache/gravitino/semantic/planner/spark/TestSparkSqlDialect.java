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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.SemanticQueryPlanException;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dialects;
import org.apache.gravitino.semantic.Expression;
import org.junit.jupiter.api.Test;

/** Tests the quoting, dialect and literal rules of {@link SparkSqlDialect}. */
public class TestSparkSqlDialect {

  @Test
  public void testQuotesIdentifiers() {
    assertEquals("`amount`", SparkSqlDialect.quote("amount"));
    assertEquals("`total revenue`", SparkSqlDialect.quote("total revenue"));
    assertEquals("`a``b`", SparkSqlDialect.quote("a`b"));
  }

  @Test
  public void testQuotesEveryNamespaceLevel() {
    assertEquals(
        "`sales`.`mart`.`orders`",
        SparkSqlDialect.quote(NameIdentifier.of("sales", "mart", "orders")));
    assertEquals("`orders`", SparkSqlDialect.quote(NameIdentifier.of("orders")));
  }

  @Test
  public void testAcceptsSparkAndAnsiDialects() {
    for (String dialect :
        Arrays.asList(
            "spark-sql", "SPARK", "spark_sql", "databricks", "ANSI_SQL", "sql", " Ansi ")) {
      assertTrue(SparkSqlDialect.isSupported(dialect), dialect);
    }
  }

  @Test
  public void testRejectsOtherDialects() {
    for (String dialect :
        Arrays.asList("trino", "snowflake", "bigquery", "MDX", "TABLEAU", "maql", "")) {
      assertFalse(SparkSqlDialect.isSupported(dialect), dialect);
    }
    assertFalse(SparkSqlDialect.isSupported(null));
  }

  @Test
  public void testCheckSupportedReportsTheAcceptedDialects() {
    SemanticQueryPlanException exception =
        assertThrows(
            SemanticQueryPlanException.class, () -> SparkSqlDialect.checkSupported("snowflake"));

    assertTrue(exception.getMessage().contains("snowflake"));
    assertTrue(exception.getMessage().contains("spark-sql"));
  }

  @Test
  public void testSupportedDialectListIsStable() {
    List<String> dialects = SparkSqlDialect.supportedDialects();

    assertEquals("spark-sql", dialects.get(0));
    assertTrue(dialects.contains("ansi_sql"));
    assertThrows(UnsupportedOperationException.class, () -> dialects.add("hive"));
  }

  @Test
  public void testLiteralFollowsDeclaredDataType() {
    assertEquals("100", SparkSqlDialect.literal("100", DataType.INTEGER));
    assertEquals("12.5", SparkSqlDialect.literal("12.5", DataType.DECIMAL));
    assertEquals("-3", SparkSqlDialect.literal("-3", DataType.FLOAT));
    assertEquals("TRUE", SparkSqlDialect.literal("true", DataType.BOOLEAN));
    assertEquals("FALSE", SparkSqlDialect.literal("False", DataType.BOOLEAN));
    assertEquals("'EMEA'", SparkSqlDialect.literal("EMEA", DataType.STRING));
    assertEquals("'2026-10-29'", SparkSqlDialect.literal("2026-10-29", DataType.DATE));
  }

  @Test
  public void testLiteralRejectsValuesThatDoNotMatchTheDeclaredType() {
    assertThrows(
        SemanticQueryPlanException.class, () -> SparkSqlDialect.literal("many", DataType.INTEGER));
    assertThrows(
        SemanticQueryPlanException.class, () -> SparkSqlDialect.literal("maybe", DataType.BOOLEAN));
  }

  @Test
  public void testLiteralWithoutDeclaredDataType() {
    assertEquals("42", SparkSqlDialect.literal("42", null));
    assertEquals("4.25", SparkSqlDialect.literal("4.25", null));
    assertEquals("TRUE", SparkSqlDialect.literal(" true ", null));
    assertEquals("'42A'", SparkSqlDialect.literal("42A", null));
    assertEquals("'O''Hare'", SparkSqlDialect.literal("O'Hare", null));
  }

  @Test
  public void testExpressionPrefersTheDatabricksRepresentationForSpark() {
    Expression expression =
        Expression.builder()
            .withDialects(
                new DialectExpression[] {
                  dialectExpression(Dialects.ANSI_SQL, "ANSI_TEXT"),
                  dialectExpression(Dialects.DATABRICKS, "DATABRICKS_TEXT")
                })
            .build();

    assertEquals("DATABRICKS_TEXT", SparkSqlDialect.expressionFor(expression, "spark-sql"));
    assertEquals("ANSI_TEXT", SparkSqlDialect.expressionFor(expression, "ANSI_SQL"));
    assertEquals("DATABRICKS_TEXT", SparkSqlDialect.expressionFor(expression, "bigquery"));
  }

  @Test
  public void testExpressionPrefersAnExactDialectMatch() {
    Expression expression =
        Expression.builder()
            .withDialects(
                new DialectExpression[] {
                  dialectExpression(Dialects.ANSI_SQL, "ANSI_TEXT"),
                  dialectExpression("spark-sql", "SPARK_TEXT")
                })
            .build();

    assertEquals("SPARK_TEXT", SparkSqlDialect.expressionFor(expression, "spark-sql"));
    assertEquals("SPARK_TEXT", SparkSqlDialect.expressionFor(expression, " Spark-SQL "));
  }

  @Test
  public void testExpressionFallsBackToAnsiSql() {
    Expression expression =
        Expression.builder()
            .withDialects(
                new DialectExpression[] {
                  dialectExpression(Dialects.SNOWFLAKE, "SNOWFLAKE_TEXT"),
                  dialectExpression(Dialects.ANSI_SQL, "ANSI_TEXT")
                })
            .build();

    assertEquals("ANSI_TEXT", SparkSqlDialect.expressionFor(expression, "spark-sql"));
  }

  @Test
  public void testExpressionFallsBackToTheFirstRepresentation() {
    Expression expression =
        Expression.builder()
            .withDialects(
                new DialectExpression[] {
                  dialectExpression(Dialects.MDX, "MDX_TEXT"),
                  dialectExpression(Dialects.TABLEAU, "TABLEAU_TEXT")
                })
            .build();

    assertEquals("MDX_TEXT", SparkSqlDialect.expressionFor(expression, "spark-sql"));
  }

  private static DialectExpression dialectExpression(String dialect, String text) {
    return DialectExpression.builder().withDialect(dialect).withExpression(text).build();
  }
}
