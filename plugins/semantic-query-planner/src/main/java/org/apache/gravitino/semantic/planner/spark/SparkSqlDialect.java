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

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.SemanticQueryPlanException;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dialects;
import org.apache.gravitino.semantic.Expression;

/**
 * The Spark SQL rendering rules used by {@link SparkSqlSemanticQueryPlanner}.
 *
 * <p>Every identifier the planner emits is quoted with backticks so that names containing
 * characters Spark SQL reserves need no escaping. A backtick inside a name is escaped by doubling
 * it, which is the Spark SQL convention.
 *
 * <p>The dialect matrix is small on purpose. A {@link org.apache.gravitino.semantic.SemanticQuery}
 * carries a free-form dialect identifier, and the planner only accepts identifiers that name Spark
 * SQL or an ANSI SQL dialect Spark SQL can run. Any other dialect is rejected with a {@link
 * SemanticQueryPlanException} instead of being approximated silently.
 */
public final class SparkSqlDialect {

  /** The dialect identifier that selects the Spark SQL planner. */
  public static final String NAME = "spark-sql";

  private static final List<String> SUPPORTED_DIALECTS =
      Collections.unmodifiableList(
          Arrays.asList(
              NAME, "spark", "spark_sql", "sparksql", "databricks", "ansi_sql", "ansi", "sql"));

  /**
   * The dialects consulted, in order, when an expression has no representation for the requested
   * dialect. Databricks SQL is Spark SQL, and ANSI SQL is the portable representation Gravitino
   * documents for Semantic Model expressions.
   */
  private static final List<String> FALLBACK_EXPRESSION_DIALECTS =
      Collections.unmodifiableList(Arrays.asList(Dialects.DATABRICKS, Dialects.ANSI_SQL));

  private static final Set<DataType> NUMERIC_DATA_TYPES =
      Collections.unmodifiableSet(EnumSet.of(DataType.INTEGER, DataType.DECIMAL, DataType.FLOAT));

  private static final Pattern NUMERIC_LITERAL = Pattern.compile("[+-]?\\d+(\\.\\d+)?");

  private SparkSqlDialect() {}

  /**
   * Returns the dialect identifiers the planner accepts.
   *
   * @return An unmodifiable list of accepted dialect identifiers.
   */
  public static List<String> supportedDialects() {
    return SUPPORTED_DIALECTS;
  }

  /**
   * Reports whether the planner accepts a dialect identifier.
   *
   * @param dialect The dialect identifier of a Semantic Model query.
   * @return {@code true} if the planner generates SQL for the dialect.
   */
  public static boolean isSupported(@Nullable String dialect) {
    return dialect != null && SUPPORTED_DIALECTS.contains(normalize(dialect));
  }

  /**
   * Verifies that the planner accepts a dialect identifier.
   *
   * @param dialect The dialect identifier of a Semantic Model query.
   * @throws SemanticQueryPlanException If the dialect is not Spark SQL or ANSI SQL.
   */
  public static void checkSupported(String dialect) {
    if (!isSupported(dialect)) {
      throw new SemanticQueryPlanException(
          "Unsupported dialect '%s': planner '%s' only generates Spark SQL. Supported dialects are %s",
          dialect, NAME, SUPPORTED_DIALECTS);
    }
  }

  /**
   * Quotes a single SQL identifier with backticks.
   *
   * @param identifier The identifier to quote.
   * @return The backtick-quoted identifier.
   */
  public static String quote(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  /**
   * Quotes every level of an identifier with backticks and joins the levels with dots.
   *
   * @param identifier The identifier to quote, including its namespace.
   * @return The backtick-quoted, dot-separated identifier.
   */
  public static String quote(NameIdentifier identifier) {
    StringBuilder quoted = new StringBuilder();
    for (String level : identifier.namespace().levels()) {
      quoted.append(quote(level)).append('.');
    }
    return quoted.append(quote(identifier.name())).toString();
  }

  /**
   * Renders a Semantic Model expression text for Spark SQL.
   *
   * <p>The method prefers the representation written in the requested dialect, then the Databricks
   * SQL representation, then the ANSI SQL representation, and finally the first representation the
   * expression carries, so that the result never depends on hash ordering.
   *
   * @param expression The expression to select a representation from.
   * @param requestedDialect The dialect identifier of the query.
   * @return The expression text to render.
   */
  public static String expressionFor(Expression expression, String requestedDialect) {
    DialectExpression[] dialects = expression.dialects();
    String wanted = normalize(requestedDialect);
    for (DialectExpression candidate : dialects) {
      if (normalize(candidate.dialect()).equals(wanted)) {
        return candidate.expression();
      }
    }
    for (String fallback : FALLBACK_EXPRESSION_DIALECTS) {
      for (DialectExpression candidate : dialects) {
        if (normalize(candidate.dialect()).equals(normalize(fallback))) {
          return candidate.expression();
        }
      }
    }
    return dialects[0].expression();
  }

  /**
   * Renders a filter value as a Spark SQL literal.
   *
   * <p>Semantic Model filters carry the compared value as text. When the field declares a {@link
   * DataType}, the literal follows it so that numeric and boolean columns compare without an
   * implicit cast; otherwise the value is quoted as a string literal unless it already looks like a
   * number or a boolean.
   *
   * @param value The filter value.
   * @param datatype The declared data type of the filtered field, or {@code null} if it has none.
   * @return The Spark SQL literal.
   * @throws SemanticQueryPlanException If the value cannot be represented as the declared type.
   */
  public static String literal(String value, @Nullable DataType datatype) {
    if (datatype != null && NUMERIC_DATA_TYPES.contains(datatype)) {
      if (!NUMERIC_LITERAL.matcher(value.trim()).matches()) {
        throw new SemanticQueryPlanException(
            "Filter value '%s' is not a valid %s literal", value, datatype);
      }
      return value.trim();
    }

    if (datatype == DataType.BOOLEAN) {
      String normalized = value.trim().toLowerCase(Locale.ROOT);
      if (!"true".equals(normalized) && !"false".equals(normalized)) {
        throw new SemanticQueryPlanException(
            "Filter value '%s' is not a valid %s literal", value, datatype);
      }
      return normalized.toUpperCase(Locale.ROOT);
    }

    if (datatype == null) {
      String trimmed = value.trim();
      if (NUMERIC_LITERAL.matcher(trimmed).matches()) {
        return trimmed;
      }
      String normalized = trimmed.toLowerCase(Locale.ROOT);
      if ("true".equals(normalized) || "false".equals(normalized)) {
        return normalized.toUpperCase(Locale.ROOT);
      }
    }

    return "'" + value.replace("'", "''") + "'";
  }

  private static String normalize(@Nullable String dialect) {
    return dialect == null ? "" : dialect.trim().toLowerCase(Locale.ROOT);
  }
}
