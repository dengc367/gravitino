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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

/**
 * A small tokenizer and rewriter for the SQL text Semantic Model expressions carry.
 *
 * <p>Expressions are author-written SQL fragments such as {@code SUM(orders.order_amount)} or
 * {@code order_amount}. The rewriter does not parse SQL grammar; it only recognises the reference
 * shapes the Semantic Model API defines:
 *
 * <ul>
 *   <li>{@code <dataset>.<column>} where {@code <dataset>} names a dataset of the model is
 *       rewritten to the alias of that dataset.
 *   <li>a bare or backtick-quoted identifier that names a field declared by the owning dataset is
 *       rewritten to the alias of that dataset.
 *   <li>every other token is copied unchanged, so function names, keywords, literals and casts are
 *       never corrupted.
 * </ul>
 *
 * <p>Every identifier that a rewritten reference emits is backtick-quoted.
 */
final class SparkSqlExpressions {

  private static final Set<String> AGGREGATE_FUNCTIONS =
      Collections.unmodifiableSet(
          new LinkedHashSet<>(
              Arrays.asList(
                  "SUM",
                  "COUNT",
                  "AVG",
                  "MIN",
                  "MAX",
                  "MEDIAN",
                  "MODE",
                  "STDDEV",
                  "STDDEV_POP",
                  "STDDEV_SAMP",
                  "VARIANCE",
                  "VAR_POP",
                  "VAR_SAMP",
                  "SKEWNESS",
                  "KURTOSIS",
                  "CORR",
                  "COVAR_POP",
                  "COVAR_SAMP",
                  "APPROX_COUNT_DISTINCT",
                  "APPROX_PERCENTILE",
                  "PERCENTILE",
                  "PERCENTILE_APPROX",
                  "COLLECT_LIST",
                  "COLLECT_SET",
                  "FIRST",
                  "FIRST_VALUE",
                  "LAST",
                  "LAST_VALUE",
                  "ANY_VALUE",
                  "BOOL_AND",
                  "BOOL_OR",
                  "EVERY",
                  "SOME",
                  "MAX_BY",
                  "MIN_BY")));

  private SparkSqlExpressions() {}

  /**
   * Rewrites the dataset references of an expression to dataset aliases.
   *
   * @param expression The author-written expression text.
   * @param fieldNames The names of the fields the owning dataset declares.
   * @param ownerAlias The alias of the dataset that owns the expression.
   * @param datasetNames The names of all datasets in the model.
   * @param datasetAliases Resolves a dataset name to its alias.
   * @return The expression text with rewritten references.
   */
  static String rewrite(
      String expression,
      Set<String> fieldNames,
      String ownerAlias,
      Set<String> datasetNames,
      Function<String, String> datasetAliases) {
    List<Token> tokens = tokenize(expression);
    StringBuilder sql = new StringBuilder(expression.length() + 16);
    int index = 0;
    while (index < tokens.size()) {
      Token token = tokens.get(index);
      if (token.kind == Kind.IDENT) {
        List<String> chain = new ArrayList<>();
        chain.add(token.text);
        int cursor = index;
        while (true) {
          int dot = nextSignificant(tokens, cursor + 1);
          if (dot < 0 || !tokens.get(dot).isPunctuation(".")) {
            break;
          }
          int level = nextSignificant(tokens, dot + 1);
          if (level < 0 || tokens.get(level).kind != Kind.IDENT) {
            break;
          }
          chain.add(tokens.get(level).text);
          cursor = level;
        }

        if (chain.size() > 1) {
          String head = chain.get(0);
          sql.append(
              SparkSqlDialect.quote(
                  datasetNames.contains(head) ? datasetAliases.apply(head) : head));
          for (int level = 1; level < chain.size(); level++) {
            sql.append('.').append(SparkSqlDialect.quote(chain.get(level)));
          }
          index = cursor + 1;
          continue;
        }

        int after = nextSignificant(tokens, index + 1);
        boolean functionName = after >= 0 && tokens.get(after).isPunctuation("(");
        if (!functionName && fieldNames.contains(token.text)) {
          appendQualified(sql, ownerAlias, token.text);
        } else {
          sql.append(token.text);
        }
        index++;
        continue;
      }

      if (token.kind == Kind.QUOTED_IDENTIFIER) {
        String name = token.unquote();
        if (fieldNames.contains(name)) {
          appendQualified(sql, ownerAlias, name);
        } else {
          sql.append(token.text);
        }
        index++;
        continue;
      }

      sql.append(token.text);
      index++;
    }
    return sql.toString();
  }

  /**
   * Lists the datasets an expression references as {@code <dataset>.<column>}, in first-appearance
   * order.
   *
   * @param expression The author-written expression text.
   * @param datasetNames The names of all datasets in the model.
   * @return An unmodifiable list of referenced dataset names; empty if the expression qualifies no
   *     reference with a dataset name.
   */
  static List<String> referencedDatasets(String expression, Set<String> datasetNames) {
    List<Token> tokens = tokenize(expression);
    Set<String> referenced = new LinkedHashSet<>();
    for (int index = 0; index < tokens.size(); index++) {
      Token token = tokens.get(index);
      if (token.kind != Kind.IDENT || !datasetNames.contains(token.text)) {
        continue;
      }
      int dot = nextSignificant(tokens, index + 1);
      if (dot < 0 || !tokens.get(dot).isPunctuation(".")) {
        continue;
      }
      int column = nextSignificant(tokens, dot + 1);
      if (column >= 0 && tokens.get(column).kind == Kind.IDENT) {
        referenced.add(token.text);
      }
    }
    return Collections.unmodifiableList(new ArrayList<>(referenced));
  }

  /**
   * Lists the identifier tokens of an expression, excluding function names.
   *
   * @param expression The expression text.
   * @return An unmodifiable set of identifiers, in first-appearance order.
   */
  static Set<String> identifiers(String expression) {
    List<Token> tokens = tokenize(expression);
    Set<String> identifiers = new LinkedHashSet<>();
    for (int index = 0; index < tokens.size(); index++) {
      Token token = tokens.get(index);
      if (token.kind == Kind.QUOTED_IDENTIFIER) {
        identifiers.add(token.unquote());
      } else if (token.kind == Kind.IDENT) {
        int after = nextSignificant(tokens, index + 1);
        if (after < 0 || !tokens.get(after).isPunctuation("(")) {
          identifiers.add(token.text);
        }
      }
    }
    return Collections.unmodifiableSet(identifiers);
  }

  /**
   * Reports whether an expression applies an aggregate function.
   *
   * @param expression The expression text.
   * @return {@code true} if the expression calls a known aggregate function.
   */
  static boolean containsAggregate(String expression) {
    List<Token> tokens = tokenize(expression);
    for (int index = 0; index < tokens.size(); index++) {
      Token token = tokens.get(index);
      if (token.kind != Kind.IDENT) {
        continue;
      }
      int after = nextSignificant(tokens, index + 1);
      if (after >= 0
          && tokens.get(after).isPunctuation("(")
          && AGGREGATE_FUNCTIONS.contains(token.text.toUpperCase(Locale.ROOT))) {
        return true;
      }
    }
    return false;
  }

  private static void appendQualified(StringBuilder sql, String alias, String column) {
    sql.append(SparkSqlDialect.quote(alias)).append('.').append(SparkSqlDialect.quote(column));
  }

  private static int nextSignificant(List<Token> tokens, int from) {
    for (int index = from; index < tokens.size(); index++) {
      if (tokens.get(index).kind != Kind.WHITESPACE) {
        return index;
      }
    }
    return -1;
  }

  private static List<Token> tokenize(String expression) {
    List<Token> tokens = new ArrayList<>();
    int index = 0;
    int length = expression.length();
    while (index < length) {
      char current = expression.charAt(index);
      if (Character.isWhitespace(current)) {
        int start = index;
        while (index < length && Character.isWhitespace(expression.charAt(index))) {
          index++;
        }
        tokens.add(new Token(Kind.WHITESPACE, expression.substring(start, index)));
      } else if (current == '\'' || current == '"') {
        index = addQuoted(tokens, expression, index, current, Kind.STRING);
      } else if (current == '`') {
        index = addQuoted(tokens, expression, index, current, Kind.QUOTED_IDENTIFIER);
      } else if (Character.isDigit(current)) {
        int start = index;
        while (index < length
            && (Character.isDigit(expression.charAt(index)) || expression.charAt(index) == '.')) {
          index++;
        }
        tokens.add(new Token(Kind.NUMBER, expression.substring(start, index)));
      } else if (Character.isJavaIdentifierStart(current)) {
        int start = index;
        while (index < length && Character.isJavaIdentifierPart(expression.charAt(index))) {
          index++;
        }
        tokens.add(new Token(Kind.IDENT, expression.substring(start, index)));
      } else {
        tokens.add(new Token(Kind.PUNCTUATION, String.valueOf(current)));
        index++;
      }
    }
    return tokens;
  }

  private static int addQuoted(
      List<Token> tokens, String expression, int start, char quote, Kind kind) {
    int index = start + 1;
    int length = expression.length();
    while (index < length) {
      if (expression.charAt(index) == quote) {
        if (index + 1 < length && expression.charAt(index + 1) == quote) {
          index += 2;
          continue;
        }
        index++;
        break;
      }
      index++;
    }
    tokens.add(new Token(kind, expression.substring(start, index)));
    return index;
  }

  /** The kind of a token the expression tokenizer produces. */
  private enum Kind {
    IDENT,
    QUOTED_IDENTIFIER,
    STRING,
    NUMBER,
    PUNCTUATION,
    WHITESPACE
  }

  /** One token of an expression. */
  private static final class Token {

    private final Kind kind;
    private final String text;

    private Token(Kind kind, String text) {
      this.kind = kind;
      this.text = text;
    }

    private boolean isPunctuation(String symbol) {
      return kind == Kind.PUNCTUATION && text.equals(symbol);
    }

    private String unquote() {
      String inner = text.length() >= 2 ? text.substring(1, text.length() - 1) : "";
      return inner.replace("``", "`");
    }
  }
}
