// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ClientParser implements PrepareResult {

  private final String sql;
  private final List<byte[]> queryParts;
  private final boolean isMultiStatement;
  private final int paramCount;
  private static Map<String, ClientParser> cache = new LinkedHashMap<>(512);

  private ClientParser(
      String sql, List<byte[]> queryParts, int paramCount, boolean isMultiStatement) {
    this.sql = sql;
    this.queryParts = queryParts;
    this.paramCount = paramCount;
    this.isMultiStatement = isMultiStatement;
  }

  /**
   * Create a new Client Parser Object required for the RewriteBatchedStatement requirement.
   *
   * @param queryString
   * @param queryParts
   * @param paramCount
   * @return
   */
  public static ClientParser parameterPartsForRewriteBatchedStatement(
      String queryString, List<byte[]> queryParts, int paramCount) {
    return new ClientParser(queryString, queryParts, paramCount, false);
  }

  /**
   * Separate query in a String list and set flag isQueryMultipleRewritable. The resulting string
   * list is separed by ? that are not in comments. isQueryMultipleRewritable flag is set if query
   * can be rewrite in one query (all case but if using "-- comment"). example for query : "INSERT
   * INTO tableName(id, name) VALUES (?, ?)" result list will be : {"INSERT INTO tableName(id, name)
   * VALUES (", ", ", ")"}
   *
   * @param queryString query
   * @param noBackslashEscapes escape mode
   * @return ClientPrepareResult
   */
  public static ClientParser parameterParts(String queryString, boolean noBackslashEscapes) {
    if (cache.containsKey(queryString)) return cache.get(queryString);

    List<byte[]> partList = new ArrayList<>();
    LexState state = LexState.Normal;
    char lastChar = '\0';
    boolean endingSemicolon = false;
    boolean isMultiStatement = false;

    boolean singleQuotes = false;
    int lastParameterPosition = 0;

    char[] query = queryString.toCharArray();
    int queryLength = query.length;
    for (int i = 0; i < queryLength; i++) {

      char car = query[i];
      if (state == LexState.Escape
          && !((car == '\'' && singleQuotes) || (car == '"' && !singleQuotes))) {
        state = LexState.String;
        lastChar = car;
        continue;
      }
      switch (car) {
        case '*':
          if (state == LexState.Normal && lastChar == '/') {
            state = LexState.SlashStarComment;
          }
          break;

        case '/':
          if (state == LexState.SlashStarComment && lastChar == '*') {
            state = LexState.Normal;
          } else if (state == LexState.Normal && lastChar == '/') {
            state = LexState.EOLComment;
          }
          break;

        case '#':
          if (state == LexState.Normal) {
            state = LexState.EOLComment;
          }
          break;

        case '-':
          if (state == LexState.Normal && lastChar == '-') {
            state = LexState.EOLComment;
          }
          break;

        case '\n':
          if (state == LexState.EOLComment) {
            state = LexState.Normal;
          }
          break;

        case '"':
          if (state == LexState.Normal) {
            state = LexState.String;
            singleQuotes = false;
          } else if (state == LexState.String && !singleQuotes) {
            state = LexState.Normal;
          } else if (state == LexState.Escape) {
            state = LexState.String;
          }
          break;

        case '\'':
          if (state == LexState.Normal) {
            state = LexState.String;
            singleQuotes = true;
          } else if (state == LexState.String && singleQuotes) {
            state = LexState.Normal;
          } else if (state == LexState.Escape) {
            state = LexState.String;
          }
          break;

        case '\\':
          if (noBackslashEscapes) {
            break;
          }
          if (state == LexState.String) {
            state = LexState.Escape;
          }
          break;
        case ';':
          if (state == LexState.Normal) {
            endingSemicolon = true;
          }
          break;
        case '?':
          if (state == LexState.Normal) {
            partList.add(
                queryString.substring(lastParameterPosition, i).getBytes(StandardCharsets.UTF_8));
            lastParameterPosition = i + 1;
          }
          break;
        case '`':
          if (state == LexState.Backtick) {
            state = LexState.Normal;
          } else if (state == LexState.Normal) {
            state = LexState.Backtick;
          }
          break;
        default:
          // multiple queries
          if (state == LexState.Normal && endingSemicolon && ((byte) car >= 40)) {
            endingSemicolon = false;
            isMultiStatement = true;
          }
          break;
      }
      lastChar = car;
    }
    if (lastParameterPosition == 0) {
      partList.add(queryString.getBytes(StandardCharsets.UTF_8));
    } else {
      partList.add(
          queryString
              .substring(lastParameterPosition, queryLength)
              .getBytes(StandardCharsets.UTF_8));
    }

    ClientParser clientParser =
        new ClientParser(queryString, partList, partList.size() - 1, isMultiStatement);
    if (queryString.length() < 16384) cache.put(queryString, clientParser);
    return clientParser;
  }

  public String getSql() {
    return sql;
  }

  public List<byte[]> getQueryParts() {
    return queryParts;
  }

  public int getParamCount() {
    return paramCount;
  }

  public boolean isMultiStatement() {
    return isMultiStatement;
  }

  enum LexState {
    Normal, /* inside  query */
    String, /* inside string */
    SlashStarComment, /* inside slash-star comment */
    Escape, /* found backslash */
    EOLComment, /* # comment, or // comment, or -- comment */
    Backtick /* found backtick */
  }
}
