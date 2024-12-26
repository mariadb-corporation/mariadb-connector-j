// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class ClientParser implements PrepareResult {

  private final String sql;
  private final byte[] query;
  private final List<Integer> paramPositions;
  private final int paramCount;
  private final boolean isInsert;
  private final boolean isInsertDuplicate;
  private final boolean isMultiQuery;

  private ClientParser(
      String sql,
      byte[] query,
      List<Integer> paramPositions,
      boolean isInsert,
      boolean isInsertDuplicate,
      boolean isMultiQuery) {
    this.sql = sql;
    this.query = query;
    this.paramPositions = paramPositions;
    this.paramCount = paramPositions.size();
    this.isInsert = isInsert;
    this.isInsertDuplicate = isInsertDuplicate;
    this.isMultiQuery = isMultiQuery;
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

    List<Integer> paramPositions = new ArrayList<>();
    LexState state = LexState.Normal;
    byte lastChar = 0x00;

    boolean singleQuotes = false;
    boolean isInsert = false;
    boolean isInsertDupplicate = false;
    int multiQueryIdx = -1;
    byte[] query = queryString.getBytes(StandardCharsets.UTF_8);
    int queryLength = query.length;
    for (int i = 0; i < queryLength; i++) {

      byte car = query[i];
      if (state == LexState.Escape
          && !((car == '\'' && singleQuotes) || (car == '"' && !singleQuotes))) {
        state = LexState.String;
        lastChar = car;
        continue;
      }
      switch (car) {
        case (byte) '*':
          if (state == LexState.Normal && lastChar == (byte) '/') {
            state = LexState.SlashStarComment;
          }
          break;

        case (byte) '/':
          if (state == LexState.SlashStarComment && lastChar == (byte) '*') {
            state = LexState.Normal;
          } else if (state == LexState.Normal && lastChar == (byte) '/') {
            state = LexState.EOLComment;
          }
          break;

        case (byte) ';':
          if (state == LexState.Normal && multiQueryIdx == -1) {
            multiQueryIdx = i;
          }
          break;

        case (byte) '#':
          if (state == LexState.Normal) {
            state = LexState.EOLComment;
          }
          break;

        case (byte) '-':
          if (state == LexState.Normal && lastChar == (byte) '-') {
            state = LexState.EOLComment;
          }
          break;

        case (byte) '\n':
          if (state == LexState.EOLComment) {
            state = LexState.Normal;
          }
          break;

        case (byte) '"':
          if (state == LexState.Normal) {
            state = LexState.String;
            singleQuotes = false;
          } else if (state == LexState.String && !singleQuotes) {
            state = LexState.Normal;
          } else if (state == LexState.Escape) {
            state = LexState.String;
          }
          break;

        case (byte) '\'':
          if (state == LexState.Normal) {
            state = LexState.String;
            singleQuotes = true;
          } else if (state == LexState.String && singleQuotes) {
            state = LexState.Normal;
          } else if (state == LexState.Escape) {
            state = LexState.String;
          }
          break;

        case (byte) 'I':
        case (byte) 'i':
          if (state == LexState.Normal
              && !isInsert
              && i + 6 < queryLength
              && (query[i + 1] == (byte) 'n' || query[i + 1] == (byte) 'N')
              && (query[i + 2] == (byte) 's' || query[i + 2] == (byte) 'S')
              && (query[i + 3] == (byte) 'e' || query[i + 3] == (byte) 'E')
              && (query[i + 4] == (byte) 'r' || query[i + 4] == (byte) 'R')
              && (query[i + 5] == (byte) 't' || query[i + 5] == (byte) 'T')) {
            if (i > 0 && (query[i - 1] > ' ' && "();><=-+,".indexOf(query[i - 1]) == -1)) {
              break;
            }
            if (query[i + 6] > ' ' && "();><=-+,".indexOf(query[i + 6]) == -1) {
              break;
            }
            i += 5;
            isInsert = true;
          }
          break;
        case (byte) 'D':
        case (byte) 'd':
          if (isInsert
              && state == LexState.Normal
              && i + 9 < queryLength
              && (query[i + 1] == (byte) 'u' || query[i + 1] == (byte) 'U')
              && (query[i + 2] == (byte) 'p' || query[i + 2] == (byte) 'P')
              && (query[i + 3] == (byte) 'l' || query[i + 3] == (byte) 'L')
              && (query[i + 4] == (byte) 'i' || query[i + 4] == (byte) 'I')
              && (query[i + 5] == (byte) 'c' || query[i + 5] == (byte) 'C')
              && (query[i + 6] == (byte) 'a' || query[i + 6] == (byte) 'A')
              && (query[i + 7] == (byte) 't' || query[i + 7] == (byte) 'T')
              && (query[i + 8] == (byte) 'e' || query[i + 8] == (byte) 'E')) {
            if (i > 0 && (query[i - 1] > ' ' && "();><=-+,".indexOf(query[i - 1]) == -1)) {
              break;
            }
            if (query[i + 9] > ' ' && "();><=-+,".indexOf(query[i + 9]) == -1) {
              break;
            }
            i += 9;
            isInsertDupplicate = true;
          }
          break;

        case (byte) '\\':
          if (noBackslashEscapes) {
            break;
          }
          if (state == LexState.String) {
            state = LexState.Escape;
          }
          break;
        case (byte) '?':
          if (state == LexState.Normal) {
            paramPositions.add(i);
          }
          break;
        case (byte) '`':
          if (state == LexState.Backtick) {
            state = LexState.Normal;
          } else if (state == LexState.Normal) {
            state = LexState.Backtick;
          }
          break;
      }
      lastChar = car;
    }

    // multi contains ";" not finishing statement.
    boolean isMulti = multiQueryIdx != -1 && multiQueryIdx < queryLength - 1;
    if (isMulti) {
      // ensure there is not only empty
      boolean hasAdditionalPart = false;
      for (int i = multiQueryIdx + 1; i < queryLength; i++) {
        byte car = query[i];
        if (car != (byte) ' ' && car != (byte) '\n' && car != (byte) '\r' && car != (byte) '\t') {
          hasAdditionalPart = true;
          break;
        }
      }
      isMulti = hasAdditionalPart;
    }
    return new ClientParser(
        queryString, query, paramPositions, isInsert, isInsertDupplicate, isMulti);
  }

  public String getSql() {
    return sql;
  }

  public byte[] getQuery() {
    return query;
  }

  public List<Integer> getParamPositions() {
    return paramPositions;
  }

  public int getParamCount() {
    return paramCount;
  }

  public boolean isInsert() {
    return isInsert;
  }

  public boolean isInsertDuplicate() {
    return isInsertDuplicate;
  }

  public boolean isMultiQuery() {
    return isMultiQuery;
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
