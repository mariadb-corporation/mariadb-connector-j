// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public final class ClientParser implements PrepareResult {

  // This regex is referred from ServerPreparedStatement. It is used to determine whether input SQL
  // string is of 'Insert' statement or not.
  public static final Pattern INSERT_STATEMENT_PATTERN =
      Pattern.compile(
          "^(\\s*\\/\\*([^\\*]|\\*[^\\/])*\\*\\/)*\\s*(INSERT)", Pattern.CASE_INSENSITIVE);

  public static final Pattern INSERT_ON_DUPLICATE_KEY_UPDATE_STATEMENT_PATTERN =
      Pattern.compile(
          "^.+[^`](ON)\\s.*(DUPLICATE)\\s.*(KEY)\\s.*(UPDATE)[^`].+", Pattern.CASE_INSENSITIVE);

  private final String sql;
  private final byte[] query;
  private final List<Integer> paramPositions;
  private final int paramCount;

  private ClientParser(String sql, byte[] query, List<Integer> paramPositions) {
    this.sql = sql;
    this.query = query;
    this.paramPositions = paramPositions;
    this.paramCount = paramPositions.size();
  }

  // isRewriteBatchedApplicable returns true if the parameter sql
  // represents INSERT operation without 'ON DUPLICATE KEY UPDATE' clause
  //
  public boolean isRewriteBatchedApplicable() {
    return INSERT_STATEMENT_PATTERN.matcher(this.sql).find()
        && !INSERT_ON_DUPLICATE_KEY_UPDATE_STATEMENT_PATTERN.matcher(this.sql).find();
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
    return new ClientParser(queryString, query, paramPositions);
  }

  public String getSql() {
    return sql;
  }

  public byte[] getQuery() {
    return query;
  }

  public int getParamCount() {
    return paramCount;
  }

  public List<Integer> getParamPositions() {
    return paramPositions;
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
