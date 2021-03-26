/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class ClientParser implements PrepareResult {

  private final String sql;
  private final List<byte[]> queryParts;
  private final boolean rewriteType;
  private final int paramCount;
  private boolean isQueryMultiValuesRewritable;
  private boolean isQueryMultipleRewritable;

  private ClientParser(
      String sql,
      List<byte[]> queryParts,
      boolean isQueryMultiValuesRewritable,
      boolean isQueryMultipleRewritable,
      boolean rewriteType) {
    this.sql = sql;
    this.queryParts = queryParts;
    this.isQueryMultiValuesRewritable = isQueryMultiValuesRewritable;
    this.isQueryMultipleRewritable = isQueryMultipleRewritable;
    this.paramCount = queryParts.size() - (rewriteType ? 3 : 1);
    this.rewriteType = rewriteType;
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
    boolean reWritablePrepare = false;
    boolean multipleQueriesPrepare = true;
    List<byte[]> partList = new ArrayList<>();
    LexState state = LexState.Normal;
    char lastChar = '\0';
    boolean endingSemicolon = false;

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
            multipleQueriesPrepare = false;
          }
          break;

        case '\n':
          if (state == LexState.EOLComment) {
            multipleQueriesPrepare = true;
            state = LexState.Normal;
          }
          break;

        case '"':
          if (state == LexState.Normal) {
            state = LexState.String;
            singleQuotes = false;
          } else if (state == LexState.String && !singleQuotes) {
            state = LexState.Normal;
          } else if (state == LexState.Escape && !singleQuotes) {
            state = LexState.String;
          }
          break;

        case '\'':
          if (state == LexState.Normal) {
            state = LexState.String;
            singleQuotes = true;
          } else if (state == LexState.String && singleQuotes) {
            state = LexState.Normal;
          } else if (state == LexState.Escape && singleQuotes) {
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
            multipleQueriesPrepare = false;
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
            multipleQueriesPrepare = true;
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

    return new ClientParser(
        queryString, partList, reWritablePrepare, multipleQueriesPrepare, false);
  }

  public String getSql() {
    return sql;
  }

  public List<byte[]> getQueryParts() {
    return queryParts;
  }

  public boolean isQueryMultiValuesRewritable() {
    return isQueryMultiValuesRewritable;
  }

  public boolean isQueryMultipleRewritable() {
    return isQueryMultipleRewritable;
  }

  public boolean isRewriteType() {
    return rewriteType;
  }

  public int getParamCount() {
    return paramCount;
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
