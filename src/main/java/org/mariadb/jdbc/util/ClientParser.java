// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class ClientParser implements PrepareResult {

  private final String sql;
  private final byte[] query;
  private final List<Integer> paramPositions;
  private final List<Integer> valuesBracketPositions;
  private final int paramCount;
  private final boolean isInsert;
  private final boolean isInsertDuplicate;
  private final boolean isMultiQuery;

  private ClientParser(
      String sql,
      byte[] query,
      List<Integer> paramPositions,
      List<Integer> valuesBracketPositions,
      boolean isInsert,
      boolean isInsertDuplicate,
      boolean isMultiQuery) {
    this.sql = sql;
    this.query = query;
    this.paramPositions = paramPositions;
    this.valuesBracketPositions = valuesBracketPositions;
    this.paramCount = paramPositions.size();
    this.isInsert = isInsert;
    this.isInsertDuplicate = isInsertDuplicate;
    this.isMultiQuery = isMultiQuery;
  }

  /**
   * For a given <code>queryString</code>, get
   *
   * <ul>
   *   <li>query - a byte array containing the UTF8 representation of that string
   *   <li>paramPositions - the byte positions of any '?' positional parameters in <code>query
   *       </code>
   * </ul>
   *
   * and set the following flags:
   *
   * <ul>
   *   <li>isInsert - queryString contains 'INSERT' outside of quotes, without one of the characters
   *       '();><=-+,' immediately preceding or following
   *   <li>isInsertDuplicate - isInsert && queryString contains 'DUPLICATE' outside of quotes,
   *       without one of the characters '();><=-+,' immediately preceding or following
   *   <li>isMulti - queryString contains text after the last ';' outside of quotes
   *   <li>
   *
   * @param queryString query
   * @param noBackslashEscapes escape mode
   * @return ClientParser
   */
  public static ClientParser parameterParts(String queryString, boolean noBackslashEscapes) {

    List<Integer> paramPositions = new ArrayList<>();
    LexState state = LexState.Normal;
    byte lastChar = 0x00;

    boolean singleQuotes = false;
    boolean isInsert = false;
    boolean isInsertDuplicate = false;
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
            isInsertDuplicate = true;
          }
          break;

        case (byte) '\\':
          if (state == LexState.String && !noBackslashEscapes) {
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
        queryString, query, paramPositions, null, isInsert, isInsertDuplicate, isMulti);
  }

  /**
   * For a given <code>queryString</code>, get the fields and flags from {@link
   * #parameterParts(String, boolean)}, and
   *
   * <ul>
   *   <li>valuesBracketPositions - a two-element list containing the positions of the opening and
   *       closing parenthesis of the VALUES block
   * </ul>
   *
   * @param queryString query
   * @param noBackslashEscapes escape mode
   * @return ClientParser
   */
  public static ClientParser rewritableParts(String queryString, boolean noBackslashEscapes) {
    boolean reWritablePrepare = true;
    List<Integer> paramPositions = new ArrayList<>();
    List<Integer> valuesBracketPositions = new ArrayList<>();

    LexState state = LexState.Normal;
    byte lastChar = 0x00;

    boolean singleQuotes = false;
    boolean isInsert = false;
    boolean isInsertDuplicate = false;
    boolean afterValues = false;
    int isInParenthesis = 0;
    int lastParenthesisPosition = 0;
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
            isInsertDuplicate = true;
          }
          break;
        case 's':
        case 'S':
          if (state == LexState.Normal
              && !valuesBracketPositions.isEmpty()
              && queryLength > i + 7
              && (query[i + 1] == 'e' || query[i + 1] == 'E')
              && (query[i + 2] == 'l' || query[i + 2] == 'L')
              && (query[i + 3] == 'e' || query[i + 3] == 'E')
              && (query[i + 4] == 'c' || query[i + 4] == 'C')
              && (query[i + 5] == 't' || query[i + 5] == 'T')) {

            // field/table name might contain 'select'
            if (i > 0 && (query[i - 1] > ' ' && "();><=-+,".indexOf(query[i - 1]) == -1)) {
              break;
            }
            if (query[i + 6] > ' ' && "();><=-+,".indexOf(query[i + 6]) == -1) {
              break;
            }

            // SELECT queries, INSERT FROM SELECT not rewritable
            reWritablePrepare = false;
          }
          break;
        case 'v':
        case 'V':
          if (state == LexState.Normal
              && valuesBracketPositions.isEmpty()
              && (lastChar == ')' || ((byte) lastChar <= 40))
              && queryLength > i + 7
              && (query[i + 1] == 'a' || query[i + 1] == 'A')
              && (query[i + 2] == 'l' || query[i + 2] == 'L')
              && (query[i + 3] == 'u' || query[i + 3] == 'U')
              && (query[i + 4] == 'e' || query[i + 4] == 'E')
              && (query[i + 5] == 's' || query[i + 5] == 'S')
              && (query[i + 6] == '(' || ((byte) query[i + 6] <= 40))) {
            afterValues = true;
            if (query[i + 6] == '(') {
              valuesBracketPositions.add(i + 6);
            }
            i = i + 5;
          }
          break;
        case 'l':
        case 'L':
          if (state == LexState.Normal
              && queryLength > i + 14
              && (query[i + 1] == 'a' || query[i + 1] == 'A')
              && (query[i + 2] == 's' || query[i + 2] == 'S')
              && (query[i + 3] == 't' || query[i + 3] == 'T')
              && query[i + 4] == '_'
              && (query[i + 5] == 'i' || query[i + 5] == 'I')
              && (query[i + 6] == 'n' || query[i + 6] == 'N')
              && (query[i + 7] == 's' || query[i + 7] == 'S')
              && (query[i + 8] == 'e' || query[i + 8] == 'E')
              && (query[i + 9] == 'r' || query[i + 9] == 'R')
              && (query[i + 10] == 't' || query[i + 10] == 'T')
              && query[i + 11] == '_'
              && (query[i + 12] == 'i' || query[i + 12] == 'I')
              && (query[i + 13] == 'd' || query[i + 13] == 'D')
              && query[i + 14] == '(') {
            reWritablePrepare = false;
          }
          break;
        case '(':
          if (state == LexState.Normal) {
            isInParenthesis++;
            if (afterValues == true && valuesBracketPositions.isEmpty()) {;
              valuesBracketPositions.add(i);
            }
          }
          break;
        case (byte) '\\':
          if (state == LexState.String && !noBackslashEscapes) {
            state = LexState.Escape;
          }
          break;
        case ')':
          if (state == LexState.Normal) {
            isInParenthesis--;
            if (isInParenthesis == 0 && valuesBracketPositions.size() == 1) {
              lastParenthesisPosition = i;
            }
          }
          break;
        case (byte) '?':
          if (state == LexState.Normal) {
            paramPositions.add(i);
            // have parameter outside values parenthesis
            if (valuesBracketPositions.size() == 1 && lastParenthesisPosition > 0 && isInParenthesis == 0) {
              reWritablePrepare = false;
            }
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

    if (valuesBracketPositions.size() == 1 && lastParenthesisPosition > 0) {
      valuesBracketPositions.add(lastParenthesisPosition);
    }

    if (isMulti || !isInsert || !reWritablePrepare || valuesBracketPositions.size() != 2) {
      valuesBracketPositions = null;
    }

    return new ClientParser(
        queryString,
        query,
        paramPositions,
        valuesBracketPositions,
        isInsert,
        isInsertDuplicate,
        isMulti);
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

  public List<Integer> getValuesBracketPositions() {
    return valuesBracketPositions;
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
    Normal, /* inside query */
    String, /* inside string */
    SlashStarComment, /* inside slash-star comment */
    Escape, /* found backslash */
    EOLComment, /* # comment, or // comment, or -- comment */
    Backtick /* found backtick */
  }
}
