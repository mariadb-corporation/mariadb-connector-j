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

    List<Integer> paramPositions = new ArrayList<>(20);
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
        case '*':
          if (state == LexState.Normal && lastChar == '/') {
            state = LexState.SlashStarComment;
          }
          break;

        case '/':
          if (state == LexState.SlashStarComment && lastChar == '*') {
            state = LexState.Normal;
          }
          break;

        case ';':
          if (state == LexState.Normal && multiQueryIdx == -1) {
            multiQueryIdx = i;
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

        case 'I':
        case 'i':
          if (state == LexState.Normal
              && !isInsert
              && i + 6 < queryLength
              && equalsIgnoreCase(query[i + 1], (byte) 'n')
              && equalsIgnoreCase(query[i + 2], (byte) 's')
              && equalsIgnoreCase(query[i + 3], (byte) 'e')
              && equalsIgnoreCase(query[i + 4], (byte) 'r')
              && equalsIgnoreCase(query[i + 5], (byte) 't')) {
            if (i > 0 && (query[i - 1] > ' ' && !isDelimiter(query[i - 1]))) {
              break;
            }
            if (query[i + 6] > ' ' && !isDelimiter(query[i + 6])) {
              break;
            }
            i += 5;
            isInsert = true;
          }
          break;
        case 'D':
        case 'd':
          if (isInsert
              && state == LexState.Normal
              && i + 9 < queryLength
              && equalsIgnoreCase(query[i + 1], (byte) 'u')
              && equalsIgnoreCase(query[i + 2], (byte) 'p')
              && equalsIgnoreCase(query[i + 3], (byte) 'l')
              && equalsIgnoreCase(query[i + 4], (byte) 'i')
              && equalsIgnoreCase(query[i + 5], (byte) 'c')
              && equalsIgnoreCase(query[i + 6], (byte) 'a')
              && equalsIgnoreCase(query[i + 7], (byte) 't')
              && equalsIgnoreCase(query[i + 8], (byte) 'e')) {
            if (i > 0 && (query[i - 1] > ' ' && !isDelimiter(query[i - 1]))) {
              break;
            }
            if (query[i + 9] > ' ' && !isDelimiter(query[i + 9])) {
              break;
            }
            i += 9;
            isInsertDuplicate = true;
          }
          break;

        case '\\':
          if (state == LexState.String && !noBackslashEscapes) {
            state = LexState.Escape;
          }
          break;
        case '?':
          if (state == LexState.Normal) {
            paramPositions.add(i);
          }
          break;
        case '`':
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
        if (!isWhitespace(query[i])) {
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
    List<Integer> paramPositions = new ArrayList<>(20);
    List<Integer> valuesBracketPositions = new ArrayList<>(2);

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
        case '*':
          if (state == LexState.Normal && lastChar == '/') {
            state = LexState.SlashStarComment;
          }
          break;

        case '/':
          if (state == LexState.SlashStarComment && lastChar == '*') {
            state = LexState.Normal;
          }
          break;

        case ';':
          if (state == LexState.Normal && multiQueryIdx == -1) {
            multiQueryIdx = i;
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

        case 'I':
        case 'i':
          if (state == LexState.Normal
              && !isInsert
              && i + 6 < queryLength
              && equalsIgnoreCase(query[i + 1], (byte) 'n')
              && equalsIgnoreCase(query[i + 2], (byte) 's')
              && equalsIgnoreCase(query[i + 3], (byte) 'e')
              && equalsIgnoreCase(query[i + 4], (byte) 'r')
              && equalsIgnoreCase(query[i + 5], (byte) 't')) {
            if (i > 0 && (query[i - 1] > ' ' && !isDelimiter(query[i - 1]))) {
              break;
            }
            if (query[i + 6] > ' ' && !isDelimiter(query[i + 6])) {
              break;
            }
            i += 5;
            isInsert = true;
          }
          break;
        case 'D':
        case 'd':
          if (isInsert
              && state == LexState.Normal
              && i + 9 < queryLength
              && equalsIgnoreCase(query[i + 1], (byte) 'u')
              && equalsIgnoreCase(query[i + 2], (byte) 'p')
              && equalsIgnoreCase(query[i + 3], (byte) 'l')
              && equalsIgnoreCase(query[i + 4], (byte) 'i')
              && equalsIgnoreCase(query[i + 5], (byte) 'c')
              && equalsIgnoreCase(query[i + 6], (byte) 'a')
              && equalsIgnoreCase(query[i + 7], (byte) 't')
              && equalsIgnoreCase(query[i + 8], (byte) 'e')) {
            if (i > 0 && (query[i - 1] > ' ' && !isDelimiter(query[i - 1]))) {
              break;
            }
            if (query[i + 9] > ' ' && !isDelimiter(query[i + 9])) {
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
              && equalsIgnoreCase(query[i + 1], (byte) 'e')
              && equalsIgnoreCase(query[i + 2], (byte) 'l')
              && equalsIgnoreCase(query[i + 3], (byte) 'e')
              && equalsIgnoreCase(query[i + 4], (byte) 'c')
              && equalsIgnoreCase(query[i + 5], (byte) 't')) {

            // field/table name might contain 'select'
            if (i > 0 && (query[i - 1] > ' ' && !isDelimiter(query[i - 1]))) {
              break;
            }
            if (query[i + 6] > ' ' && !isDelimiter(query[i + 6])) {
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
              && equalsIgnoreCase(query[i + 1], (byte) 'a')
              && equalsIgnoreCase(query[i + 2], (byte) 'l')
              && equalsIgnoreCase(query[i + 3], (byte) 'u')
              && equalsIgnoreCase(query[i + 4], (byte) 'e')
              && equalsIgnoreCase(query[i + 5], (byte) 's')
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
              && equalsIgnoreCase(query[i + 1], (byte) 'a')
              && equalsIgnoreCase(query[i + 2], (byte) 's')
              && equalsIgnoreCase(query[i + 3], (byte) 't')
              && query[i + 4] == '_'
              && equalsIgnoreCase(query[i + 5], (byte) 'i')
              && equalsIgnoreCase(query[i + 6], (byte) 'n')
              && equalsIgnoreCase(query[i + 7], (byte) 's')
              && equalsIgnoreCase(query[i + 8], (byte) 'e')
              && equalsIgnoreCase(query[i + 9], (byte) 'r')
              && equalsIgnoreCase(query[i + 10], (byte) 't')
              && query[i + 11] == '_'
              && equalsIgnoreCase(query[i + 12], (byte) 'i')
              && equalsIgnoreCase(query[i + 13], (byte) 'd')
              && query[i + 14] == '(') {
            reWritablePrepare = false;
          }
          break;
        case '(':
          if (state == LexState.Normal) {
            isInParenthesis++;
            if (afterValues && valuesBracketPositions.isEmpty()) {
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
            if (valuesBracketPositions.size() == 1
                && lastParenthesisPosition > 0
                && isInParenthesis == 0) {
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

  /** Fast check if byte is a delimiter character: ();><=-+, Avoids String.indexOf() overhead */
  private static boolean isDelimiter(byte b) {
    return b == '(' || b == ')' || b == ';' || b == '>' || b == '<' || b == '=' || b == '-'
        || b == '+' || b == ',';
  }

  /** Fast check if byte is whitespace: space, \n, \r, \t */
  private static boolean isWhitespace(byte b) {
    return b == ' ' || b == '\n' || b == '\r' || b == '\t';
  }

  /**
   * Fast case-insensitive byte comparison for ASCII letters. Uses bitwise OR to convert to
   * lowercase before comparison.
   */
  private static boolean equalsIgnoreCase(byte b, byte lower) {
    return (b | 0x20) == lower;
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
