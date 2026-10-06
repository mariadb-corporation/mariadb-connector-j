// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Client-side parsed query: parameter positions and the properties needed to rewrite it.
 *
 * @param sql original query
 * @param query query bytes
 * @param paramPositions positions of the parameter placeholders in {@code query}
 * @param valuesBracketPositions positions of the VALUES bracket, for batch rewriting
 * @param paramCount number of parameters
 * @param isInsert whether the query is an INSERT
 * @param isInsertDuplicate whether the query has an ON DUPLICATE KEY UPDATE clause
 * @param isMultiQuery whether the query contains several statements
 */
public record ClientParser(
    String sql,
    byte[] query,
    int[] paramPositions,
    List<Integer> valuesBracketPositions,
    int paramCount,
    boolean isInsert,
    boolean isInsertDuplicate,
    boolean isMultiQuery)
    implements PrepareResult {

  /**
   * Bytes having a meaning for {@link #parameterParts(String, boolean)} when outside strings and
   * comments, before / after the INSERT keyword is found.
   */
  private static final boolean[] NORMAL_SPECIAL_BEFORE_INSERT = new boolean[256];

  private static final boolean[] NORMAL_SPECIAL_AFTER_INSERT = new boolean[256];

  /**
   * Bytes having a meaning for {@link #rewritableParts(String, boolean)} when outside strings and
   * comments. Index : 1 if INSERT is found ('D' then matters, 'I' does not anymore) + 2 if the
   * VALUES opening bracket is found ('S' then matters, 'V' does not anymore).
   */
  private static final boolean[][] REWRITABLE_SPECIAL = new boolean[4][256];

  static {
    for (char c : new char[] {'*', ';', '#', '-', '"', '\'', '?', '`'}) {
      NORMAL_SPECIAL_BEFORE_INSERT[c] = true;
      NORMAL_SPECIAL_AFTER_INSERT[c] = true;
    }
    NORMAL_SPECIAL_BEFORE_INSERT['I'] = true;
    NORMAL_SPECIAL_BEFORE_INSERT['i'] = true;
    NORMAL_SPECIAL_AFTER_INSERT['D'] = true;
    NORMAL_SPECIAL_AFTER_INSERT['d'] = true;
    for (int state = 0; state < 4; state++) {
      String keywords = ((state & 1) == 0 ? "Ii" : "Dd") + ((state & 2) == 0 ? "Vv" : "Ss");
      for (char c : ("*;#-\"'?`Ll()" + keywords).toCharArray()) {
        REWRITABLE_SPECIAL[state][c] = true;
      }
    }
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

    int[] paramPositions = new int[16];
    int paramCount = 0;
    boolean isInsert = false;
    boolean isInsertDuplicate = false;
    int multiQueryIdx = -1;
    byte[] query = queryString.getBytes(StandardCharsets.UTF_8);
    int queryLength = query.length;

    boolean[] special = NORMAL_SPECIAL_BEFORE_INSERT;
    // the byte before index i is only looked at ("/*", "--") from this index
    int lookBehindFrom = 1;
    int i = 0;

    while (i < queryLength) {
      byte car = query[i];
      if (!special[car & 0xFF]) {
        i++;
        continue;
      }
      switch (car) {
        case '?':
          if (paramCount == paramPositions.length) {
            paramPositions = Arrays.copyOf(paramPositions, paramCount * 2);
          }
          paramPositions[paramCount++] = i;
          break;

        case '\'':
        case '"':
          // string: skip to the closing quote, a backslash escaping the next byte
          i++;
          while (i < queryLength && query[i] != car) {
            if (query[i] == '\\' && !noBackslashEscapes) i++;
            i++;
          }
          break;

        case '`':
          i++;
          while (i < queryLength && query[i] != '`') i++;
          break;

        case '#':
          i++;
          while (i < queryLength && query[i] != '\n') i++;
          break;

        case '-':
          if (i >= lookBehindFrom && query[i - 1] == '-') {
            // '--' starts a comment only if followed by whitespace or control character
            // (not in expressions like '2--1'), or at end of query
            if (i + 1 >= queryLength || (query[i + 1] >= 0x00 && query[i + 1] <= ' ')) {
              i++;
              while (i < queryLength && query[i] != '\n') i++;
            }
          }
          break;

        case '*':
          if (i >= lookBehindFrom && query[i - 1] == '/') {
            // comment: skip to the first '/' preceded by '*'
            i++;
            while (i < queryLength && !(query[i] == '/' && query[i - 1] == '*')) i++;
            lookBehindFrom = i + 2;
          }
          break;

        case ';':
          if (multiQueryIdx == -1) {
            multiQueryIdx = i;
          }
          break;

        case 'I':
        case 'i':
          if (isKeyword(query, i, INSERT)) {
            i += 5;
            lookBehindFrom = i + 2;
            isInsert = true;
            special = NORMAL_SPECIAL_AFTER_INSERT;
          }
          break;

        case 'D':
        case 'd':
          if (isKeyword(query, i, DUPLICATE)) {
            i += 9;
            lookBehindFrom = i + 2;
            isInsertDuplicate = true;
          }
          break;
      }
      i++;
    }

    // multi contains ";" not finishing statement.
    boolean isMulti = multiQueryIdx != -1 && multiQueryIdx < queryLength - 1;
    if (isMulti) {
      // ensure there is not only empty
      boolean hasAdditionalPart = false;
      for (int j = multiQueryIdx + 1; j < queryLength; j++) {
        if (!isWhitespace(query[j])) {
          hasAdditionalPart = true;
          break;
        }
      }
      isMulti = hasAdditionalPart;
    }
    return new ClientParser(
        queryString,
        query,
        Arrays.copyOf(paramPositions, paramCount),
        null,
        paramCount,
        isInsert,
        isInsertDuplicate,
        isMulti);
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
    int[] paramPositions = new int[16];
    int paramCount = 0;
    List<Integer> valuesBracketPositions = new ArrayList<>(2);

    boolean isInsert = false;
    boolean isInsertDuplicate = false;
    boolean afterValues = false;
    boolean valuesClosed = false;
    int isInParenthesis = 0;
    int multiQueryIdx = -1;
    byte[] query = queryString.getBytes(StandardCharsets.UTF_8);
    int queryLength = query.length;

    // the byte before index i is query[i - 1] from this index, lookBehindOverride before
    int lookBehindFrom = 1;
    byte lookBehindOverride = 0;
    int i = 0;

    // bytes without any meaning outside strings and comments are skipped in a tight loop
    boolean[] special = REWRITABLE_SPECIAL[0];
    while (true) {
      while (i < queryLength && !special[query[i] & 0xFF]) i++;
      if (i >= queryLength) break;
      byte car = query[i];
      switch (car) {
        case '?':
          if (paramCount == paramPositions.length) {
            paramPositions = Arrays.copyOf(paramPositions, paramCount * 2);
          }
          paramPositions[paramCount++] = i;
          // have parameter outside values parenthesis
          if (valuesClosed) {
            reWritablePrepare = false;
          }
          break;

        case '\'':
        case '"':
          // string: skip to the closing quote, a backslash escaping the next byte
          i++;
          while (i < queryLength && query[i] != car) {
            if (query[i] == '\\' && !noBackslashEscapes) i++;
            i++;
          }
          break;

        case '`':
          i++;
          while (i < queryLength && query[i] != '`') i++;
          break;

        case '#':
          i++;
          while (i < queryLength && query[i] != '\n') i++;
          break;

        case '-':
          if ((i >= lookBehindFrom ? query[i - 1] : lookBehindOverride) == '-') {
            // '--' starts a comment only if followed by whitespace or control character
            // (not in expressions like '2--1'), or at end of query
            if (i + 1 >= queryLength || (query[i + 1] >= 0x00 && query[i + 1] <= ' ')) {
              i++;
              while (i < queryLength && query[i] != '\n') i++;
            }
          }
          break;

        case '*':
          if ((i >= lookBehindFrom ? query[i - 1] : lookBehindOverride) == '/') {
            // comment: skip to the first '/' preceded by '*'
            i++;
            while (i < queryLength && !(query[i] == '/' && query[i - 1] == '*')) i++;
            lookBehindFrom = i + 2;
            lookBehindOverride = 0;
          }
          break;

        case ';':
          if (multiQueryIdx == -1) {
            multiQueryIdx = i;
          }
          break;

        case 'I':
        case 'i':
          if (!isInsert && isKeyword(query, i, INSERT)) {
            i += 5;
            lookBehindFrom = i + 2;
            lookBehindOverride = car;
            isInsert = true;
            special = REWRITABLE_SPECIAL[valuesBracketPositions.isEmpty() ? 1 : 3];
          }
          break;
        case 'D':
        case 'd':
          if (isInsert && isKeyword(query, i, DUPLICATE)) {
            i += 9;
            lookBehindFrom = i + 2;
            lookBehindOverride = car;
            isInsertDuplicate = true;
          }
          break;
        case 's':
        case 'S':
          // field/table name might contain 'select'
          if (!valuesBracketPositions.isEmpty()
              && queryLength > i + 7
              && isKeyword(query, i, SELECT)) {
            // SELECT queries, INSERT FROM SELECT not rewritable
            reWritablePrepare = false;
          }
          break;
        case 'v':
        case 'V':
          // previous byte must be ')' or a separator
          if (valuesBracketPositions.isEmpty()
              && ((i >= lookBehindFrom ? query[i - 1] : lookBehindOverride) & 0xFF) <= ')'
              && queryLength > i + 7
              && startsWithIgnoreCase(query, i, VALUES)
              && (query[i + 6] & 0xFF) <= '(') {
            afterValues = true;
            if (query[i + 6] == '(') {
              valuesBracketPositions.add(i + 6);
              special = REWRITABLE_SPECIAL[isInsert ? 3 : 2];
            }
            i = i + 5;
            lookBehindFrom = i + 2;
            lookBehindOverride = car;
          }
          break;
        case 'l':
        case 'L':
          if (queryLength > i + 14 && isLastInsertIdCall(query, i)) {
            reWritablePrepare = false;
          }
          break;
        case '(':
          isInParenthesis++;
          if (afterValues && valuesBracketPositions.isEmpty()) {
            valuesBracketPositions.add(i);
            special = REWRITABLE_SPECIAL[isInsert ? 3 : 2];
          }
          break;
        case ')':
          isInParenthesis--;
          if (afterValues
              && !valuesClosed
              && isInParenthesis == 0
              && valuesBracketPositions.size() == 1) {
            // This is the closing parenthesis of a VALUES tuple.
            // Determine if VALUES contains multiple tuples
            // or if this closes the (single) VALUES tuple list.
            int j = skipBlanksAndComments(query, i + 1);

            if (j < queryLength && query[j] == ',') {
              // VALUES contains multiple tuples. Keep parsing until the last tuple closes.
            } else {
              valuesBracketPositions.add(i);
              valuesClosed = true;
            }
          }
          break;
      }
      i++;
    }

    // multi contains ";" not finishing statement.
    boolean isMulti = multiQueryIdx != -1 && multiQueryIdx < queryLength - 1;
    if (isMulti) {
      // ensure there is not only empty
      boolean hasAdditionalPart = false;
      for (int j = multiQueryIdx + 1; j < queryLength; j++) {
        if (!isWhitespace(query[j])) {
          hasAdditionalPart = true;
          break;
        }
      }
      isMulti = hasAdditionalPart;
    }

    if (isMulti || !isInsert || !reWritablePrepare || valuesBracketPositions.size() != 2) {
      valuesBracketPositions = null;
    }

    return new ClientParser(
        queryString,
        query,
        Arrays.copyOf(paramPositions, paramCount),
        valuesBracketPositions,
        paramCount,
        isInsert,
        isInsertDuplicate,
        isMulti);
  }

  private static final byte[] INSERT = {'i', 'n', 's', 'e', 'r', 't'};
  private static final byte[] DUPLICATE = {'d', 'u', 'p', 'l', 'i', 'c', 'a', 't', 'e'};
  private static final byte[] SELECT = {'s', 'e', 'l', 'e', 'c', 't'};
  private static final byte[] VALUES = {'v', 'a', 'l', 'u', 'e', 's'};

  /**
   * Indicate if the query has, at index pos, the given keyword (any case) followed by at least
   * one byte.
   */
  private static boolean startsWithIgnoreCase(byte[] query, int pos, byte[] lowerKeyword) {
    if (pos + lowerKeyword.length >= query.length) return false;
    // first byte is already known to match
    for (int k = 1; k < lowerKeyword.length; k++) {
      if (!equalsIgnoreCase(query[pos + k], lowerKeyword[k])) return false;
    }
    return true;
  }

  /**
   * Indicate if the query has, at index pos, the given keyword (any case), not being part of a
   * longer name : it must be preceded (but at query start) and followed by a blank or a delimiter.
   */
  private static boolean isKeyword(byte[] query, int pos, byte[] lowerKeyword) {
    if (!startsWithIgnoreCase(query, pos, lowerKeyword)) return false;
    if (pos > 0 && ((query[pos - 1] & 0xFF) > ' ' && !isDelimiter(query[pos - 1]))) {
      return false;
    }
    byte next = query[pos + lowerKeyword.length];
    return (next & 0xFF) <= ' ' || isDelimiter(next);
  }

  /** Indicate if the query has "last_insert_id(" (any case) at index pos. */
  private static boolean isLastInsertIdCall(byte[] query, int i) {
    return equalsIgnoreCase(query[i + 1], (byte) 'a')
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
        && query[i + 14] == '(';
  }

  /** Index of the first byte, from index j, that is neither a blank nor in a comment. */
  private static int skipBlanksAndComments(byte[] query, int j) {
    int queryLength = query.length;
    while (j < queryLength) {
      byte c = query[j];
      if (isWhitespace(c)) {
        j++;
        continue;
      }
      // skip comments
      if (c == '#') {
        j++;
        while (j < queryLength && query[j] != '\n') {
          j++;
        }
        continue;
      }
      if (c == '-' && j + 1 < queryLength && query[j + 1] == '-') {
        j += 2;
        while (j < queryLength && query[j] != '\n') {
          j++;
        }
        continue;
      }
      if (c == '/' && j + 1 < queryLength && query[j + 1] == '*') {
        j += 2;
        while (j + 1 < queryLength && !(query[j] == '*' && query[j + 1] == '/')) {
          j++;
        }
        j = Math.min(j + 2, queryLength);
        continue;
      }
      break;
    }
    return j;
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
}
