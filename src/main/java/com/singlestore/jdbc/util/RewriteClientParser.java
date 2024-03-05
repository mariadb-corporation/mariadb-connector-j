// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
// Copyright (c) 2021-2024 SingleStore, Inc.

package com.singlestore.jdbc.util;

import com.singlestore.jdbc.util.ClientParser.LexState;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RewriteClientParser implements PrepareResult {

  private final String sql;
  private final List<byte[]> queryParts;
  private final int paramCount;
  private final int queryPartsLength;
  private final boolean isQueryMultiValuesRewritable;

  private RewriteClientParser(
      String sql,
      List<byte[]> queryParts,
      int queryPartsLength,
      boolean isQueryMultiValuesRewritable) {
    this.sql = sql;
    this.queryParts = queryParts;
    this.queryPartsLength = queryPartsLength;
    this.isQueryMultiValuesRewritable = isQueryMultiValuesRewritable;
    this.paramCount = queryParts.size() - 3;
  }

  /**
   * Separate query in a String list and set flag isQueryMultiValuesRewritable The parameters "?"
   * (not in comments) emplacements are to be known.
   *
   * <p>The only rewritten queries follow these notation: INSERT [LOW_PRIORITY | DELAYED |
   * HIGH_PRIORITY] [IGNORE] [INTO] tbl_name [PARTITION (partition_list)] [(col,...)] {VALUES |
   * VALUE} (...) [ ON DUPLICATE KEY UPDATE col=expr [, col=expr] ... ] With expr without parameter.
   *
   * <p>Query with LAST_INSERT_ID() will not be rewritten
   *
   * <p>INSERT ... SELECT will not be rewritten.
   *
   * <p>String list :
   *
   * <ul>
   *   <li>pre value part
   *   <li>After value and first parameter part
   *   <li>for each parameters :
   *       <ul>
   *         <li>part after parameter and before last parenthesis
   *         <li>Last query part
   *       </ul>
   * </ul>
   *
   * <p>example : INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ?, 5, ?, 8) ON DUPLICATE
   * KEY UPDATE col2=col2+10
   *
   * <ul>
   *   <li>pre value part : INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES
   *   <li>after value part : "(9 "
   *   <li>part after parameter 1: ", 5," - ", 5," - ",8)"
   *   <li>last part : ON DUPLICATE KEY UPDATE col2=col2+10
   * </ul>
   *
   * <p>With 2 series of parameters, this query will be rewritten like [INSERT INTO
   * TABLE(col1,col2,col3,col4, col5) VALUES][ (9, param0_1, 5, param0_2, 8)][, (9, param1_1, 5,
   * param1_2, 8)][ ON DUPLICATE KEY UPDATE col2=col2+10]
   *
   * @param queryString query String
   * @param noBackslashEscapes must backslash be escaped.
   * @return List of query part.
   */
  public static RewriteClientParser rewritableParts(
      String queryString, boolean noBackslashEscapes) {
    boolean reWritablePrepare = true;
    List<byte[]> partList = new ArrayList<>();
    LexState state = LexState.Normal;
    char lastChar = '\0';

    StringBuilder sb = new StringBuilder();

    String preValuePart1 = null;
    String preValuePart2 = null;
    String postValuePart = null;

    boolean singleQuotes = false;

    int isInParenthesis = 0;
    boolean skipChar = false;
    boolean isFirstChar = true;
    boolean isInsert = false;
    boolean isReplace = false;
    boolean semicolon = false;
    boolean hasParam = false;

    char[] query = queryString.toCharArray();
    int queryLength = query.length;
    for (int i = 0; i < queryLength; i++) {

      char car = query[i];
      if (state == LexState.Escape
          && !((car == '\'' && singleQuotes) || (car == '"' && !singleQuotes))) {
        sb.append(car);
        lastChar = car;
        state = LexState.String;
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
          } else if (state == LexState.Escape && !singleQuotes) {
            state = LexState.String;
          }
          break;
        case ';':
          if (state == LexState.Normal) {
            semicolon = true;
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

        case '?':
          if (state == LexState.Normal) {
            hasParam = true;
            if (preValuePart1 == null) {
              preValuePart1 = sb.toString();
              sb.setLength(0);
            }
            if (preValuePart2 == null) {
              preValuePart2 = sb.toString();
              sb.setLength(0);
            } else {
              if (postValuePart != null) {
                // having parameters after the last ")" of value is not rewritable
                reWritablePrepare = false;

                // add part
                sb.insert(0, postValuePart);
                postValuePart = null;
              }
              partList.add(sb.toString().getBytes(StandardCharsets.UTF_8));
              sb.setLength(0);
            }

            skipChar = true;
          }
          break;
        case '`':
          if (state == LexState.Backtick) {
            state = LexState.Normal;
          } else if (state == LexState.Normal) {
            state = LexState.Backtick;
          }
          break;

        case 's':
        case 'S':
          if (state == LexState.Normal
              && postValuePart == null
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
              && preValuePart1 == null
              && (lastChar == ')' || ((byte) lastChar <= 40))
              && queryLength > i + 7
              && (query[i + 1] == 'a' || query[i + 1] == 'A')
              && (query[i + 2] == 'l' || query[i + 2] == 'L')
              && (query[i + 3] == 'u' || query[i + 3] == 'U')
              && (query[i + 4] == 'e' || query[i + 4] == 'E')
              && (query[i + 5] == 's' || query[i + 5] == 'S')
              && (query[i + 6] == '(' || ((byte) query[i + 6] <= 40))) {
            sb.append(car);
            sb.append(query[i + 1]);
            sb.append(query[i + 2]);
            sb.append(query[i + 3]);
            sb.append(query[i + 4]);
            sb.append(query[i + 5]);
            i = i + 5;
            preValuePart1 = sb.toString();
            sb.setLength(0);
            skipChar = true;
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
            sb.append(car);
            reWritablePrepare = false;
            skipChar = true;
          }
          break;
        case '(':
          if (state == LexState.Normal) {
            isInParenthesis++;
          }
          break;
        case ')':
          if (state == LexState.Normal) {
            isInParenthesis--;
            if (isInParenthesis == 0 && preValuePart2 != null && postValuePart == null) {
              sb.append(car);
              postValuePart = sb.toString();
              sb.setLength(0);
              skipChar = true;
            }
          }
          break;
        default:
          if (state == LexState.Normal && isFirstChar && ((byte) car >= 40)) {
            if (car == 'I' || car == 'i') {
              isInsert = true;
            } else if (car == 'R' || car == 'r') {
              isReplace = true;
            }
            isFirstChar = false;
          }
          // multiple queries
          if (state == LexState.Normal && semicolon && ((byte) car >= 40)) {
            reWritablePrepare = false;
          }
          break;
      }

      lastChar = car;
      if (skipChar) {
        skipChar = false;
      } else {
        sb.append(car);
      }
    }

    if (!hasParam) {
      // permit to have rewrite without parameter
      if (preValuePart1 == null) {
        partList.add(0, sb.toString().getBytes(StandardCharsets.UTF_8));
        partList.add(1, new byte[0]);
      } else {
        partList.add(0, preValuePart1.getBytes(StandardCharsets.UTF_8));
        partList.add(1, sb.toString().getBytes(StandardCharsets.UTF_8));
      }
      sb.setLength(0);
    } else {
      partList.add(
          0,
          (preValuePart1 == null) ? new byte[0] : preValuePart1.getBytes(StandardCharsets.UTF_8));
      partList.add(
          1,
          (preValuePart2 == null) ? new byte[0] : preValuePart2.getBytes(StandardCharsets.UTF_8));
    }

    if (!isInsert && !isReplace) {
      reWritablePrepare = false;
    }

    // postValuePart is the value after the last parameter and parenthesis
    // if no param, don't add to the list.
    if (hasParam) {
      partList.add(
          (postValuePart == null) ? new byte[0] : postValuePart.getBytes(StandardCharsets.UTF_8));
    }
    partList.add(sb.toString().getBytes(StandardCharsets.UTF_8));

    int staticLength = 1;
    for (byte[] queryPart : partList) {
      staticLength += queryPart.length;
    }
    return new RewriteClientParser(queryString, partList, staticLength, reWritablePrepare);
  }

  @Override
  public String getSql() {
    return sql;
  }

  @Override
  public int getParamCount() {
    return paramCount;
  }

  public List<byte[]> getQueryParts() {
    return queryParts;
  }

  public boolean isQueryMultiValuesRewritable() {
    return isQueryMultiValuesRewritable;
  }

  public int getQueryPartsLength() {
    return queryPartsLength;
  }
}
