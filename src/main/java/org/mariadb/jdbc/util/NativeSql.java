// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.util;

import java.sql.SQLException;
import java.util.Locale;
import org.mariadb.jdbc.client.Context;

public final class NativeSql {

  /** Characters having a meaning for {@link #parse(String, Context)} outside strings/comments. */
  private static final boolean[] SPECIAL = new boolean[128];

  static {
    for (char c : "*/#-\"'`{}".toCharArray()) {
      SPECIAL[c] = true;
    }
  }

  public static String parse(String sql, Context context) throws SQLException {
    if (!sql.contains("{")) {
      return sql;
    }

    StringBuilder sb = new StringBuilder();
    char[] query = sql.toCharArray();
    int queryLength = query.length;
    int lastEscapePart = 0;
    int escapeIdx = 0;
    boolean inEscape = false;
    // start of the text not yet copied to the result, when not in an escape sequence
    int copyFrom = 0;
    // the character before index idx is query[idx - 1] from this index, lookBehindOverride before
    int lookBehindFrom = 1;
    char lookBehindOverride = '\0';
    int idx = 0;

    // characters without any meaning outside strings and comments are skipped in a tight loop
    while (idx < queryLength) {
      char car = query[idx];
      if (car >= 128 || !SPECIAL[car]) {
        idx++;
        continue;
      }
      switch (car) {
        case '\'':
        case '"':
          // string: skip to the closing quote, a backslash escaping the next character
          idx++;
          while (idx < queryLength && query[idx] != car) {
            if (query[idx] == '\\') idx++;
            idx++;
          }
          break;

        case '`':
          idx++;
          while (idx < queryLength && query[idx] != '`') idx++;
          break;

        case '#':
          idx++;
          while (idx < queryLength && query[idx] != '\n') idx++;
          break;

        case '-':
          if ((idx >= lookBehindFrom ? query[idx - 1] : lookBehindOverride) == '-') {
            idx++;
            while (idx < queryLength && query[idx] != '\n') idx++;
          }
          break;

        case '/':
          if ((idx >= lookBehindFrom ? query[idx - 1] : lookBehindOverride) == '/') {
            idx++;
            while (idx < queryLength && query[idx] != '\n') idx++;
          }
          break;

        case '*':
          if ((idx >= lookBehindFrom ? query[idx - 1] : lookBehindOverride) == '/') {
            // comment: skip to the first '/' preceded by '*'
            idx++;
            while (idx < queryLength && !(query[idx] == '/' && query[idx - 1] == '*')) idx++;
          }
          break;

        case '{':
          if (!inEscape) {
            inEscape = true;
            lastEscapePart = idx;
            sb.append(query, copyFrom, idx - copyFrom);
          }
          escapeIdx++;
          break;

        case '}':
          if (inEscape) {
            escapeIdx--;

            if (escapeIdx == 0) {
              String str = sql.substring(lastEscapePart, idx + 1);
              String escapedSeq = resolveEscapes(str, context);
              sb.append(escapedSeq);
              inEscape = false;
              copyFrom = idx + 1;
              // the closing '}' is not seen as the character before the next one
              lookBehindOverride = idx >= lookBehindFrom ? query[idx - 1] : lookBehindOverride;
              lookBehindFrom = idx + 2;
            }
          }
          break;
      }
      idx++;
    }
    if (inEscape) {
      throw new SQLException(
          "Invalid escape sequence , missing closing '}' character in '" + sql + "'");
    }
    return sb.append(query, copyFrom, queryLength - copyFrom).toString();
  }

  private static String resolveEscapes(String escaped, Context context) throws SQLException {
    int endIndex = escaped.length() - 1;
    if (escaped.startsWith("{fn ")) {
      String resolvedParams = replaceFunctionParameter(escaped.substring(4, endIndex), context);
      return parse(resolvedParams, context);
    } else if (escaped.startsWith("{oj ")) {
      // Outer join
      // the server supports "oj" in any case, even "oJ"
      return parse(escaped.substring(4, endIndex), context);
    } else if (escaped.startsWith("{d ")) {
      // date literal
      return escaped.substring(3, endIndex);
    } else if (escaped.startsWith("{t ")) {
      // time literal
      return escaped.substring(3, endIndex);
    } else if (escaped.startsWith("{ts ")) {
      // timestamp literal
      return escaped.substring(4, endIndex);
    } else if (escaped.startsWith("{d'")) {
      // date literal, no space
      return escaped.substring(2, endIndex);
    } else if (escaped.startsWith("{t'")) {
      // time literal
      return escaped.substring(2, endIndex);
    } else if (escaped.startsWith("{ts'")) {
      // timestamp literal
      return escaped.substring(3, endIndex);
    } else if (escaped.startsWith("{call ") || escaped.startsWith("{CALL ")) {
      // We support uppercase "{CALL" only because Connector/J supports it. It is not in the JDBC
      // spec.
      return parse(escaped.substring(1, endIndex), context);
    } else if (escaped.startsWith("{?")) {
      // likely ?=call(...)
      return parse(escaped.substring(1, endIndex), context);
    } else if (escaped.startsWith("{ ") || escaped.startsWith("{\n")) {
      // Spaces and newlines before keyword, this is not JDBC compliant, however some it works in
      // some drivers,
      // so we support it, too
      for (int i = 2; i < escaped.length(); i++) {
        if (!Character.isWhitespace(escaped.charAt(i))) {
          return resolveEscapes("{" + escaped.substring(i), context);
        }
      }
    } else if (escaped.startsWith("{\r\n")) {
      // Spaces and newlines before keyword, this is not JDBC compliant, however some it works in
      // some drivers,
      // so we support it, too
      for (int i = 3; i < escaped.length(); i++) {
        if (!Character.isWhitespace(escaped.charAt(i))) {
          return resolveEscapes("{" + escaped.substring(i), context);
        }
      }
    }
    throw new SQLException("unknown escape sequence " + escaped);
  }

  /**
   * Helper function to replace function parameters in escaped string. 3 functions are handles :
   *
   * <ul>
   *   <li>CONVERT(value, type): replacing SQL_XXX types to convertible type, i.e SQL_BIGINT to
   *       INTEGER
   *   <li>TIMESTAMPDIFF(type, ...): replacing type SQL_TSI_XXX in type with XXX, i.e SQL_TSI_HOUR
   *       with HOUR
   *   <li>TIMESTAMPADD(type, ...): replacing type SQL_TSI_XXX in type with XXX, i.e SQL_TSI_HOUR
   *       with HOUR
   * </ul>
   *
   * <p>caution: this use MariaDB server conversion: 'SELECT CONVERT('2147483648', INTEGER)' will
   * return a BIGINT. MySQL will throw a syntax error.
   *
   * @param functionString input string
   * @return unescaped string
   */
  private static String replaceFunctionParameter(String functionString, Context context) {

    char[] input = functionString.toCharArray();
    StringBuilder sb = new StringBuilder();
    int index;
    for (index = 0; index < input.length; index++) {
      if (input[index] != ' ') {
        break;
      }
    }

    for (;
        index < input.length
            && ((input[index] >= 'a' && input[index] <= 'z')
                || (input[index] >= 'A' && input[index] <= 'Z'));
        index++) {
      sb.append(input[index]);
    }
    String func = sb.toString().toLowerCase(Locale.ROOT);
    switch (func) {
      case "convert":
        // Handle "convert(value, type)" case
        // extract last parameter, after the last ','
        int lastCommaIndex = functionString.lastIndexOf(',');
        int firstParentheses = functionString.indexOf('(');
        String value = functionString.substring(firstParentheses + 1, lastCommaIndex);
        for (index = lastCommaIndex + 1; index < input.length; index++) {
          if (!Character.isWhitespace(input[index])) {
            break;
          }
        }

        int endParam = index + 1;
        for (; endParam < input.length; endParam++) {
          if ((input[endParam] < 'a' || input[endParam] > 'z')
              && (input[endParam] < 'A' || input[endParam] > 'Z')
              && input[endParam] != '_') {
            break;
          }
        }
        String typeParam = new String(input, index, endParam - index).toUpperCase(Locale.ROOT);
        if (typeParam.startsWith("SQL_")) {
          typeParam = typeParam.substring(4);
        }

        switch (typeParam) {
          case "BOOLEAN":
            return "1=" + value;

          case "BIGINT":
          case "SMALLINT":
          case "TINYINT":
            typeParam = "SIGNED INTEGER";
            break;

          case "BIT":
            typeParam = "UNSIGNED INTEGER";
            break;

          case "BLOB":
          case "VARBINARY":
          case "LONGVARBINARY":
          case "ROWID":
            typeParam = "BINARY";
            break;

          case "NCHAR":
          case "CLOB":
          case "NCLOB":
          case "DATALINK":
          case "VARCHAR":
          case "NVARCHAR":
          case "LONGVARCHAR":
          case "LONGNVARCHAR":
          case "SQLXML":
          case "LONGNCHAR":
            typeParam = "CHAR";
            break;

          case "DOUBLE":
          case "FLOAT":
            if (context.getVersion().isMariaDBServer()
                || context.getVersion().versionGreaterOrEqual(8, 0, 17)) {
              typeParam = "DOUBLE";
              break;
            }
            return "0.0+" + value;

          case "REAL":
          case "NUMERIC":
            typeParam = "DECIMAL";
            break;

          case "TIMESTAMP":
            typeParam = "DATETIME";
            break;

          default:
            break;
        }
        return new String(input, 0, index)
            + typeParam
            + new String(input, endParam, input.length - endParam);

      case "timestampdiff":
      case "timestampadd":
        // Skip to first parameter
        for (; index < input.length; index++) {
          if (!Character.isWhitespace(input[index]) && input[index] != '(') {
            break;
          }
        }
        if (index < input.length - 8) {
          String paramPrefix = new String(input, index, 8);
          if ("SQL_TSI_".equals(paramPrefix)) {
            return new String(input, 0, index)
                + new String(input, index + 8, input.length - (index + 8));
          }
        }
        return functionString;

      default:
        return functionString;
    }
  }
}
