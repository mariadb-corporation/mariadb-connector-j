/*
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
 */

package org.mariadb.jdbc.util;

import java.sql.SQLException;
import java.util.Locale;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class NativeSql {

  public static String parse(String sql, Context context) throws SQLException {
    if (!sql.contains("{")) {
      return sql;
    }

    StringBuilder escapeSequenceBuf = new StringBuilder();
    StringBuilder sqlBuffer = new StringBuilder();

    char[] charArray = sql.toCharArray();
    char lastChar = 0;
    boolean inQuote = false;
    char quoteChar = 0;
    boolean inComment = false;
    boolean isSlashSlashComment = false;
    int inEscapeSeq = 0;

    for (char car : charArray) {
      if (lastChar == '\\'
          && (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) == 0) {
        sqlBuffer.append(car);
        // avoid considering escaped backslash as a futur escape character
        lastChar = ' ';
        continue;
      }

      switch (car) {
        case '\'':
        case '"':
        case '`':
          if (!inComment) {
            if (inQuote) {
              if (quoteChar == car) {
                inQuote = false;
              }
            } else {
              inQuote = true;
              quoteChar = car;
            }
          }
          break;

        case '*':
          if (!inQuote && !inComment && lastChar == '/') {
            inComment = true;
            isSlashSlashComment = false;
          }
          break;

        case '-':
        case '/':
          if (!inQuote) {
            if (inComment) {
              if (lastChar == '*' && !isSlashSlashComment) {
                inComment = false;
              }
            } else {
              if (lastChar == car) {
                inComment = true;
                isSlashSlashComment = true;
              }
            }
          }
          break;
        case '\n':
          if (inComment && isSlashSlashComment) {
            // slash-slash and dash-dash comments ends with the end of line
            inComment = false;
          }
          break;
        case '{':
          if (!inQuote && !inComment) {
            inEscapeSeq++;
          }
          break;

        case '}':
          if (!inQuote && !inComment) {
            inEscapeSeq--;
            if (inEscapeSeq == 0) {
              escapeSequenceBuf.append(car);
              sqlBuffer.append(resolveEscapes(escapeSequenceBuf.toString(), context));
              escapeSequenceBuf.setLength(0);
              lastChar = car;
              continue;
            }
          }
          break;

        default:
          break;
      }
      lastChar = car;
      if (inEscapeSeq > 0) {
        escapeSequenceBuf.append(car);
      } else {
        sqlBuffer.append(car);
      }
    }
    if (inEscapeSeq > 0) {
      throw new SQLException(
          "Invalid escape sequence , missing closing '}' character in '" + sqlBuffer);
    }
    return sqlBuffer.toString();
  }

  private static String resolveEscapes(String escaped, Context context) throws SQLException {
    int endIndex = escaped.length() - 1;
    String escapedLower = escaped.toLowerCase(Locale.ROOT);
    if (escaped.startsWith("{fn ")) {
      String resolvedParams = replaceFunctionParameter(escaped.substring(4, endIndex), context);
      return parse(resolvedParams, context);
    } else if (escapedLower.startsWith("{oj ")) {
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
    } else if (escaped.startsWith("{escape ")) {
      return escaped.substring(1, endIndex);
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
