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

public final class Security {

  /**
   * Parse the option "sessionVariable" to ensure having no injection. semi-column not in string
   * will be replaced by comma.
   *
   * @param sessionVariable option value
   * @return parsed String
   */
  public static String parseSessionVariables(String sessionVariable) {
    StringBuilder out = new StringBuilder();
    StringBuilder sb = new StringBuilder();
    Parse state = Parse.Normal;
    boolean iskey = true;
    boolean singleQuotes = true;
    boolean first = true;
    String key = null;

    char[] chars = sessionVariable.toCharArray();

    for (char car : chars) {

      if (state == Parse.Escape) {
        sb.append(car);
        state = Parse.String;
        continue;
      }

      switch (car) {
        case '"':
          if (state == Parse.Normal) {
            state = Parse.String;
            singleQuotes = false;
          } else if (state == Parse.String && !singleQuotes) {
            state = Parse.Normal;
          }
          break;

        case '\'':
          if (state == Parse.Normal) {
            state = Parse.String;
            singleQuotes = true;
          } else if (state == Parse.String && singleQuotes) {
            state = Parse.Normal;
          }
          break;

        case '\\':
          if (state == Parse.String) {
            state = Parse.Escape;
          }
          break;

        case ';':
        case ',':
          if (state == Parse.Normal) {
            if (!iskey) {
              if (!first) {
                out.append(",");
              }
              out.append(key);
              out.append(sb);
              first = false;
            } else {
              key = sb.toString().trim();
              if (!key.isEmpty()) {
                if (!first) {
                  out.append(",");
                }
                out.append(key);
                first = false;
              }
            }
            iskey = true;
            key = null;
            sb = new StringBuilder();
            continue;
          }
          break;

        case '=':
          if (state == Parse.Normal && iskey) {
            key = sb.toString().trim();
            iskey = false;
            sb = new StringBuilder();
          }
          break;

        default:
          // nothing
      }

      sb.append(car);
    }

    if (!iskey) {
      if (!first) {
        out.append(",");
      }
      out.append(key);
      out.append(sb);
    } else {
      String tmpkey = sb.toString().trim();
      if (!tmpkey.isEmpty() && !first) {
        out.append(",");
      }
      out.append(tmpkey);
    }
    return out.toString();
  }

  private enum Parse {
    Normal,
    String, /* inside string */
    Quote,
    Escape /* found backslash */
  }
}
