// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.util;

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
          } else if (!singleQuotes) {
            state = Parse.Normal;
          }
          break;

        case '\'':
          if (state == Parse.Normal) {
            state = Parse.String;
            singleQuotes = true;
          } else if (singleQuotes) {
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
    Escape /* found backslash */
  }
}
