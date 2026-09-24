// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.util;

import java.util.HexFormat;

public final class StringUtils {
  /**
   * Upper-case hexadecimal representation of a byte array.
   *
   * @param bytes bytes, may be null
   * @return hex string, empty for null
   */
  public static String byteArrayToHexString(final byte[] bytes) {
    return (bytes != null) ? HexFormat.of().withUpperCase().formatHex(bytes) : "";
  }

  /**
   * Parse the year, month and day of a {@code YYYY-MM-DD} text value, optionally followed by a time
   * part separated by a space. Fields are parsed in place with {@link
   * Integer#parseInt(CharSequence, int, int, int)}: no regex split, no substring allocation.
   *
   * @param val text value
   * @return {year, month, day}, or null when the value has not three dash-separated fields
   * @throws NumberFormatException if a field is not a number
   */
  public static int[] parseYearMonthDay(String val) {
    int first = val.indexOf('-');
    if (first < 0) return null;
    int second = val.indexOf('-', first + 1);
    if (second < 0) return null;
    int end = val.indexOf(' ', second + 1);
    if (end < 0) end = val.length();
    return new int[] {
      Integer.parseInt(val, 0, first, 10),
      Integer.parseInt(val, first + 1, second, 10),
      Integer.parseInt(val, second + 1, end, 10)
    };
  }
}
