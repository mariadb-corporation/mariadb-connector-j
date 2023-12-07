// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.type;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Polygon {

  private final LineString[] lines;

  private static final Pattern polygonPattern = Pattern.compile("^POLYGON\\((.*)\\)$");

  public Polygon(String s) throws IllegalArgumentException {
    Matcher m = polygonPattern.matcher(s);
    if (!m.matches()) {
      throw new IllegalArgumentException();
    }
    // split by "), (", but keep the parentheses by using lookahead and lookbehind
    String[] ringStrings = m.group(1).split("(?<=\\)), (?=\\()");

    lines = new LineString[ringStrings.length];
    for (int i = 0; i < ringStrings.length; i++) {
      lines[i] = LineString.FromRingString(ringStrings[i]);
    }
  }

  public Polygon(LineString[] lines) {
    this.lines = lines;
  }

  public LineString[] getLines() {
    return lines;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("POLYGON(");
    int indexLine = 0;
    for (LineString ls : lines) {
      if (indexLine++ > 0) {
        sb.append(", ");
      }
      sb.append("(");
      int index = 0;
      for (Point pt : ls.getPoints()) {
        if (index++ > 0) {
          sb.append(", ");
        }
        sb.append(pt.getX()).append(" ").append(pt.getY());
      }
      sb.append(")");
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !(o instanceof Polygon)) return false;
    return toString().equals(o.toString());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(lines);
  }
}
