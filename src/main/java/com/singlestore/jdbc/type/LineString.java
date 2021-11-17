// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.type;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LineString {

  private final Point[] points;

  private static final Pattern linePattern = Pattern.compile("^LINESTRING\\((.*)\\)$");

  public LineString(String s) throws IllegalArgumentException {
    Matcher m = linePattern.matcher(s);
    if (!m.matches()) {
      throw new IllegalArgumentException();
    }
    String[] pointStrings = m.group(1).split(",");
    points = parsePoints(pointStrings);
  }

  private static Point[] parsePoints(String[] pointStrings) {
    Point[] points = new Point[pointStrings.length];
    for (int i = 0; i < pointStrings.length; i++) {
      String[] coords = pointStrings[i].trim().split(" ");
      points[i] = new Point(Double.parseDouble(coords[0]), Double.parseDouble(coords[1]));
    }
    return points;
  }

  public LineString(Point[] points) {
    this.points = points;
  }

  public Point[] getPoints() {
    return points;
  }

  public static LineString FromRingString(String pointsString) throws IllegalArgumentException {
    if (pointsString.length() <= 2) {
      throw new IllegalArgumentException(
          String.format("Failed to decode '%s' as LineString", pointsString));
    }
    // remove parentheses if they are present
    if (pointsString.charAt(0) == '(' && pointsString.charAt(pointsString.length() - 1) == ')') {
      pointsString = pointsString.substring(1, pointsString.length() - 1);
    }
    return new LineString(parsePoints(pointsString.split(",")));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("LINESTRING(");
    int index = 0;
    for (Point pt : points) {
      if (index++ > 0) {
        sb.append(",");
      }
      sb.append(pt.getX()).append(" ").append(pt.getY());
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof LineString)) return false;
    return toString().equals(o.toString());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(points);
  }
}
