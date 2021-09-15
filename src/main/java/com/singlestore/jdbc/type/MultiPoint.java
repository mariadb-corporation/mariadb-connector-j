// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.type;

import java.util.Arrays;

public class MultiPoint implements Geometry {

  private final Point[] points;

  public MultiPoint(Point[] points) {
    this.points = points;
  }

  public Point[] getPoints() {
    return points;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("MULTIPOINT(");
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
    if (o == null || !(o instanceof MultiPoint)) return false;
    return toString().equals(o.toString());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(points);
  }
}
