// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.type;

import java.util.Arrays;
import java.util.Objects;

public class LineString implements Geometry {

  private final Point[] points;
  private final boolean open;

  public LineString(Point[] points, boolean open) {
    this.points = points;
    this.open = open;
  }

  public Point[] getPoints() {
    return points;
  }

  public boolean isOpen() {
    return open;
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
    if (o == null || !(o instanceof LineString)) return false;
    return open == ((LineString) o).isOpen() && toString().equals(o.toString());
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(open);
    result = 31 * result + Arrays.hashCode(points);
    return result;
  }
}
