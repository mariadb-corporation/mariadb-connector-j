// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.type;

import java.util.Arrays;

/** Multi-point */
public class MultiPoint implements Geometry {

  private final Point[] points;

  /**
   * Constructor
   *
   * @param points points
   */
  public MultiPoint(Point[] points) {
    this.points = points;
  }

  /**
   * Get points
   *
   * @return points
   */
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
