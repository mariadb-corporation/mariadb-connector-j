// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.type;

import java.util.Arrays;
import java.util.Objects;

/** Linestring object */
public class LineString implements Geometry {

  private final Point[] points;
  private final boolean open;

  /**
   * Constructor
   *
   * @param points point list
   * @param open open linestring
   */
  public LineString(Point[] points, boolean open) {
    this.points = points;
    this.open = open;
  }

  /**
   * get points
   *
   * @return points
   */
  public Point[] getPoints() {
    return points;
  }

  /**
   * Is form open
   *
   * @return is open
   */
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
