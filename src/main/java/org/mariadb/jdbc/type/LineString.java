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

package org.mariadb.jdbc.type;

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
    return toString().equals(o.toString());
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(open);
    result = 31 * result + Arrays.hashCode(points);
    return result;
  }
}
