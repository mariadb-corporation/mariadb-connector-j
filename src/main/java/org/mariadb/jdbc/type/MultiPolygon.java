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

public class MultiPolygon implements Geometry {

  private final Polygon[] polygons;

  public MultiPolygon(Polygon[] polygons) {
    this.polygons = polygons;
  }

  public Polygon[] getPolygons() {
    return polygons;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("MULTIPOLYGON(");
    int indexpoly = 0;
    for (Polygon poly : polygons) {
      if (indexpoly++ > 0) {
        sb.append(",");
      }
      sb.append("(");
      int indexLine = 0;
      for (LineString ls : poly.getLines()) {
        if (indexLine++ > 0) {
          sb.append(",");
        }
        sb.append("(");
        int index = 0;
        for (Point pt : ls.getPoints()) {
          if (index++ > 0) {
            sb.append(",");
          }
          sb.append(pt.getX()).append(" ").append(pt.getY());
        }
        sb.append(")");
      }
      sb.append(")");
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !(o instanceof MultiPolygon)) return false;
    return toString().equals(o.toString());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(polygons);
  }
}
