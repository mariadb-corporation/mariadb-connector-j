// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.type;

import java.util.Arrays;

/** Multi polygon */
public class MultiPolygon implements Geometry {

  private final Polygon[] polygons;

  /**
   * Constructor
   *
   * @param polygons polygons
   */
  public MultiPolygon(Polygon[] polygons) {
    this.polygons = polygons;
  }

  /**
   * Get polygons
   *
   * @return polygons
   */
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
