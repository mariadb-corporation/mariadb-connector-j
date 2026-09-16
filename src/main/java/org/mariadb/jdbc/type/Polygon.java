// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.type;

import java.util.Arrays;

/** Polygon */
public class Polygon implements Geometry {

  private final LineString[] lines;

  /**
   * polygon
   *
   * @param lines lines
   */
  public Polygon(LineString[] lines) {
    this.lines = lines;
  }

  /**
   * get lines
   *
   * @return lines
   */
  public LineString[] getLines() {
    return lines;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("POLYGON(");
    int indexLine = 0;
    for (LineString ls : lines) {
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
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !(o instanceof Polygon)) return false;
    return Arrays.equals(lines, ((Polygon) o).lines);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(lines);
  }
}
