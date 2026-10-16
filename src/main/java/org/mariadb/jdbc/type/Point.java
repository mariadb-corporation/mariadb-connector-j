// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.type;

import java.util.Objects;

/** Point */
public class Point implements Geometry {

  private final double x;
  private final double y;

  /**
   * Constructor
   *
   * @param x abscissa
   * @param y ordinate
   */
  public Point(double x, double y) {
    this.x = x;
    this.y = y;
  }

  /**
   * Get abscissa
   *
   * @return abscissa
   */
  public double getX() {
    return x;
  }

  /**
   * get ordinate
   *
   * @return ordinate
   */
  public double getY() {
    return y;
  }

  @Override
  public String toString() {
    return "POINT(" + x + " " + y + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !(o instanceof Point)) return false;
    Point other = (Point) o;
    return Double.compare(x, other.x) == 0 && Double.compare(y, other.y) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(x, y);
  }
}
