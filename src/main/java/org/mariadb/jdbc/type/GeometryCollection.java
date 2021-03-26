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

public class GeometryCollection implements Geometry {

  private final Geometry[] geometries;

  public GeometryCollection(Geometry[] geometries) {
    this.geometries = geometries;
  }

  public Geometry[] getGeometries() {
    return geometries;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("GEOMETRYCOLLECTION(");
    int indexpoly = 0;
    for (Geometry geo : geometries) {
      if (indexpoly++ > 0) {
        sb.append(",");
      }
      sb.append(geo.toString());
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || !(o instanceof GeometryCollection)) return false;
    return toString().equals(o.toString());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(geometries);
  }
}
