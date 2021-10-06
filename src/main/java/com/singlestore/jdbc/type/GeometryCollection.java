// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.type;

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
