// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.type;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Point {

  private final double x;
  private final double y;

  public Point(double x, double y) {
    this.x = x;
    this.y = y;
  }

  private static final Pattern pointPattern = Pattern.compile("^POINT\\((.*) (.*)\\)$");

  public Point(String s) throws IllegalArgumentException {
    Matcher m = pointPattern.matcher(s);
    if (!m.matches()) {
      throw new IllegalArgumentException();
    }

    this.x = Double.parseDouble(m.group(1));
    this.y = Double.parseDouble(m.group(2));
  }

  public double getX() {
    return x;
  }

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
    return toString().equals(o.toString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(x, y);
  }
}
