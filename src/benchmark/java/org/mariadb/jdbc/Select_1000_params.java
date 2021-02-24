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

package org.mariadb.jdbc;

import org.openjdk.jmh.annotations.Benchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Select_1000_params extends Common {

  private static final String sql;

  static {
    StringBuilder sb = new StringBuilder("select ?");
    for (int i = 1; i < 1000; i++) {
      sb.append(",?");
    }
    sql = sb.toString();
  }

  private static int[] randParams() {
    int[] rnds = new int[1000];
    for (int i = 0; i < 1000; i++) {
      rnds[i] = (int) (Math.random() * 1000);
    }
    return rnds;
  }

  @Benchmark
  public Integer text(MyState state) throws Throwable {
    return run(state.connectionText);
  }

  @Benchmark
  public Integer binary(MyState state) throws Throwable {
    return run(state.connectionBinary);
  }

  private Integer run(Connection con) throws Throwable {
    int[] rnds = randParams();
    try (PreparedStatement st = con.prepareStatement(sql)) {
      for (int i = 1; i <= 1000; i++) {
        st.setInt(i, rnds[i - 1]);
      }
      ResultSet rs = st.executeQuery();
      rs.next();
      for (int i = 1; i <= 1000; i++) {
        if (rnds[i - 1] != rs.getInt(i)) throw new IllegalStateException("ERROR");
      }
      return rs.getInt(1);
    }
  }
}
