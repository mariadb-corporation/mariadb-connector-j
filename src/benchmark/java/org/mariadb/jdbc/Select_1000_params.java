// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc;

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
