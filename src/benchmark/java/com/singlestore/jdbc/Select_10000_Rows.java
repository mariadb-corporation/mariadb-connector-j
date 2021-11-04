// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

import org.openjdk.jmh.annotations.Benchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Select_10000_Rows extends Common {
  private static final String sql =
      "SELECT lpad(conv(floor(rand()*pow(36,8)), 10, 36), 8, 0) as rnd_str_8 FROM range_1_10000";


  @Benchmark
  public String[] text(MyState state) throws Throwable {
    return run(state.connectionText);
  }


  @Benchmark
  public String[] binary(MyState state) throws Throwable {
    return run(state.connectionBinary);
  }

  private String[] run(Connection con) throws Throwable {
    try (PreparedStatement st = con.prepareStatement(sql)) {

      ResultSet rs = st.executeQuery();
      String[] res = new String[10000];
      int i = 0;
      while (rs.next()) {
        res[i++] = rs.getString(1);
      }
      return res;
    }
  }
}
