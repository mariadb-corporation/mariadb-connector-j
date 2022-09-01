// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc;

import org.openjdk.jmh.annotations.Benchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Select_1000_Rows extends Common {
  private static final String sql =
      "select seq, 'abcdefghijabcdefghijabcdefghijaa' from seq_1_to_1000";

  @Benchmark
  public String[] text(MyState state) throws Throwable {
    return run(state.connectionText, state.fetchSize);
  }

  @Benchmark
  public String[] binary(MyState state) throws Throwable {
    return run(state.connectionBinary, state.fetchSize);
  }

  private String[] run(Connection con, int fetchSize) throws Throwable {
    try (PreparedStatement st = con.prepareStatement(sql)) {
      st.setFetchSize(fetchSize);
      ResultSet rs = st.executeQuery();
      String[] res = new String[1000];
      int i = 0;
      while (rs.next()) {
        rs.getInt(1);
        res[i++] = rs.getString(2);
      }
      return res;
    }
  }
}
