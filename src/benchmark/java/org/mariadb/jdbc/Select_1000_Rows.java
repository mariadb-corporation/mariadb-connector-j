// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.openjdk.jmh.annotations.Benchmark;

public class Select_1000_Rows extends Common {
  private static final String sql =
      "select seq, 'abcdefghijabcdefghijabcdefghijaa' from seq_1_to_1000";

  @Benchmark
  public int text(MyState state) throws Throwable {
    return run(state.connectionText);
  }

  @Benchmark
  public int binary(MyState state) throws Throwable {
    return run(state.connectionBinary);
  }

  private int run(Connection con) throws Throwable {
    try (PreparedStatement st = con.prepareStatement(sql)) {
      ResultSet rs = st.executeQuery();
      int i = 0;
      while (rs.next()) {
        i = rs.getInt(1);
        rs.getString(2);
      }
      return i;
    }
  }
}
