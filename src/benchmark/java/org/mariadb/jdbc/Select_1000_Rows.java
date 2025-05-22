// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

public class Select_1000_Rows extends Common {
  private static final String sql =
      "select seq, 'abcdefghijabcdefghijabcdefghijaa' from seq_1_to_1000";

  @Benchmark
  public void text(MyState state, Blackhole blackhole) throws Throwable {
    run(state.connectionText, blackhole);
  }

  @Benchmark
  public void textsequential(MyState state, Blackhole blackhole) throws Throwable {
    try (PreparedStatement st = state.connectionText.prepareStatement(sql, MariaDbResultSet.TYPE_SEQUENTIAL_ACCESS_ONLY, ResultSet.CONCUR_READ_ONLY)) {
      ResultSet rs = st.executeQuery();
      while (rs.next()) {
        blackhole.consume(rs.getInt(1));
        blackhole.consume(rs.getString(2));
      }
    }
  }

  @Benchmark
  public void binary(MyState state, Blackhole blackhole) throws Throwable {
    run(state.connectionBinary, blackhole);
  }

  @Benchmark
  public void binarysequential(MyState state, Blackhole blackhole) throws Throwable {
    try (PreparedStatement st = state.connectionBinary.prepareStatement(sql, MariaDbResultSet.TYPE_SEQUENTIAL_ACCESS_ONLY, ResultSet.CONCUR_READ_ONLY)) {
      ResultSet rs = st.executeQuery();
      while (rs.next()) {
        blackhole.consume(rs.getInt(1));
        blackhole.consume(rs.getString(2));
      }
    }
  }

  private void run(Connection con, Blackhole blackhole) throws Throwable {
    try (PreparedStatement st = con.prepareStatement(sql)) {
      ResultSet rs = st.executeQuery();
      while (rs.next()) {
        blackhole.consume(rs.getInt(1));
        blackhole.consume(rs.getString(2));
      }
    }
  }
}
