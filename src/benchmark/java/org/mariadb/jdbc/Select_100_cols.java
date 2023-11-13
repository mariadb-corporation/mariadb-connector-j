// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc;

import java.sql.*;
import java.sql.Connection;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

public class Select_100_cols extends Common {

  @Benchmark
  public void text(MyState state, Blackhole blackhole) throws Throwable {
    run(state.connectionText, blackhole);
  }

  @Benchmark
  public void binary(MyState state, Blackhole blackhole) throws Throwable {
    run(state.connectionBinary, blackhole);
  }

  @Benchmark
  public void binaryNoCache(MyState state, Blackhole blackhole) throws Throwable {
    run(state.connectionBinaryNoCache, blackhole);
  }

  @Benchmark
  public void binaryNoPipeline(MyState state, Blackhole blackhole) throws Throwable {
    run(state.connectionBinaryNoPipeline, blackhole);
  }


  private void run(Connection con, Blackhole blackhole) throws Throwable {

    try (PreparedStatement prep = con.prepareStatement("select * FROM test100")) {
      try (ResultSet rs = prep.executeQuery()) {
        rs.next();
        for (int i = 0; i < 100; i++) {
          blackhole.consume(rs.getInt(i + 1));
        }
      }
    }
  }

}
