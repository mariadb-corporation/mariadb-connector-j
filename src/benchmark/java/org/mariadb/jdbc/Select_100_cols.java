// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.sql.*;
import java.sql.Connection;
import java.sql.Statement;

public class Select_100_cols extends Common {

  @Benchmark
  public int[] text(MyState state) throws Throwable {
    return run(state.connectionText);
  }

  @Benchmark
  public int[] binary(MyState state) throws Throwable {
    return run(state.connectionBinary);
  }

  @Benchmark
  public int[] binaryNoCache(MyState state) throws Throwable {
    return run(state.connectionBinaryNoCache);
  }

  private int[] run(Connection con) throws Throwable {

    try (PreparedStatement prep = con.prepareStatement("select * FROM test100")) {
      ResultSet rs = prep.executeQuery();
      rs.next();
      int[] objs = new int[100];
      for (int i = 0; i < 100; i++) {
        objs[i] = rs.getInt(i + 1);
      }
      return objs;
    }
  }

}
