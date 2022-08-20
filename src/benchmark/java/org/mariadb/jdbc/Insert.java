// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc;

import org.openjdk.jmh.annotations.Benchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class Insert extends Common {

  @Benchmark
  public int text(MyState state) throws Throwable {
    return run(state.connectionText);
  }

  @Benchmark
  public int binary(MyState state) throws Throwable {
    return run(state.connectionBinary);
  }

  private int run(Connection con) throws Throwable {

    try (PreparedStatement prep = con.prepareStatement("INSERT INTO testBlackHole values (?,?)")) {
      prep.setInt(1, 1);
      prep.setString(2, "azertyuiop");
      return prep.executeUpdate();
    }
  }

}
