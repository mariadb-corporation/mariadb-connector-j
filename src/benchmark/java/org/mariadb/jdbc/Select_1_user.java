// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc;

import org.openjdk.jmh.annotations.Benchmark;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class Select_1_user extends Common {

  @Benchmark
  public Object[] text(MyState state) throws Throwable {
    return run(state.connectionText);
  }


  @Benchmark
  public Object[] binary(MyState state) throws Throwable {
    return run(state.connectionBinary);
  }

  private Object[] run(Connection con) throws Throwable {
    final int numberOfUserCol = 46;
    try (Statement st = con.createStatement()) {
      ResultSet rs = st.executeQuery("select * FROM mysql.user LIMIT 1");
      rs.next();
      Object[] objs = new Object[numberOfUserCol];
      for (int i = 0; i < numberOfUserCol; i++) {
        objs[i] = rs.getObject(i + 1);
      }
      return objs;
    }
  }
}
