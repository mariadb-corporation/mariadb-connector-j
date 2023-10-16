// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc;

import java.sql.ResultSet;
import java.sql.Statement;
import org.openjdk.jmh.annotations.Benchmark;

public class Select_1 extends Common {

  @Benchmark
  public int run(MyState state) throws Throwable {
    try (Statement st = state.connectionText.createStatement()) {
      ResultSet rs = st.executeQuery("select 1");
      rs.next();
      return rs.getInt(1);
    }
  }
}
