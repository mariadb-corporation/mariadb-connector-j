/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc;

import org.openjdk.jmh.annotations.Benchmark;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Select_10000_Rows extends Common {
  private static final String sql =
          "SELECT lpad(conv(floor(rand()*pow(36,8)), 10, 36), 8, 0) as rnd_str_8 FROM seq_1_to_10000";

  @Benchmark
  public String[] run(MyState state) throws Throwable {
    try (PreparedStatement st = state.connection.prepareStatement(sql)) {

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
