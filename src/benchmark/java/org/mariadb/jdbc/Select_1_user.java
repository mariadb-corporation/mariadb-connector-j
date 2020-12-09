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
