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

import java.sql.ResultSet;
import java.sql.Statement;

public class Select_1 extends Common {

  @Benchmark
  public int run(MyState state) throws Throwable {
    try (Statement st = state.connection.createStatement()) {
      int rnd = (int) (Math.random() * 1000);
      ResultSet rs = st.executeQuery("select " + rnd);
      rs.next();
      return rs.getInt(1);
    }
  }
}
