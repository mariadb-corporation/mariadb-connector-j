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

import org.openjdk.jmh.annotations.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 2)
@Threads(value = -1) // detecting CPU count
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class Common {

  @State(Scope.Thread)
  public static class MyState {

    // conf
    public final String host = System.getProperty("TEST_HOST", "localhost");
    public final int port = Integer.parseInt(System.getProperty("TEST_PORT", "3306"));
    public final String username = System.getProperty("TEST_USERNAME", "root");
    public final String password = System.getProperty("TEST_PASSWORD", "");
    public final String database = System.getProperty("TEST_DATABASE", "testj");
    // connections
    protected Connection connectionText;
    protected Connection connectionBinary;

    @Param({"mysql", "mariadb"})
    String driver;

    @Setup(Level.Trial)
    public void doSetup() throws Exception {

      String className;
      switch (driver) {
        case "mysql":
          className = "com.mysql.cj.jdbc.Driver";
          break;
        case "mariadb":
          className = "org.mariadb.jdbc.Driver";
          break;
        default:
          throw new RuntimeException("wrong param");
      }
      try {
        String jdbcBase = "%s:%s/%s?user=%s&password=%s&sslMode=DISABLED&useServerPrepStmts=%s&cachePrepStmts=%s&serverTimezone=UTC";
        String jdbcUrlText =
                String.format(
                        jdbcBase,
                        host, port, database, username, password, false, false);
        String jdbcUrlBinary =
                String.format(
                        jdbcBase,
                        host, port, database, username, password, true, true);
        connectionText =
            ((java.sql.Driver) Class.forName(className).getDeclaredConstructor().newInstance())
                .connect("jdbc:" + driver + "://" + jdbcUrlText, new Properties());
        connectionBinary =
                ((java.sql.Driver) Class.forName(className).getDeclaredConstructor().newInstance())
                        .connect("jdbc:" + driver + "://" + jdbcUrlBinary, new Properties());
      } catch (SQLException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @TearDown(Level.Trial)
    public void doTearDown() throws SQLException {
      connectionText.close();
      connectionBinary.close();
    }
  }
}
