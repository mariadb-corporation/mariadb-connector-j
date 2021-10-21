// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc;

import org.openjdk.jmh.annotations.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 5)
@Threads(value = -1) // detecting CPU count
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class Common {

  // conf
  public final static String host = System.getProperty("TEST_HOST", "localhost");
  public final static int port = Integer.parseInt(System.getProperty("TEST_PORT", "3306"));
  public final static String username = System.getProperty("TEST_USERNAME", "root");
  public final static String password = System.getProperty("TEST_PASSWORD", "");
  public final static String database = System.getProperty("TEST_DATABASE", "testj");
  public final static String other = System.getProperty("TEST_OTHER", "");
  static {
    new SetupData();
  }

  @State(Scope.Thread)
  public static class MyState {

    // connections
    protected Connection connectionText;
    protected Connection connectionBinary;

    @Param({"mysql", "mariadb"})
    String driver;

    @Setup(Level.Trial)
    public void createConnections() throws Exception {

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
        String jdbcBase = "jdbc:%s://%s:%s/%s?user=%s&password=%s&sslMode=DISABLED&useServerPrepStmts=%s&cachePrepStmts=%s&serverTimezone=UTC&%s";
        String jdbcUrlText =
                String.format(
                        jdbcBase,
                        driver, host, port, database, username, password, false, false, other);
        String jdbcUrlBinary =
                String.format(
                        jdbcBase,
                        driver, host, port, database, username, password, true, true, other);
        connectionText =
            ((java.sql.Driver) Class.forName(className).getDeclaredConstructor().newInstance())
                .connect(jdbcUrlText, new Properties());
        connectionBinary =
                ((java.sql.Driver) Class.forName(className).getDeclaredConstructor().newInstance())
                        .connect(jdbcUrlBinary, new Properties());
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

  public static class SetupData {
    static {
      try {
        try (Connection conn =
                     DriverManager.getConnection(
                             String.format(
                                     "jdbc:mariadb://%s:%s/%s?user=%s&password=%s",
                                     host, port, database, username, password))) {
          Statement stmt = conn.createStatement();
          stmt.executeUpdate("DROP TABLE IF EXISTS test100");
          StringBuilder sb = new StringBuilder("CREATE TABLE test100 (i1 int");
          StringBuilder sb2 = new StringBuilder("INSERT INTO test100 value (1");
          for (int i = 2; i <= 100; i++) {
            sb.append(",i").append(i).append(" int");
            sb2.append(",").append(i);
          }
          sb.append(")");
          sb2.append(")");
          stmt.executeUpdate(sb.toString());
          stmt.executeUpdate(sb2.toString());
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

}
