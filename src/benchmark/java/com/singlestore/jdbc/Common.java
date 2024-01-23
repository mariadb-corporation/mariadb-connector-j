// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 5)
@Threads(value = -1) // detecting CPU count
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class Common {

  @State(Scope.Thread)
  public static class MyState {

    // conf
    public static final String host = System.getProperty("TEST_HOST", "localhost");
    public static final int port = Integer.parseInt(System.getProperty("TEST_PORT", "3306"));
    public static final String username = System.getProperty("TEST_USERNAME", "root");
    public static final String password = System.getProperty("TEST_PASSWORD", "root");
    public static final String database = System.getProperty("TEST_DATABASE", "test");
    public final static String other = System.getProperty("TEST_OTHER", "");

    static {
      new SetupData();
    }

    // connections
    protected Connection connectionText;
    protected Connection connectionTextRewrite;

    protected Connection connectionBinary;

    protected Connection connectionBinaryNoPipeline;
    protected Connection connectionBinaryNoCache;

    @Param({"singlestore", "mariadb", "mysql"})
    String driver;

    @Setup(Level.Trial)
    public void doSetup() throws Exception {

      String className;
      switch (driver) {
        case "singlestore":
          className = "com.singlestore.jdbc.Driver";
          break;
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
        // allowLoadLocalInfile is specified for mysql driver instead of allowLocalInfile,
        // S2 and mariadb will just ignore it
        String jdbcBase = "jdbc:%s://%s:%s/%s?user=%s&password=%s&sslMode=DISABLED&useServerPrepStmts=%s&cachePrepStmts=%s&serverTimezone=UTC%s";
        String jdbcUrlText =
            String.format(
                jdbcBase,
                driver, host, port, database, username, password, false, true, other);
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
        String jdbcUrlTextRewrite =
            String.format(
                jdbcBase,
                driver, host, port, database, username, password, false, false,
                "&rewriteBatchedStatements=true&useBulkStmts=false" + other);
        connectionTextRewrite =
            ((java.sql.Driver) Class.forName(className).getDeclaredConstructor().newInstance())
                .connect(jdbcUrlTextRewrite, new Properties());

        String jdbcUrlBinaryNoCache =
            String.format(
                jdbcBase,
                driver, host, port, database, username, password, true, false,
                "&prepStmtCacheSize=0" + other);

        connectionBinaryNoCache =
            ((java.sql.Driver) Class.forName(className).getDeclaredConstructor().newInstance())
                .connect(jdbcUrlBinaryNoCache, new Properties());

        String jdbcUrlBinaryNoCacheNoPipeline =
            String.format(
                jdbcBase,
                driver, host, port, database, username, password, true, true,
                "&prepStmtCacheSize=0&cachePrepStmts=false&disablePipeline=true" + other);
        connectionBinaryNoPipeline =
            ((java.sql.Driver) Class.forName(className).getDeclaredConstructor().newInstance())
                .connect(jdbcUrlBinaryNoCacheNoPipeline, new Properties());
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @TearDown(Level.Trial)
    public void doTearDown() throws SQLException {
      connectionText.close();
      connectionBinary.close();
      connectionTextRewrite.close();
      connectionBinaryNoCache.close();
      connectionBinaryNoPipeline.close();
    }

    public static class SetupData {

      static {
        try {
          try (Connection conn =
              DriverManager.getConnection(
                  String.format(
                      "jdbc:singlestore://%s:%s/%s?user=%s&password=%s",
                      host, port, database, username, password))) {
            try (Statement st = conn.createStatement()) {
              try {
                st.execute("CREATE TABLE range_1_10000(n int)");
                st.execute("CREATE OR REPLACE PROCEDURE fill_range() AS BEGIN " +
                    "FOR i IN 1 .. 10000 LOOP" +
                    " INSERT INTO range_1_10000 VALUES (i);" +
                    "END LOOP;" +
                    "END");
                st.execute("CALL fill_range()");
              } catch (SQLSyntaxErrorException e) {
                if (!e.getMessage().contains("Table 'range_1_10000' already exists")) {
                  throw e;
                }
              }
              st.execute("DROP TABLE IF EXISTS perfTestTextBatch");
              String createTable = "CREATE TABLE perfTestTextBatch (id MEDIUMINT NOT NULL AUTO_INCREMENT,t0 text, PRIMARY KEY (id)) COLLATE='utf8mb4_unicode_ci'";
              st.execute(createTable);

              st.execute("DROP TABLE IF EXISTS test100");
              StringBuilder sb = new StringBuilder("CREATE TABLE test100 (i1 int");
              StringBuilder sb2 = new StringBuilder("INSERT INTO test100 value (1");
              for (int i = 2; i <= 100; i++) {
                sb.append(",i").append(i).append(" int");
                sb2.append(",").append(i);
              }
              sb.append(")");
              sb2.append(")");
              st.executeUpdate(sb.toString());
              st.executeUpdate(sb2.toString());
              st.execute("DROP TABLE IF EXISTS perfTestUpdateBatch");
              st.execute(
                      "CREATE TABLE perfTestUpdateBatch (\n" +
                              "id bigint(11) NOT NULL AUTO_INCREMENT,\n" +
                              "f1 bigint(11) NOT NULL,\n" +
                              "f2 varchar(50) DEFAULT NULL,\n" +
                              "f3 varchar(50) DEFAULT NULL,\n" +
                              "f4 varchar(50) DEFAULT NULL,\n" +
                              "f5 varchar(50) DEFAULT NULL,\n" +
                              "f6 varchar(50) DEFAULT NULL,\n" +
                              "f7 varchar(50) DEFAULT NULL,\n" +
                              "f8 varchar(50) DEFAULT NULL,\n" +
                              "f9 varchar(50) DEFAULT NULL, PRIMARY KEY (id));");
              try (PreparedStatement prep = conn.prepareStatement("INSERT INTO perfTestUpdateBatch(f1) value (?)")) {
                for (int i = 0; i < 1000; i++) {
                  prep.setInt(1, 0);
                  prep.addBatch();
                }
                prep.executeBatch();
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
