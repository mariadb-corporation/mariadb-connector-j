// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.*;

public class ConfigurationTest extends Common {

  @BeforeAll
  public static void begin() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE testSessionVariable(id int not null primary key auto_increment, test"
            + " varchar(10))");
    stmt.execute("FLUSH TABLES");
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS testSessionVariable");
  }

  @Test
  void testSessionVariable() throws SQLException {
    try (Connection connection =
        createCon("sessionVariables=auto_increment_increment=2&allowMultiQueries=true")) {
      Statement stmt = connection.createStatement();
      stmt.execute(
          "INSERT INTO testSessionVariable (test) values ('bb'),('cc');"
              + "INSERT INTO testSessionVariable (test) values ('dd'),('ee')",
          Statement.RETURN_GENERATED_KEYS);

      ResultSet rs = stmt.getGeneratedKeys();

      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertTrue(rs.next());
      if (!"galera".equals(System.getenv("srv"))) {
        assertEquals(5, rs.getInt(1));
      }
      assertFalse(rs.next());
      assertFalse(stmt.getMoreResults());
      rs.clearWarnings();
    }

    // Xpand doesn't support session_track_system_variables
    if (!isXpand()) {
      try (Connection connection =
          createCon("sessionVariables=session_track_system_variables='some\\';f,\"ff'")) {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT @@session_track_system_variables");
        assertTrue(rs.next());
      } catch (SQLException e) {
        // in case server check variable validity
        Assertions.assertTrue(
            e.getCause().getMessage().contains("Unknown system variable 'some';f'"));
      }
      try (Connection connection =
          createCon("sessionVariables=session_track_system_variables=\"some\\\";f,'ff'\"")) {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT @@session_track_system_variables");
        assertTrue(rs.next());
      } catch (SQLException e) {
        // in case server check variable validity
        Assertions.assertTrue(
            e.getCause().getMessage().contains("Unknown system variable 'some\";f'"));
      }
      try (Connection connection =
          createCon("sessionVariables=session_track_system_variables=connect_timeout")) {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT @@session_track_system_variables");
        assertTrue(rs.next());
      }
    }
  }

  @Test
  void connectionAttributes() throws SQLException {
    // xpand doesn't support @@performance_schema variable
    Assumptions.assumeTrue(!"maxscale".equals(System.getenv("srv")) && !isXpand());

    try (org.mariadb.jdbc.Connection conn =
        createCon("&connectionAttributes=test:test1,test2:test2Val,test3")) {
      Statement stmt = conn.createStatement();
      ResultSet rs1 = stmt.executeQuery("SELECT @@performance_schema");
      rs1.next();
      Assumptions.assumeTrue("1".equals(rs1.getString(1)));

      ResultSet rs =
          stmt.executeQuery(
              "SELECT * from performance_schema.session_connect_attrs where processlist_id="
                  + conn.getThreadId()
                  + " AND ATTR_NAME like 'test%'");
      assertTrue(rs.next());
      assertEquals("test1", rs.getString("ATTR_VALUE"));
      assertTrue(rs.next());
      assertEquals("test2Val", rs.getString("ATTR_VALUE"));
      assertTrue(rs.next());
      assertNull(rs.getString("ATTR_VALUE"));
    }
  }

  @Test
  void useMysqlMetadata() throws SQLException {
    assertEquals(
        isMariaDBServer() ? "MariaDB" : "MySQL", sharedConn.getMetaData().getDatabaseProductName());
    try (org.mariadb.jdbc.Connection conn = createCon("&useMysqlMetadata=true")) {
      assertEquals("MySQL", conn.getMetaData().getDatabaseProductName());
    }
  }

  @Test
  void jdbcCompliantTruncation() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT @@sql_mode");
    String sqlMode = "";
    if (rs.next()) sqlMode = rs.getString(1);
    stmt.execute("DROP TABLE IF EXISTS jdbcCompliantTruncation");
    stmt.execute("CREATE TABLE jdbcCompliantTruncation(val varchar(15) NOT NULL)");
    try {
      stmt.execute("INSERT INTO jdbcCompliantTruncation(val) VALUES ('12345678901234567890')");
      fail("expect sql_mode to have strict mode by default");
    } catch (SQLException e) {
      assertEquals(1406, e.getErrorCode()); // Data too long for column 'val' at row 1
    }
    try {
      stmt.execute("SET @@global.sql_mode = ''");
      try (org.mariadb.jdbc.Connection conn = createCon("&jdbcCompliantTruncation=false")) {
        Statement stmt2 = conn.createStatement();
        stmt2.execute("INSERT INTO jdbcCompliantTruncation(val) VALUES ('12345678901234567890')");
        SQLWarning warn = stmt2.getWarnings();
        assertNotNull(warn);
        ResultSet rs2 = stmt2.executeQuery("SELECT * FROM jdbcCompliantTruncation");
        rs2.next();
        assertEquals("123456789012345", rs2.getString(1));
      }

      try (org.mariadb.jdbc.Connection conn = createCon("&jdbcCompliantTruncation=true")) {
        try {
          Statement stmt2 = conn.createStatement();
          stmt2.execute("INSERT INTO jdbcCompliantTruncation(val) VALUES ('12345678901234567890')");
          fail("expect sql_mode to have strict mode by default");
        } catch (SQLException e) {
          assertEquals(1406, e.getErrorCode()); // Data too long for column 'val' at row 1
        }
      }

    } finally {
      stmt.execute("SET @@global.sql_mode = '" + sqlMode + "'");
    }
  }

  @Test
  void connectionCollationTest() throws SQLException {
    try (org.mariadb.jdbc.Connection conn =
        createCon("&connectionCollation=utf8mb4_vietnamese_ci")) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT @@session.COLLATION_CONNECTION");
      rs.next();
      assertEquals("utf8mb4_vietnamese_ci", rs.getString(1));
    }

    try (org.mariadb.jdbc.Connection conn = createCon("&connectionCollation=")) {
      Statement stmt = conn.createStatement();
      ResultSet rs =
          stmt.executeQuery("SELECT @@global.COLLATION_CONNECTION, @@session.COLLATION_CONNECTION");
      rs.next();
      if (rs.getString(1).startsWith("utf8mb4")) {
        // could be different, because charset default might differ from server default
        assertTrue(
            rs.getString(2).equals(rs.getString(1))
                || "utf8mb4_unicode_ci".equals(rs.getString(1))
                || "utf8mb4_general_ci".equals(rs.getString(1)),
            "collation was " + rs.getString(1));
      }
    }

    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery("SELECT @@global.COLLATION_CONNECTION, @@session.COLLATION_CONNECTION");
    rs.next();
    // could be different, because charset default might differ from server default
    if (rs.getString(1).startsWith("utf8mb4")) {
      assertTrue(
          rs.getString(2).equals(rs.getString(1))
              || "utf8mb4_unicode_ci".equals(rs.getString(1))
              || "utf8mb4_general_ci".equals(rs.getString(1)));
    }

    assertThrowsContains(
        SQLException.class,
        () -> createCon("&connectionCollation=utf8_vietnamese_ci"),
        "wrong connection collation 'utf8_vietnamese_ci' only utf8mb4 collation are accepted");
    assertThrowsContains(
        SQLException.class,
        () -> createCon("&connectionCollation=utf8mb4_vietnamese_ci;SELECT"),
        "wrong connection collation 'utf8mb4_vietnamese_ci;SELECT' name");
    assertThrowsContains(
        SQLException.class,
        () -> createCon("&connectionCollation=utf8mb4_🙏"),
        "wrong connection collation 'utf8mb4_🙏' name");
  }
}
