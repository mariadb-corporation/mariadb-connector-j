/*
 *
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
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc;

import static org.junit.Assert.*;

import java.io.*;
import java.sql.*;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class LocalInfileInputStreamTest extends BaseTest {

  @BeforeClass()
  public static void initClass() throws SQLException {
    drop();
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute("CREATE TABLE LocalInfileInputStreamTest(id int, test varchar(100))");
      stmt.execute("CREATE TABLE LocalInfileInputStreamTest2(id int, test varchar(100))");
      stmt.execute("CREATE TABLE LocalInfileXmlInputStreamTest(id int, test varchar(100))");
      stmt.execute("CREATE TABLE ttlocal(id int, test varchar(100))");
      stmt.execute("CREATE TABLE ttXmllocal(id int, test varchar(100))");
      stmt.execute("CREATE TABLE ldinfile(a varchar(10))");
      stmt.execute(
          "CREATE TABLE `infile`(`a` varchar(50) DEFAULT NULL, `b` varchar(50) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=latin1");
      stmt.execute("FLUSH TABLES");
    }
  }

  @AfterClass
  public static void drop() throws SQLException {
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS LocalInfileInputStreamTest");
      stmt.execute("DROP TABLE IF EXISTS LocalInfileInputStreamTest2");
      stmt.execute("DROP TABLE IF EXISTS LocalInfileXmlInputStreamTest");
      stmt.execute("DROP TABLE IF EXISTS ttlocal");
      stmt.execute("DROP TABLE IF EXISTS ttXmllocal");
      stmt.execute("DROP TABLE IF EXISTS ldinfile");
      stmt.execute("DROP TABLE IF EXISTS `infile`");
    }
  }

  private static boolean checkLocal() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT @@local_infile");
    if (rs.next()) {
      return rs.getInt(1) == 1;
    }
    return false;
  }

  @Test
  public void loadDataInBatch() throws SQLException {
    Assume.assumeFalse((!isMariadbServer() && minVersion(8, 0, 3)));
    Assume.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    String batch_update =
        "LOAD DATA LOCAL INFILE 'dummy.tsv' INTO TABLE LocalInfileInputStreamTest2 (id, test)";
    String builder = "1\thello\n2\tworld\n";
    try (Connection con = setConnection()) {
      Statement smt = con.createStatement();
      InputStream inputStream = new ByteArrayInputStream(builder.getBytes());
      ((MariaDbStatement) smt).setLocalInfileInputStream(inputStream);
      smt.addBatch(batch_update);
      smt.addBatch("SET UNIQUE_CHECKS=1");
      smt.executeBatch();
    }
  }

  @Test
  public void testLocalInfileInputStream() throws SQLException {
    Assume.assumeFalse((!isMariadbServer() && minVersion(8, 0, 3)));
    Assume.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    try (Connection connection = setConnection("&allowLocalInfile=true")) {
      try (Statement st = connection.createStatement()) {
        // Build a tab-separated record file
        String builder = "1\thello\n2\tworld\n";

        InputStream inputStream = new ByteArrayInputStream(builder.getBytes());
        ((MariaDbStatement) st).setLocalInfileInputStream(inputStream);
        st.executeUpdate(
            "LOAD DATA LOCAL INFILE 'dummy.tsv' INTO TABLE LocalInfileInputStreamTest (id, test)");

        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM LocalInfileInputStreamTest");
        assertTrue(rs.next());

        int count = rs.getInt(1);
        assertEquals(2, count);

        rs = st.executeQuery("SELECT * FROM LocalInfileInputStreamTest");

        validateRecord(rs, 1, "hello");
        validateRecord(rs, 2, "world");
      }
    }
  }

  @Test
  public void testLocalXmlInfileInputStream() throws SQLException {
    Assume.assumeFalse((!isMariadbServer() && minVersion(8, 0, 3)));
    Assume.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    try (Connection connection = setConnection("&allowLocalInfile=true")) {
      try (Statement st = connection.createStatement()) {
        // Build a tab-separated record file
        String builder = "<row id=\"1\" test=\"hello\" />\n<row id=\"2\" test=\"world\" />\n";

        InputStream inputStream = new ByteArrayInputStream(builder.getBytes());
        ((MariaDbStatement) st).setLocalInfileInputStream(inputStream);
        st.executeUpdate(
            "LOAD XML LOCAL INFILE 'dummy.tsv' INTO TABLE LocalInfileXmlInputStreamTest (id, test)");

        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM LocalInfileXmlInputStreamTest");
        assertTrue(rs.next());

        int count = rs.getInt(1);
        assertEquals(2, count);

        rs = st.executeQuery("SELECT * FROM LocalInfileXmlInputStreamTest");

        validateRecord(rs, 1, "hello");
        validateRecord(rs, 2, "world");
      }
    }
  }

  @Test
  public void testLocalInfileValidInterceptor() throws Exception {
    Assume.assumeFalse((!isMariadbServer() && minVersion(8, 0, 3)));
    Assume.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    File temp = File.createTempFile("validateInfile", ".txt");
    StringBuilder builder = new StringBuilder();
    builder.append("1,hello\n");
    builder.append("2,world\n");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp))) {
      bw.write(builder.toString());
    }
    try (Connection connection = setConnection("&allowLocalInfile=true")) {
      testLocalInfile(connection, temp.getAbsolutePath().replace("\\", "/"));
    }
  }

  @Test
  public void testLocalXmlInfileValidInterceptor() throws Exception {
    Assume.assumeFalse((!isMariadbServer() && minVersion(8, 0, 3)));
    Assume.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    File temp = File.createTempFile("validateInfile", ".txt");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp))) {
      bw.write("<row id=\"1\" test=\"hello\" />\n<row id=\"2\" test=\"world\" />\n");
    }
    try (Connection connection = setConnection("&allowLocalInfile=true")) {
      testXmlLocalInfile(connection, temp.getAbsolutePath().replace("\\", "/"));
    }
  }

  @Test
  public void testLocalInfileUnValidInterceptor() throws Exception {
    Assume.assumeFalse((!isMariadbServer() && minVersion(8, 0, 3)));
    Assume.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    File temp = File.createTempFile("localInfile", ".txt");
    StringBuilder builder = new StringBuilder();
    builder.append("1,hello\n");
    builder.append("2,world\n");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp))) {
      bw.write(builder.toString());
    }
    try (Connection connection = setConnection("&allowLocalInfile=true")) {
      try {
        testLocalInfile(connection, temp.getAbsolutePath().replace("\\", "/"));
        fail("Must have been intercepted");
      } catch (SQLException sqle) {
        assertTrue(
            sqle.getMessage().contains("LOAD DATA LOCAL INFILE request to send local file named")
                && sqle.getMessage()
                    .contains(
                        "not validated by interceptor \"org.mariadb.jdbc.LocalInfileInterceptorImpl\""));
      }
      // check that connection state is correct
      Statement st = connection.createStatement();
      ResultSet rs = st.executeQuery("SELECT 1");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
  }

  private void testLocalInfile(Connection connection, String file) throws SQLException {
    try (Statement st = connection.createStatement()) {
      st.executeUpdate(
          "LOAD DATA LOCAL INFILE '"
              + file
              + "' INTO TABLE ttlocal "
              + "  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'"
              + "  (id, test)");

      ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM ttlocal");
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));

      rs = st.executeQuery("SELECT * FROM ttlocal");

      validateRecord(rs, 1, "hello");
      validateRecord(rs, 2, "world");
    }
  }

  private void testXmlLocalInfile(Connection connection, String file) throws SQLException {
    try (Statement st = connection.createStatement()) {
      st.executeUpdate(
          "LOAD XML LOCAL INFILE '"
              + file
              + "' INTO TABLE ttXmllocal "
              + "  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'"
              + "  (id, test)");

      ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM ttXmllocal");
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));

      rs = st.executeQuery("SELECT * FROM ttXmllocal");

      validateRecord(rs, 1, "hello");
      validateRecord(rs, 2, "world");
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void loadDataInfileEmpty() throws SQLException, IOException {
    Assume.assumeFalse((!isMariadbServer() && minVersion(8, 0, 3)));
    Assume.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    // Create temp file.
    File temp = File.createTempFile("validateInfile", ".tmp");
    try (Connection connection = setConnection("&allowLocalInfile=true")) {
      Statement st = connection.createStatement();
      st.execute(
          "LOAD DATA LOCAL INFILE '"
              + temp.getAbsolutePath().replace('\\', '/')
              + "' INTO TABLE ldinfile");
      try (ResultSet rs = st.executeQuery("SELECT * FROM ldinfile")) {
        assertFalse(rs.next());
      }
    } finally {
      temp.delete();
    }
  }

  @Test
  public void testPrepareLocalInfileWithoutInputStream() throws SQLException {
    Assume.assumeFalse((!isMariadbServer() && minVersion(8, 0, 3)));
    Assume.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    try (Connection connection = setConnection("&allowLocalInfile=true")) {
      try {
        PreparedStatement st =
            connection.prepareStatement(
                "LOAD DATA LOCAL INFILE 'validateInfile.tsv' INTO TABLE ldinfile");
        st.execute();
        fail();
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Could not send file"));
        // check that connection is alright
        try {
          assertFalse(connection.isClosed());
          Statement st = connection.createStatement();
          st.execute("SELECT 1");
        } catch (SQLException eee) {
          fail();
        }
      }
    }
  }

  private void validateRecord(ResultSet rs, int expectedId, String expectedTest)
      throws SQLException {
    assertTrue(rs.next());

    int id = rs.getInt(1);
    String test = rs.getString(2);
    assertEquals(expectedId, id);
    assertEquals(expectedTest, test);
  }

  private File createTmpData(long recordNumber) throws Exception {
    File file = File.createTempFile("./infile" + recordNumber, ".tmp");

    // write it
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      // Every row is 8 bytes to make counting easier
      for (long i = 0; i < recordNumber; i++) {
        writer.write("\"a\",\"b\"");
        writer.write("\n");
      }
    }

    return file;
  }

  private void checkBigLocalInfile(long fileSize) throws Exception {
    long recordNumber = fileSize / 8;
    try (Connection connection = setConnection("&allowLocalInfile=true")) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("truncate `infile`");
        File file = createTmpData(recordNumber);

        try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
          MariaDbStatement stmt = statement.unwrap(MariaDbStatement.class);
          stmt.setLocalInfileInputStream(is);
          int insertNumber =
              stmt.executeUpdate(
                  "LOAD DATA LOCAL INFILE 'ignoredFileName' "
                      + "INTO TABLE `infile` "
                      + "COLUMNS TERMINATED BY ',' ENCLOSED BY '\\\"' ESCAPED BY '\\\\' "
                      + "LINES TERMINATED BY '\\n' (`a`, `b`)");
          assertEquals(insertNumber, recordNumber);
        }
        file.delete();
        statement.setFetchSize(1000); // to avoid using too much memory for tests
        try (ResultSet rs = statement.executeQuery("SELECT * FROM `infile`")) {
          for (int i = 0; i < recordNumber; i++) {
            assertTrue("record " + i + " doesn't exist", rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("b", rs.getString(2));
          }
          assertFalse(rs.next());
        }
      }
    }
  }

  /**
   * CONJ-375 : error with local infile with size > 16mb.
   *
   * @throws Exception if error occus
   */
  @Test
  public void testSmallBigLocalInfileInputStream() throws Exception {
    Assume.assumeFalse((!isMariadbServer() && minVersion(8, 0, 3)));
    Assume.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    checkBigLocalInfile(256);
  }

  @Test
  public void test2xBigLocalInfileInputStream() throws Exception {
    Assume.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    Assume.assumeFalse((!isMariadbServer() && minVersion(8, 0, 3)));
    Assume.assumeTrue(checkMaxAllowedPacketMore40m("test2xBigLocalInfileInputStream"));
    checkBigLocalInfile(16777216 * 2);
  }

  @Test
  public void testMoreThanMaxAllowedPacketLocalInfileInputStream() throws Exception {
    Assume.assumeFalse((!isMariadbServer() && minVersion(8, 0, 3)));
    Assume.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    Assume.assumeFalse(sharedIsAurora());
    Statement stmt = sharedConnection.createStatement();
    ResultSet rs = stmt.executeQuery("select @@max_allowed_packet");
    assertTrue(rs.next());
    long maxAllowedPacket = rs.getLong(1);
    Assume.assumeTrue(maxAllowedPacket < 100_000_000);
    checkBigLocalInfile(maxAllowedPacket + 1024);
  }

  @Test
  public void loadDataBasicWindows() throws Exception {
    Assume.assumeTrue(checkLocal());
    Assume.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    File temp = File.createTempFile("validateInfiledummyloadDataBasic", ".txt");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp))) {
      bw.write("1\thello2\n2\tworld\n");
    }

    try (Connection con = setConnection("allowLocalInfile")) {
      Statement stmt = con.createStatement();
      stmt.execute("TRUNCATE LocalInfileInputStreamTest2");
      stmt.execute(
          "LOAD DATA LOCAL INFILE '"
              + temp.getCanonicalPath().replace("\\", "\\\\")
              + "' INTO TABLE LocalInfileInputStreamTest2 (id, test)");
      ResultSet rs = stmt.executeQuery("SELECT * FROM LocalInfileInputStreamTest2");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertEquals("hello2", rs.getString(2));
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertEquals("world", rs.getString(2));
      while (rs.next()) {
        System.out.println(rs.getString(2));
      }
      assertFalse(rs.next());

    } finally {
      temp.delete();
    }
  }
}
