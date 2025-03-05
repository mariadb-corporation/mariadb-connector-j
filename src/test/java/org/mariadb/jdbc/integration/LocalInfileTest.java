// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.sql.*;
import java.util.Locale;
import org.junit.jupiter.api.*;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class LocalInfileTest extends Common {
  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Assumptions.assumeTrue(!isXpand());
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE LocalInfileInputStreamTest(id int, test varchar(100))");
    stmt.execute("CREATE TABLE LocalInfileInputStreamTest2(id int, test varchar(100))");
    stmt.execute("CREATE TABLE ttlocal(id int, test varchar(100))");
    stmt.execute("CREATE TABLE ldinfile(a varchar(10))");
    stmt.execute(
        "CREATE TABLE `infile`(`a` varchar(50) DEFAULT NULL, `b` varchar(50) DEFAULT NULL)"
            + " ENGINE=InnoDB DEFAULT CHARSET=latin1");
    stmt.execute(
        "CREATE TABLE small_load_data_infile(id int not null primary key auto_increment, name"
            + " char(20)) ENGINE=myisam");
    stmt.execute(
        "CREATE TABLE big_load_data_infile(id int not null primary key auto_increment, name"
            + " char(20)) ENGINE=myisam");
    stmt.execute("FLUSH TABLES");
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS AllowMultiQueriesTest");
    stmt.execute("DROP TABLE IF EXISTS LocalInfileInputStreamTest");
    stmt.execute("DROP TABLE IF EXISTS LocalInfileInputStreamTest2");
    stmt.execute("DROP TABLE IF EXISTS ttlocal");
    stmt.execute("DROP TABLE IF EXISTS ldinfile");
    stmt.execute("DROP TABLE IF EXISTS `infile`");
    stmt.execute("DROP TABLE IF EXISTS big_load_data_infile");
    stmt.execute("DROP TABLE IF EXISTS small_load_data_infile");
  }

  private static boolean checkLocal() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT @@local_infile");
    if (rs.next()) {
      return rs.getInt(1) == 1;
    }
    return false;
  }

  @Test
  public void smallLoadDataInfileTest() throws SQLException, IOException {
    Assumptions.assumeFalse((!isMariaDBServer() && minVersion(8, 0, 3)));
    try (VeryLongAutoGeneratedInputStream in = new VeryLongAutoGeneratedInputStream(50)) {
      try (Connection connection = createCon()) {
        Statement statement = connection.createStatement();
        org.mariadb.jdbc.Statement mariaDbStatement =
            statement.unwrap(org.mariadb.jdbc.Statement.class);
        mariaDbStatement.setLocalInfileInputStream(in);

        String sql =
            "LOAD DATA LOCAL INFILE 'dummyFileName'"
                + " INTO TABLE small_load_data_infile "
                + " FIELDS TERMINATED BY '\\t' ENCLOSED BY ''"
                + " ESCAPED BY '\\\\' LINES TERMINATED BY '\\n'";
        statement.execute(sql);

        ResultSet rs = statement.executeQuery("select count(*) from small_load_data_infile");
        assertTrue(rs.next());
        assertEquals(50, rs.getInt(1));
      }
    }
  }

  @Test
  public void bigLoadDataInfileTest() throws SQLException, IOException {
    Assumptions.assumeTrue(runLongTest());
    ResultSet rs1 = sharedConn.createStatement().executeQuery("select @@max_allowed_packet");
    assertTrue(rs1.next());
    long maxAllowedPacket = rs1.getLong(1);
    Assumptions.assumeTrue(maxAllowedPacket > 100_000_000);

    try (VeryLongAutoGeneratedInputStream in = new VeryLongAutoGeneratedInputStream(5000000)) {
      try (Connection connection = createCon("&allowLocalInfile=true")) {
        Statement statement = connection.createStatement();
        org.mariadb.jdbc.Statement mariaDbStatement =
            statement.unwrap(org.mariadb.jdbc.Statement.class);
        mariaDbStatement.setLocalInfileInputStream(in);

        String sql =
            "LOAD DATA LOCAL INFILE 'dummyFileName'"
                + " INTO TABLE big_load_data_infile "
                + " FIELDS TERMINATED BY '\\t' ENCLOSED BY ''"
                + " ESCAPED BY '\\\\' LINES TERMINATED BY '\\n'";

        assertFalse(statement.execute(sql));
        ResultSet rs = statement.executeQuery("select count(*) from big_load_data_infile");
        assertTrue(rs.next());
        assertEquals(in.numberOfRows, rs.getInt(1));
      }
    }
  }

  @Test
  public void streamInBatch() throws SQLException {
    Assumptions.assumeFalse((!isMariaDBServer() && minVersion(8, 0, 3)));
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    String batch_update =
        "LOAD DATA LOCAL INFILE 'dummy.tsv' INTO TABLE LocalInfileInputStreamTest2 (id, test)";
    String builder = "1\thello\n2\tworld\n";

    org.mariadb.jdbc.Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE LocalInfileInputStreamTest2");
    InputStream inputStream = new ByteArrayInputStream(builder.getBytes());
    stmt.setLocalInfileInputStream(inputStream);
    stmt.addBatch(batch_update);
    stmt.addBatch("SET UNIQUE_CHECKS=1");
    stmt.executeBatch();
    stmt.addBatch(batch_update);
    try {
      stmt.executeBatch();
    } catch (SQLException e) {
      assertTrue(e.getCause().getCause() instanceof FileNotFoundException);
    }

    try (PreparedStatement prep =
        sharedConn.prepareStatement(
            "LOAD DATA LOCAL INFILE 'dummy.tsv' INTO TABLE LocalInfileInputStreamTest2 (id,"
                + " test)")) {
      inputStream = new ByteArrayInputStream(builder.getBytes());
      ((org.mariadb.jdbc.Statement) prep).setLocalInfileInputStream(inputStream);
      prep.addBatch();
      prep.executeBatch();
      try {
        prep.addBatch();
        prep.executeBatch();
      } catch (SQLException e) {
        assertTrue(e.getCause().getCause() instanceof FileNotFoundException);
      }
    }
    try (PreparedStatement prep =
        sharedConnBinary.prepareStatement(
            "LOAD DATA LOCAL INFILE 'dummy.tsv' INTO TABLE LocalInfileInputStreamTest2 (id,"
                + " test)")) {
      inputStream = new ByteArrayInputStream(builder.getBytes());
      ((org.mariadb.jdbc.Statement) prep).setLocalInfileInputStream(inputStream);
      prep.addBatch();
      prep.executeBatch();
      try {
        prep.addBatch();
        prep.executeBatch();
      } catch (SQLException e) {
        assertTrue(e.getCause().getCause() instanceof FileNotFoundException);
      }
    }
  }

  @Test
  public void throwExceptions() throws Exception {
    Assumptions.assumeTrue(
        (isMariaDBServer() || !minVersion(8, 0, 3))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));

    // https://jira.mariadb.org/browse/XPT-270
    Assumptions.assumeFalse(isXpand());

    try (Connection con = createCon("&allowLocalInfile=false")) {
      Statement stmt = con.createStatement();
      stmt.execute("TRUNCATE LocalInfileInputStreamTest2");
      Common.assertThrowsContains(
          SQLException.class,
          () ->
              stmt.execute(
                  "LOAD DATA LOCAL INFILE 'someFile' INTO TABLE LocalInfileInputStreamTest2 (id,"
                      + " test)"),
          "Local infile is disabled by connector. Enable `allowLocalInfile` to allow local infile"
              + " commands");
      stmt.addBatch(
          "LOAD DATA LOCAL INFILE 'someFile' INTO TABLE LocalInfileInputStreamTest2 (id, test)");
      stmt.addBatch("SET UNIQUE_CHECKS=1");

      try {
        stmt.executeBatch();
        fail();
      } catch (SQLException e) {
        assertEquals(e.getClass(), BatchUpdateException.class);
        assertTrue(
            e.getMessage()
                .contains(
                    "Local infile is disabled by connector. Enable `allowLocalInfile` to allow"
                        + " local infile commands"));
        assertNotNull(e.getCause());
        assertEquals(e.getCause().getMessage(), e.getMessage());
        assertEquals(((SQLException) e.getCause()).getSQLState(), e.getSQLState());
        assertEquals(((SQLException) e.getCause()).getErrorCode(), e.getErrorCode());
      }

      try (PreparedStatement prep =
          con.prepareStatement(
              "LOAD DATA LOCAL INFILE ? INTO TABLE LocalInfileInputStreamTest2 (id, test)")) {
        prep.setString(1, "someFile");
        Common.assertThrowsContains(
            SQLException.class,
            prep::execute,
            "Local infile is disabled by connector. Enable `allowLocalInfile` to allow local infile"
                + " commands");
      }
    }
  }

  @Test
  public void wrongFile() throws Exception {
    Assumptions.assumeTrue(checkLocal());
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));

    try (Connection con = createCon("allowLocalInfile")) {
      Statement stmt = con.createStatement();
      Common.assertThrowsContains(
          SQLException.class,
          () ->
              stmt.execute(
                  "LOAD DATA LOCAL INFILE 'someFile' INTO TABLE LocalInfileInputStreamTest2 (id,"
                      + " test)"),
          "Could not send file : someFile");
      assertTrue(con.isValid(1));
    }
  }

  @Test
  public void unReadableFile() throws Exception {
    Assumptions.assumeTrue(checkLocal());
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv"))
            && !System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win"));

    try (Connection con = createCon("allowLocalInfile")) {
      File tempFile = File.createTempFile("hello", ".tmp");
      tempFile.deleteOnExit();
      tempFile.setReadable(false);
      Statement stmt = con.createStatement();
      Common.assertThrowsContains(
          SQLException.class,
          () ->
              stmt.execute(
                  "LOAD DATA LOCAL INFILE '"
                      + tempFile.getCanonicalPath().replace("\\", "/")
                      + "' INTO TABLE LocalInfileInputStreamTest2 (id, test)"),
          "Could not send file");
      assertTrue(con.isValid(1));
    }
  }

  @Test
  public void loadDataBasic() throws Exception {
    Assumptions.assumeTrue(checkLocal());
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    File temp = File.createTempFile("dummyloadDataBasic", ".txt");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp))) {
      bw.write("1\thello2\n2\tworld\n");
    }

    try (Connection con = createCon("allowLocalInfile")) {
      Statement stmt = con.createStatement();
      stmt.execute("TRUNCATE LocalInfileInputStreamTest2");
      stmt.execute(
          "LOAD DATA LOCAL INFILE '"
              + temp.getCanonicalPath().replace("\\", "/")
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

      stmt.execute("TRUNCATE LocalInfileInputStreamTest2");
      stmt.addBatch(
          "LOAD DATA LOCAL INFILE '"
              + temp.getCanonicalPath().replace("\\", "/")
              + "' INTO TABLE LocalInfileInputStreamTest2 (id, test)");
      stmt.addBatch("SET UNIQUE_CHECKS=1");
      stmt.executeBatch();

      rs = stmt.executeQuery("SELECT * FROM LocalInfileInputStreamTest2");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertEquals("hello2", rs.getString(2));
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertEquals("world", rs.getString(2));
      assertFalse(rs.next());
    } finally {
      temp.delete();
    }
  }

  @Test
  public void loadDataBasicMultiRows() throws Exception {

    Assumptions.assumeTrue(checkLocal());
    File temp = File.createTempFile("dummyloadDataBasic2", ".txt");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp))) {
      bw.write("1\thello2\n2\tworld\n");
    }

    try (Connection con = createCon("allowLocalInfile&allowMultiQueries")) {
      Statement stmt = con.createStatement();
      stmt.execute("TRUNCATE LocalInfileInputStreamTest2");
      stmt.execute(
          "SELECT 1;LOAD DATA LOCAL INFILE '"
              + temp.getCanonicalPath().replace("\\", "/")
              + "' INTO TABLE LocalInfileInputStreamTest2 (id, test); SELECT 2");
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

      stmt.execute("TRUNCATE LocalInfileInputStreamTest2");
      stmt.addBatch(
          "SELECT 1;LOAD DATA LOCAL INFILE '"
              + temp.getCanonicalPath().replace("\\", "/")
              + "' INTO TABLE LocalInfileInputStreamTest2 (id, test)");
      stmt.addBatch("SET UNIQUE_CHECKS=1");
      stmt.executeBatch();

      rs = stmt.executeQuery("SELECT * FROM LocalInfileInputStreamTest2");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertEquals("hello2", rs.getString(2));
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertEquals("world", rs.getString(2));
      assertFalse(rs.next());
    } finally {
      temp.delete();
    }
  }

  @Test
  public void loadDataBasicWindows() throws Exception {
    Assumptions.assumeTrue(checkLocal());
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    File temp = File.createTempFile("dummyloadDataBasic", ".txt");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp))) {
      bw.write("1\thello2\n2\tworld\n");
    }

    try (Connection con = createCon("allowLocalInfile")) {
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

      stmt.execute("TRUNCATE LocalInfileInputStreamTest2");
      stmt.addBatch(
          "LOAD DATA LOCAL INFILE '"
              + temp.getCanonicalPath().replace("\\", "\\\\")
              + "' INTO TABLE LocalInfileInputStreamTest2 (id, test)");
      stmt.addBatch("SET UNIQUE_CHECKS=1");
      stmt.executeBatch();

      rs = stmt.executeQuery("SELECT * FROM LocalInfileInputStreamTest2");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertEquals("hello2", rs.getString(2));
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertEquals("world", rs.getString(2));
      assertFalse(rs.next());
    } finally {
      temp.delete();
    }
  }

  @Test
  public void loadDataValidationFails() throws Exception {
    Assumptions.assumeTrue(checkLocal());
    loadDataValidationFails(false);
    loadDataValidationFails(true);
  }

  public void loadDataValidationFails(boolean prepStmt) throws Exception {
    File temp = File.createTempFile("dummy", ".txt");
    File tempXml = File.createTempFile("xmldummy", ".txt");

    try (Connection con = createCon("&allowLocalInfile&useServerPrepStmts=" + prepStmt)) {
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp))) {
        bw.write("1\thello\n2\tworld\n");
      }
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(tempXml))) {
        bw.write("<row id=\"1\" test=\"hello\" />\n<row id=\"2\" test=\"world\" />\n");
      }
      try (PreparedStatement prep =
          con.prepareStatement(
              "LOAD DATA LOCAL INFILE ? INTO TABLE LocalInfileInputStreamTest2 (id, test)")) {
        prep.setString(1, temp.getCanonicalPath().replace("\\", "/"));
        prep.execute();
      }
      try (PreparedStatement prep =
          con.prepareStatement(
              "LOAD XML LOCAL INFILE ? INTO TABLE LocalInfileInputStreamTest2 (id, test)")) {
        prep.setString(1, tempXml.getCanonicalPath().replace("\\", "/"));
        prep.execute();
      }
      try (PreparedStatement prep =
          con.prepareStatement(
              "/* test */ LOAD  DATA LOCAL INFILE 'j' INTO TABLE LocalInfileInputStreamTest2 (id,"
                  + " test)")) {
        assertThrowsContains(SQLException.class, () -> prep.execute(), "Could not send file : j");
      }
      // special test comment inside LOAD DATA LOCAL are not checked, resulting in error
      try (PreparedStatement prep =
          con.prepareStatement(
              "LOAD /**g*/ DATA LOCAL INFILE 'h' INTO TABLE LocalInfileInputStreamTest2 (id,"
                  + " test)")) {
        assertThrowsContains(
            SQLException.class,
            () -> prep.execute(),
            "LOAD DATA LOCAL INFILE asked for file 'h' that doesn't correspond to initial query ");
      }
      // ensure connection state after errors
      ResultSet rs = con.createStatement().executeQuery("SELECT 1");
      rs.next();
      assertEquals(1, rs.getInt(1));
    } finally {
      temp.delete();
      tempXml.delete();
    }
  }

  @Test
  public void loadDataInfileEmpty() throws SQLException, IOException {
    Assumptions.assumeTrue(
        (isMariaDBServer() || !minVersion(8, 0, 3))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    // Create temp file.
    File temp = File.createTempFile("validateInfile", ".tmp");
    try (Connection connection = createCon("&allowLocalInfile=true")) {
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

  private File createTmpData(long recordNumber) throws Exception {
    File file = File.createTempFile("infile" + recordNumber, ".tmp");

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
    try (Connection connection = createCon("allowLocalInfile")) {
      Statement stmt = connection.createStatement();
      stmt.execute("truncate `infile`");
      File file = createTmpData(recordNumber);
      int insertNumber =
          stmt.executeUpdate(
              "LOAD DATA LOCAL INFILE '"
                  + file.getCanonicalPath().replace("\\", "/")
                  + "' "
                  + "INTO TABLE `infile` "
                  + "COLUMNS TERMINATED BY ',' ENCLOSED BY '\\\"' ESCAPED BY '\\\\' "
                  + "LINES TERMINATED BY '\\n' (`a`, `b`)");
      assertEquals(insertNumber, recordNumber);
      file.delete();
      stmt.setFetchSize(1000); // to avoid using too much memory for tests
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM `infile`")) {
        for (int i = 0; i < recordNumber; i++) {
          assertTrue(rs.next());
          assertEquals("a", rs.getString(1));
          assertEquals("b", rs.getString(2));
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testSmallBigLocalInfileInputStream() throws Exception {
    Assumptions.assumeTrue(
        (isMariaDBServer() || !minVersion(8, 0, 3))
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    checkBigLocalInfile(256);
  }

  @Test
  public void test2xBigLocalInfileInputStream() throws Exception {
    Assumptions.assumeTrue(
        ((isMariaDBServer() || !minVersion(8, 0, 3)) && runLongTest())
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    checkBigLocalInfile(16777216 * 2);
  }

  @Test
  public void testMoreThanMaxAllowedPacketLocalInfileInputStream() throws Exception {
    Assumptions.assumeTrue(isMariaDBServer() || !minVersion(8, 0, 3));
    Assumptions.assumeTrue(runLongTest());
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("select @@max_allowed_packet");
    assertTrue(rs.next());
    long maxAllowedPacket = rs.getLong(1);
    Assumptions.assumeTrue(maxAllowedPacket < 10_000_000);
    checkBigLocalInfile(maxAllowedPacket + 1024);
  }

  /** Custom memory conserving generator of a LOAD DATA INFILE that generates a stream. */
  private static class VeryLongAutoGeneratedInputStream extends InputStream {

    private final int numberOfRows;
    private int currentPosInBuffer;
    private byte[] buffer;
    private int currentRow;

    private VeryLongAutoGeneratedInputStream(int numberOfRows) {
      this.numberOfRows = numberOfRows;
      currentRow = 0;
    }

    @Override
    public int read() {
      if (currentRow > numberOfRows) {
        return -1;
      }
      if (buffer != null && currentPosInBuffer >= buffer.length) {
        buffer = null;
      }
      if (buffer == null) {
        currentRow++;
        currentPosInBuffer = 0;
        buffer = (currentRow + "\tname" + currentRow + "\n").getBytes();
      }
      return buffer[currentPosInBuffer++];
    }
  }
}
