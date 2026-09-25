// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.integration.resultset;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.result.SequentialBlob;
import org.mariadb.jdbc.client.result.SequentialClob;
import org.mariadb.jdbc.client.result.SequentialResult;
import org.mariadb.jdbc.integration.Common;

public class SequentialResultSetTest extends Common {

  private static final int BLOB_SIZE = 1_000_000;
  private static final byte[] BLOB1 = new byte[BLOB_SIZE];
  private static final byte[] BLOB2 = new byte[300_000];
  private static final String TEXT1;
  private static final String TEXT2 = "second row €€€ 😀 text";

  static {
    new Random(42).nextBytes(BLOB1);
    new Random(43).nextBytes(BLOB2);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100_000; i++) sb.append("abcdéfghij€😀");
    TEXT1 = sb.toString();
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS seqLob");
    stmt.execute("DROP TABLE IF EXISTS seqTypes");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE seqLob (id int not null primary key, b LONGBLOB, t LONGTEXT, i2 int, n"
            + " int)");
    try (PreparedStatement prep =
        sharedConn.prepareStatement("INSERT INTO seqLob VALUES (?, ?, ?, ?, ?)")) {
      prep.setInt(1, 1);
      prep.setBytes(2, BLOB1);
      prep.setString(3, TEXT1);
      prep.setInt(4, 10);
      prep.setNull(5, Types.INTEGER);
      prep.addBatch();
      prep.setInt(1, 2);
      prep.setBytes(2, BLOB2);
      prep.setString(3, TEXT2);
      prep.setInt(4, 20);
      prep.setInt(5, 200);
      prep.addBatch();
      prep.setInt(1, 3);
      prep.setNull(2, Types.BLOB);
      prep.setNull(3, Types.CLOB);
      prep.setInt(4, 30);
      prep.setNull(5, Types.INTEGER);
      prep.addBatch();
      prep.setInt(1, 4);
      prep.setBytes(2, new byte[0]);
      prep.setString(3, "");
      prep.setInt(4, 40);
      prep.setInt(5, 400);
      prep.addBatch();
      prep.executeBatch();
    }
    stmt.execute(
        "CREATE TABLE seqTypes (t1 tinyint, t2 smallint, t3 mediumint, t4 int, t5 bigint, t6"
            + " float, t7 double, t8 decimal(10,3), t9 date, t10 time(3), t11 datetime(6), t12"
            + " year, t13 varchar(20), t14 char(5), t15 bit(8), t16 blob, t17 text, t18 tinyint"
            + " unsigned, t19 bigint unsigned, t20 int)");
    stmt.execute(
        "INSERT INTO seqTypes VALUES (-1, -300, -70000, -2000000000, -9000000000000000000, 1.5,"
            + " 2.25, 123.456, '2020-01-02', '10:11:12.123', '2021-03-04 05:06:07.123456', 2022,"
            + " 'varchar', 'char', b'10101010', 'blob', 'text', 250, 18446744073709551615, 42),"
            + " (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,"
            + " NULL, NULL, NULL, NULL, NULL, NULL, 43)");
  }

  private static Statement sequentialStmt(java.sql.Connection con) throws SQLException {
    Statement stmt = (Statement) con.createStatement();
    stmt.setFetchSize(Integer.MIN_VALUE);
    return stmt;
  }

  private static byte[] readAll(InputStream is) throws IOException {
    return is.readAllBytes();
  }

  private static String readAll(Reader reader) throws IOException {
    StringBuilder sb = new StringBuilder();
    char[] buf = new char[8192];
    int len;
    while ((len = reader.read(buf)) >= 0) sb.append(buf, 0, len);
    return sb.toString();
  }

  @Test
  public void streamRows() throws Exception {
    streamRows(sharedConn);
    streamRows(sharedConnBinary);
    try (Connection con = createCon("useServerPrepStmts=true")) {
      try (PreparedStatement prep =
          con.prepareStatement("SELECT * FROM seqLob WHERE id >= ? ORDER BY id")) {
        prep.setFetchSize(Integer.MIN_VALUE);
        prep.setInt(1, 1);
        checkRows(prep.executeQuery());
      }
    }
  }

  private void streamRows(Connection con) throws Exception {
    try (Statement stmt = sequentialStmt(con)) {
      ResultSet rs = stmt.executeQuery("SELECT * FROM seqLob ORDER BY id");
      assertInstanceOf(SequentialResult.class, rs);
      assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
      assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
      assertEquals(Integer.MIN_VALUE, rs.getFetchSize());
      assertEquals(Integer.MIN_VALUE, stmt.getFetchSize());
      assertTrue(rs.isBeforeFirst());
      assertFalse(rs.isAfterLast());
      checkRows(rs);
    }
  }

  private void checkRows(ResultSet rs) throws Exception {
    assertTrue(rs.next());
    assertTrue(rs.isFirst());
    assertEquals(1, rs.getRow());
    assertEquals(1, rs.getInt(1));
    assertEquals(1, rs.getInt("id")); // same buffered column can be read again
    assertEquals("1", rs.getString(1));
    try (InputStream is = rs.getBinaryStream(2)) {
      assertArrayEquals(BLOB1, readAll(is));
    }
    assertFalse(rs.wasNull());
    try (Reader reader = rs.getCharacterStream(3)) {
      assertEquals(TEXT1, readAll(reader));
    }
    assertEquals(10, rs.getInt(4));
    assertEquals(0, rs.getInt(5));
    assertTrue(rs.wasNull());

    assertTrue(rs.next());
    assertFalse(rs.isFirst());
    assertEquals(2, rs.getRow());
    // skip column 1, read a blob as Blob
    Blob blob = rs.getBlob(2);
    assertInstanceOf(SequentialBlob.class, blob);
    assertEquals(BLOB2.length, blob.length());
    assertArrayEquals(Arrays.copyOfRange(BLOB2, 0, 10), blob.getBytes(1, 10));
    // forward read
    assertArrayEquals(Arrays.copyOfRange(BLOB2, 100, 110), blob.getBytes(101, 10));
    // backward read not permitted
    assertThrowsContains(SQLException.class, () -> blob.getBytes(50, 10), "already been consumed");
    try (InputStream is = blob.getBinaryStream(201, 100)) {
      assertArrayEquals(Arrays.copyOfRange(BLOB2, 200, 300), readAll(is));
    }
    assertArrayEquals(
        Arrays.copyOfRange(BLOB2, 300, BLOB2.length), readAll(blob.getBinaryStream()));
    assertEquals(-1, blob.getBinaryStream().read());
    Clob clob = rs.getClob(3);
    assertInstanceOf(SequentialClob.class, clob);
    assertEquals(TEXT2.substring(0, 6), clob.getSubString(1, 6));
    assertEquals(TEXT2.substring(11, 14), clob.getSubString(12, 3));
    assertThrowsContains(
        SQLException.class, () -> clob.getSubString(1, 3), "already been consumed");
    assertEquals(TEXT2.substring(14), readAll(clob.getCharacterStream()));
    assertEquals(20, rs.getInt(4));
    assertEquals(200, rs.getInt(5));
    assertFalse(rs.wasNull());

    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertNull(rs.getBinaryStream(2));
    assertTrue(rs.wasNull());
    // a NULL streamed column can be read again
    assertNull(rs.getBlob(2));
    assertTrue(rs.wasNull());
    assertNull(rs.getBytes(2));
    assertTrue(rs.wasNull());
    assertNull(rs.getClob(3));
    assertTrue(rs.wasNull());
    assertNull(rs.getString(3));
    assertTrue(rs.wasNull());
    assertEquals(30, rs.getInt(4));

    assertTrue(rs.next());
    assertEquals(4, rs.getInt(1));
    assertArrayEquals(new byte[0], readAll(rs.getBinaryStream(2)));
    assertFalse(rs.wasNull());
    assertEquals("", readAll(rs.getCharacterStream(3)));
    assertEquals(40, rs.getInt(4));
    assertEquals(400, rs.getInt(5));

    assertFalse(rs.next());
    assertTrue(rs.isAfterLast());
    assertEquals(0, rs.getRow());
    assertFalse(rs.next());
    assertThrowsContains(SQLException.class, () -> rs.getInt(1), "wrong row position");
    rs.close();
  }

  @Test
  public void compressionAndTls() throws Exception {
    try (Connection con = createCon("useCompression=true")) {
      streamRows(con);
    }
    try (Connection con = createCon("useCompression=true&useServerPrepStmts=true")) {
      streamRows(con);
    }
    if (haveSsl()) {
      try (Connection con = createCon("sslMode=trust")) {
        streamRows(con);
      }
      try (Connection con = createCon("sslMode=trust&useCompression=true")) {
        streamRows(con);
      }
    }
  }

  @Test
  public void accessOrder() throws Exception {
    accessOrder(sharedConn);
    accessOrder(sharedConnBinary);
  }

  private void accessOrder(Connection con) throws Exception {
    try (Statement stmt = sequentialStmt(con)) {
      ResultSet rs = stmt.executeQuery("SELECT * FROM seqLob ORDER BY id");
      assertTrue(rs.next());
      assertEquals(10, rs.getInt(4));
      // going backward is not permitted
      assertThrowsContains(SQLException.class, () -> rs.getInt(1), "increasing index order");
      assertThrowsContains(
          SQLException.class, () -> rs.getBinaryStream(2), "increasing index order");
      assertThrowsContains(SQLException.class, () -> rs.getInt(0), "Wrong index position");
      assertThrowsContains(SQLException.class, () -> rs.getInt(6), "Wrong index position");
      assertEquals(10, rs.getInt(4)); // same column again is fine
      assertEquals(0, rs.getInt(5));
      assertTrue(rs.wasNull());

      assertTrue(rs.next());
      InputStream is = rs.getBinaryStream(2);
      assertEquals(BLOB2[0] & 0xff, is.read());
      // a streamed column cannot be read again
      assertThrowsContains(
          SQLException.class, () -> rs.getBytes(2), "already been read as a stream");
      // reading a following column invalidates the stream, remaining bytes being skipped
      assertEquals(TEXT2, rs.getString(3));
      assertThrowsContains(IOException.class, is::read, "not readable anymore");
      assertEquals(20, rs.getInt(4));

      assertTrue(rs.next());
      assertTrue(rs.next());
      Blob blob = rs.getBlob(2);
      assertEquals(0, blob.length());
      // moving to next row invalidates the blob
      assertFalse(rs.next());
      assertThrowsContains(IOException.class, () -> blob.getBinaryStream().read(), "not readable");
    }
  }

  @Test
  public void stringGettersOnLob() throws Exception {
    // regular getters load the column: same result as a standard result-set
    for (Connection con : new Connection[] {sharedConn, sharedConnBinary}) {
      try (Statement stmt = sequentialStmt(con)) {
        ResultSet rs = stmt.executeQuery("SELECT * FROM seqLob ORDER BY id");
        assertTrue(rs.next());
        assertArrayEquals(BLOB1, rs.getBytes(2));
        assertArrayEquals(BLOB1, rs.getBytes(2));
        // once buffered, a stream is served from the buffer
        assertArrayEquals(BLOB1, readAll(rs.getBinaryStream(2)));
        assertEquals(TEXT1, rs.getString(3));
        assertEquals(TEXT1, readAll(rs.getCharacterStream(3)));
        assertEquals(TEXT1, rs.getClob(3).getSubString(1, TEXT1.length()));
        assertTrue(rs.next());
        assertEquals(TEXT2, rs.getObject(3, String.class));
        assertTrue(rs.next());
        assertNull(rs.getBytes(2));
        assertTrue(rs.wasNull());
        assertNull(rs.getString(3));
        assertTrue(rs.wasNull());
        assertFalse(rs.wasNull() && rs.getInt(4) == 0);
      }
    }
  }

  @Test
  public void getObjectStreams() throws Exception {
    try (Statement stmt = sequentialStmt(sharedConn)) {
      ResultSet rs = stmt.executeQuery("SELECT * FROM seqLob ORDER BY id");
      assertTrue(rs.next());
      assertArrayEquals(BLOB1, readAll(rs.getObject(2, InputStream.class)));
      assertEquals(TEXT1, readAll(rs.getObject(3, Reader.class)));
      assertTrue(rs.next());
      Blob blob = rs.getObject(2, Blob.class);
      assertInstanceOf(SequentialBlob.class, blob);
      assertArrayEquals(BLOB2, readAll(blob.getBinaryStream()));
      Clob clob = rs.getObject(3, Clob.class);
      assertInstanceOf(SequentialClob.class, clob);
      assertEquals(TEXT2, readAll(clob.getCharacterStream()));
      assertTrue(rs.next());
      assertNull(rs.getObject(2, Blob.class));
      assertNull(rs.getObject(3, NClob.class));
      assertTrue(rs.next());
      NClob nclob = rs.getNClob(3);
      assertEquals(0, nclob.length());
      assertEquals("", readAll(nclob.getCharacterStream()));
    }
  }

  @Test
  public void clobLength() throws Exception {
    for (Connection con : new Connection[] {sharedConn, sharedConnBinary}) {
      try (Statement stmt = sequentialStmt(con)) {
        ResultSet rs = stmt.executeQuery("SELECT * FROM seqLob ORDER BY id");
        assertTrue(rs.next());
        Clob clob = rs.getClob(3);
        // length needs the whole content: remaining data is loaded in memory
        assertEquals(TEXT1.length(), clob.length());
        assertEquals(TEXT1.substring(0, 20), clob.getSubString(1, 20));
        assertEquals(TEXT1.length(), clob.length());
        assertEquals(TEXT1.substring(20), readAll(clob.getCharacterStream()));
        assertTrue(rs.next());
        Clob clob2 = rs.getClob(3);
        assertEquals(TEXT2.substring(0, 5), clob2.getSubString(1, 5));
        assertEquals(TEXT2.length(), clob2.length());
        assertEquals(TEXT2.substring(5), readAll(clob2.getCharacterStream()));
        assertEquals(20, rs.getInt(4));
        assertThrowsContains(
            SQLFeatureNotSupportedException.class, () -> clob2.setString(1, "a"), "read-only");
        assertThrowsContains(
            SQLFeatureNotSupportedException.class, () -> clob2.position("a", 1), "not supported");
      }
    }
  }

  @Test
  public void blobReadOnly() throws Exception {
    try (Statement stmt = sequentialStmt(sharedConn)) {
      ResultSet rs = stmt.executeQuery("SELECT * FROM seqLob ORDER BY id");
      assertTrue(rs.next());
      Blob blob = rs.getBlob(2);
      assertThrowsContains(
          SQLFeatureNotSupportedException.class, () -> blob.setBytes(1, new byte[1]), "read-only");
      assertThrowsContains(
          SQLFeatureNotSupportedException.class, () -> blob.truncate(1), "read-only");
      assertThrowsContains(
          SQLFeatureNotSupportedException.class,
          () -> blob.position(new byte[1], 1),
          "not supported");
      assertThrowsContains(SQLException.class, () -> blob.getBytes(0, 1), "Position must be");
      assertThrowsContains(
          SQLException.class, () -> blob.getBinaryStream(1, BLOB_SIZE + 1), "exceeds");
      blob.free();
      assertThrowsContains(SQLException.class, blob::length, "freed");
      // remaining bytes are skipped
      assertEquals(10, rs.getInt(4));
    }
  }

  @Test
  public void interleavedCommand() throws Exception {
    // executing another command while a stream is half read loads remaining data in memory
    for (Connection con : new Connection[] {sharedConn, sharedConnBinary}) {
      try (Statement stmt = sequentialStmt(con)) {
        ResultSet rs = stmt.executeQuery("SELECT * FROM seqLob ORDER BY id");
        assertTrue(rs.next());
        InputStream is = rs.getBinaryStream(2);
        byte[] head = new byte[1000];
        assertEquals(1000, is.readNBytes(head, 0, 1000));
        assertArrayEquals(Arrays.copyOfRange(BLOB1, 0, 1000), head);
        try (Statement other = (Statement) con.createStatement()) {
          ResultSet rs2 = other.executeQuery("SELECT 'other'");
          assertTrue(rs2.next());
          assertEquals("other", rs2.getString(1));
        }
        assertArrayEquals(Arrays.copyOfRange(BLOB1, 1000, BLOB_SIZE), readAll(is));
        assertEquals(TEXT1, readAll(rs.getCharacterStream(3)));
        assertEquals(10, rs.getInt(4));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertArrayEquals(BLOB2, readAll(rs.getBinaryStream(2)));
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
        // after end, another statement while result not closed
        try (Statement other = (Statement) con.createStatement()) {
          ResultSet rs2 = other.executeQuery("SELECT 'other'");
          assertTrue(rs2.next());
        }
      }
    }
  }

  @Test
  public void closeBeforeEnd() throws Exception {
    for (Connection con : new Connection[] {sharedConn, sharedConnBinary}) {
      // result-set closed while a stream is half read
      Statement stmt = sequentialStmt(con);
      ResultSet rs = stmt.executeQuery("SELECT * FROM seqLob ORDER BY id");
      assertTrue(rs.next());
      InputStream is = rs.getBinaryStream(2);
      assertEquals(1000, is.readNBytes(new byte[1000], 0, 1000));
      rs.close();
      assertThrowsContains(IOException.class, is::read, "not readable anymore");
      assertThrowsContains(SQLException.class, rs::next, "closed");
      ResultSet rs2 = stmt.executeQuery("SELECT 1");
      assertTrue(rs2.next());
      assertEquals(1, rs2.getInt(1));

      // statement closed while a stream is half read
      rs = stmt.executeQuery("SELECT * FROM seqLob ORDER BY id");
      assertTrue(rs.next());
      is = rs.getBinaryStream(2);
      assertEquals(1000, is.readNBytes(new byte[1000], 0, 1000));
      stmt.close();
      assertThrowsContains(IOException.class, is::read, "not readable anymore");
      try (Statement other = (Statement) con.createStatement()) {
        ResultSet rs3 = other.executeQuery("SELECT 2");
        assertTrue(rs3.next());
        assertEquals(2, rs3.getInt(1));
      }

      // result-set closed without reading anything
      try (Statement st = sequentialStmt(con)) {
        st.executeQuery("SELECT * FROM seqLob ORDER BY id").close();
        ResultSet rs4 = st.executeQuery("SELECT 3");
        assertTrue(rs4.next());
        assertEquals(3, rs4.getInt(1));
      }
    }
  }

  @Test
  public void emptyResultAndMaxRows() throws Exception {
    try (Statement stmt = sequentialStmt(sharedConn)) {
      ResultSet rs = stmt.executeQuery("SELECT * FROM seqLob WHERE id > 100");
      assertFalse(rs.isAfterLast());
      assertFalse(rs.next());
      assertFalse(rs.isBeforeFirst());
      assertFalse(rs.isAfterLast());

      stmt.setMaxRows(2);
      rs = stmt.executeQuery("SELECT id FROM seqLob ORDER BY id");
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertFalse(rs.next());
      stmt.setMaxRows(0);

      rs = stmt.executeQuery("SELECT id FROM seqLob ORDER BY id");
      assertTrue(rs.relative(2));
      assertEquals(2, rs.getInt(1));
      assertTrue(rs.relative(0));
      assertFalse(rs.relative(5));
    }
  }

  @Test
  public void navigationNotPermitted() throws Exception {
    try (Statement stmt = sequentialStmt(sharedConn)) {
      ResultSet rs = stmt.executeQuery("SELECT id FROM seqLob ORDER BY id");
      assertTrue(rs.next());
      assertThrowsContains(SQLException.class, rs::beforeFirst, "not permit");
      assertThrowsContains(SQLException.class, rs::afterLast, "not permit");
      assertThrowsContains(SQLException.class, rs::first, "not permit");
      assertThrowsContains(SQLException.class, rs::last, "not permit");
      assertThrowsContains(SQLException.class, rs::previous, "not permit");
      assertThrowsContains(SQLException.class, () -> rs.absolute(1), "not permit");
      assertThrowsContains(SQLException.class, () -> rs.relative(-1), "not permit");
      assertThrowsContains(SQLException.class, rs::isLast, "not supported");
      assertThrowsContains(SQLException.class, () -> rs.updateInt(1, 1), "CONCUR_READ_ONLY");
      assertEquals(1, rs.getInt(1));
    }
    Statement stmt = sharedConn.createStatement();
    assertThrowsContains(SQLException.class, () -> stmt.setFetchSize(-2), "invalid fetch size");
    assertThrowsContains(SQLException.class, () -> stmt.setFetchSize(-1), "invalid fetch size");
    stmt.setFetchSize(Integer.MIN_VALUE);
    assertEquals(Integer.MIN_VALUE, stmt.getFetchSize());
    stmt.setFetchSize(0);
    // callable statements cannot use sequential access
    try (CallableStatement call = sharedConn.prepareCall("{call foo()}")) {
      assertThrowsContains(
          SQLException.class, () -> call.setFetchSize(Integer.MIN_VALUE), "invalid fetch size");
    }
    // sequential access needs a forward-only read-only statement: ignored otherwise
    try (Statement st =
        (Statement)
            sharedConn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
      st.setFetchSize(Integer.MIN_VALUE);
      ResultSet rs = st.executeQuery("SELECT id FROM seqLob ORDER BY id");
      assertFalse(rs instanceof SequentialResult);
      assertTrue(rs.last());
      assertEquals(4, rs.getInt(1));
    }
    try (Statement st =
        (Statement)
            sharedConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
      st.setFetchSize(Integer.MIN_VALUE);
      ResultSet rs = st.executeQuery("SELECT id FROM seqLob ORDER BY id");
      assertFalse(rs instanceof SequentialResult);
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
  }

  @Test
  public void contextStatusUpdated() throws Exception {
    // end of result-set packet updates connection status and warnings
    for (Connection con : new Connection[] {sharedConn, sharedConnBinary}) {
      try (Statement stmt = sequentialStmt(con)) {
        ResultSet rs = stmt.executeQuery("SELECT CAST('a' AS SIGNED), b FROM seqLob WHERE id=2");
        assertNull(stmt.getWarnings());
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertArrayEquals(BLOB2, readAll(rs.getBinaryStream(2)));
        assertFalse(rs.next());
        // getWarnings runs SHOW WARNINGS, which resets the count: one check only
        assertNotNull(rs.getWarnings());
        stmt.clearWarnings();
        // warning also read when remaining rows are skipped on close
        rs = stmt.executeQuery("SELECT CAST('a' AS SIGNED), b FROM seqLob ORDER BY id");
        assertTrue(rs.next());
        rs.close();
        assertNotNull(stmt.getWarnings());
        stmt.clearWarnings();
        // ... or loaded in memory by another command (whose own status then replaces it)
        rs = stmt.executeQuery("SELECT CAST('a' AS SIGNED), b FROM seqLob ORDER BY id");
        assertTrue(rs.next());
        try (Statement other = (Statement) con.createStatement()) {
          assertTrue(other.executeQuery("SELECT 1").next());
        }
        assertNull(stmt.getWarnings());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void multiResults() throws Exception {
    try (Connection con = createCon("allowMultiQueries=true")) {
      try (Statement stmt = sequentialStmt(con)) {
        assertTrue(
            stmt.execute(
                "SELECT id, b FROM seqLob ORDER BY id; SELECT 'a'; DO 1; SELECT i2 FROM seqLob"
                    + " ORDER BY id"));
        ResultSet rs = stmt.getResultSet();
        assertInstanceOf(SequentialResult.class, rs);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        InputStream is = rs.getBinaryStream(2);
        assertEquals(1000, is.readNBytes(new byte[1000], 0, 1000));
        // moving to next result discards the current one
        assertTrue(stmt.getMoreResults());
        rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertFalse(rs.next());
        assertFalse(stmt.getMoreResults());
        assertEquals(0, stmt.getUpdateCount());
        assertTrue(stmt.getMoreResults());
        rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
        assertFalse(stmt.getMoreResults());
        assertEquals(-1, stmt.getUpdateCount());
      }
    }
  }

  @Test
  public void defaultFetchSizeOption() throws Exception {
    try (Connection con = createCon("defaultFetchSize=" + Integer.MIN_VALUE)) {
      try (Statement stmt = (Statement) con.createStatement()) {
        assertEquals(Integer.MIN_VALUE, stmt.getFetchSize());
        ResultSet rs = stmt.executeQuery("SELECT * FROM seqLob ORDER BY id");
        assertInstanceOf(SequentialResult.class, rs);
        checkRows(rs);
      }
      try (Statement stmt =
          (Statement)
              con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
        assertInstanceOf(SequentialResult.class, stmt.executeQuery("SELECT 1"));
      }
      try (Statement stmt =
          (Statement)
              con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
        ResultSet rs = stmt.executeQuery("SELECT 1");
        assertFalse(rs instanceof SequentialResult);
      }
      try (PreparedStatement prep = con.prepareStatement("SELECT * FROM seqLob ORDER BY id")) {
        assertEquals(Integer.MIN_VALUE, prep.getFetchSize());
        ResultSet rs = prep.executeQuery();
        assertInstanceOf(SequentialResult.class, rs);
        checkRows(rs);
      }
      // driver's own queries are not sequential: metadata columns can be read in any order
      ResultSet rs = con.getMetaData().getColumns(null, null, "seqLob", null);
      assertFalse(rs instanceof SequentialResult);
      assertTrue(rs.next());
      assertEquals("id", rs.getString("COLUMN_NAME"));
      assertEquals("seqLob", rs.getString("TABLE_NAME"));
      assertEquals("id", rs.getString(4));
      // connection internal queries still work
      con.setCatalog(con.getCatalog());
      assertTrue(con.isValid(1));
      // callable statements keep standard result-sets
      try (CallableStatement call = con.prepareCall("{call foo()}")) {
        assertEquals(0, call.getFetchSize());
      }
    }
  }

  @Test
  public void allTypes() throws Exception {
    allTypes(sharedConn);
    allTypes(sharedConnBinary);
  }

  private void allTypes(Connection con) throws Exception {
    // compare each getter with a standard result-set, reading columns in order
    try (Statement stmt = sequentialStmt(con);
        Statement stmt2 = (Statement) con.createStatement()) {
      ResultSet rs = stmt.executeQuery("SELECT * FROM seqTypes");
      ResultSet expected = stmt2.executeQuery("SELECT * FROM seqTypes");
      for (int row = 0; row < 2; row++) {
        assertTrue(rs.next());
        assertTrue(expected.next());
        assertEquals(expected.getByte(1), rs.getByte(1));
        assertEquals(expected.wasNull(), rs.wasNull());
        assertEquals(expected.getShort(2), rs.getShort(2));
        assertEquals(expected.getInt(3), rs.getInt(3));
        assertEquals(expected.getInt(4), rs.getInt(4));
        assertEquals(expected.getLong(5), rs.getLong(5));
        assertEquals(expected.getFloat(6), rs.getFloat(6));
        assertEquals(expected.getDouble(7), rs.getDouble(7));
        assertEquals(expected.getBigDecimal(8), rs.getBigDecimal(8));
        assertEquals(expected.getDate(9), rs.getDate(9));
        assertEquals(expected.getTime(10), rs.getTime(10));
        assertEquals(expected.getTimestamp(11), rs.getTimestamp(11));
        assertEquals(expected.getInt(12), rs.getInt(12));
        assertEquals(expected.getString(13), rs.getString(13));
        assertEquals(expected.getString(14), rs.getString(14));
        assertArrayEquals(expected.getBytes(15), rs.getBytes(15));
        Blob b1 = expected.getBlob(16);
        Blob b2 = rs.getBlob(16);
        if (b1 == null) {
          assertNull(b2);
        } else {
          assertArrayEquals(readAll(b1.getBinaryStream()), readAll(b2.getBinaryStream()));
        }
        Reader r1 = expected.getCharacterStream(17);
        Reader r2 = rs.getCharacterStream(17);
        if (r1 == null) {
          assertNull(r2);
        } else {
          assertEquals(readAll(r1), readAll(r2));
        }
        assertEquals(expected.getInt(18), rs.getInt(18));
        assertEquals(expected.getBigDecimal(19), rs.getBigDecimal(19));
        assertEquals(expected.getObject(19), rs.getObject(19));
        assertEquals(expected.getInt(20), rs.getInt(20));
        assertEquals(expected.wasNull(), rs.wasNull());
      }
      assertFalse(rs.next());

      // skipping all types: read only the last column
      rs = stmt.executeQuery("SELECT * FROM seqTypes");
      assertTrue(rs.next());
      assertEquals(42, rs.getInt(20));
      assertTrue(rs.next());
      assertEquals(43, rs.getInt(20));
      assertFalse(rs.next());

      // stream getters on non streamable types fall back to standard decoding
      ResultSet rs3 = stmt.executeQuery("SELECT * FROM seqTypes");
      assertTrue(rs3.next());
      assertThrowsContains(SQLException.class, () -> rs3.getBinaryStream(1), "cannot be decoded");
      assertEquals(-300, rs3.getShort(2));
      assertThrowsContains(SQLException.class, () -> rs3.getClob(4), "cannot be decoded");
      assertEquals(new BigDecimal("123.456"), rs3.getBigDecimal(8));
      // a binary blob cannot be read as characters, but can be read as bytes
      assertThrowsContains(
          SQLException.class, () -> rs3.getCharacterStream(16), "cannot be decoded");
      assertArrayEquals("blob".getBytes(StandardCharsets.UTF_8), readAll(rs3.getBinaryStream(16)));
      assertArrayEquals(
          "text".getBytes(StandardCharsets.UTF_8), readAll(rs3.getBlob(17).getBinaryStream()));
    }
  }

  @Test
  public void bigRow() throws Exception {
    // row bigger than a 16M packet, split in several packets
    Assumptions.assumeTrue(runLongTest());
    long maxAllowedPacket;
    try (ResultSet rs = sharedConn.createStatement().executeQuery("SELECT @@max_allowed_packet")) {
      rs.next();
      maxAllowedPacket = rs.getLong(1);
    }
    Assumptions.assumeTrue(maxAllowedPacket >= 40 * 1024 * 1024);
    for (Connection con :
        new Connection[] {
          createCon("maxAllowedPacket=41943040"),
          createCon("maxAllowedPacket=41943040&useServerPrepStmts=true")
        }) {
      try (Statement stmt = sequentialStmt(con)) {
        stmt.execute("DROP TABLE IF EXISTS seqBig");
        stmt.execute("CREATE TABLE seqBig (id int, b LONGBLOB, t LONGTEXT, i2 int)");
        stmt.execute(
            "INSERT INTO seqBig VALUES (1, REPEAT('a', 17 * 1024 * 1024), REPEAT('é', 9 * 1024 *"
                + " 1024), 1), (2, REPEAT('b', 16777215 - 12), 'x', 2), (3, REPEAT('c', 100), 'y',"
                + " 3)");
        ResultSet rs = stmt.executeQuery("SELECT * FROM seqBig ORDER BY id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        InputStream is = rs.getBinaryStream(2);
        byte[] buf = new byte[65536];
        long total = 0;
        int len;
        while ((len = is.read(buf)) > 0) {
          for (int i = 0; i < len; i++) assertEquals('a', buf[i]);
          total += len;
        }
        assertEquals(17L * 1024 * 1024, total);
        Reader reader = rs.getCharacterStream(3);
        char[] cbuf = new char[65536];
        total = 0;
        while ((len = reader.read(cbuf)) > 0) {
          for (int i = 0; i < len; i++) assertEquals('é', cbuf[i]);
          total += len;
        }
        assertEquals(9L * 1024 * 1024, total);
        assertEquals(1, rs.getInt(4));
        assertTrue(rs.next());
        // skip the big column entirely
        assertEquals(2, rs.getInt(4));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals(100, rs.getBlob(2).length());
        assertEquals(3, rs.getInt(4));
        assertFalse(rs.next());

        // Blob over a field spanning several packets: reads across the 16M packet boundary
        rs = stmt.executeQuery("SELECT * FROM seqBig ORDER BY id");
        assertTrue(rs.next());
        Blob blob = rs.getBlob(2);
        assertEquals(17L * 1024 * 1024, blob.length());
        byte[] straddling = blob.getBytes(0xFFFFFF - 10, 40);
        assertEquals(40, straddling.length);
        for (byte b : straddling) assertEquals('a', b);
        try (InputStream bis = blob.getBinaryStream()) {
          total = 0xFFFFFF - 11 + 40;
          while ((len = bis.read(buf)) > 0) {
            for (int i = 0; i < len; i++) assertEquals('a', buf[i]);
            total += len;
          }
        }
        assertEquals(17L * 1024 * 1024, total);
        Clob clob = rs.getClob(3);
        assertEquals("éé", clob.getSubString(4 * 1024 * 1024, 2));
        assertEquals(1, rs.getInt(4));
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());

        // interleaved command in the middle of a multi-packet row
        rs = stmt.executeQuery("SELECT * FROM seqBig ORDER BY id");
        assertTrue(rs.next());
        is = rs.getBinaryStream(2);
        assertEquals(1000, is.readNBytes(new byte[1000], 0, 1000));
        try (Statement other = (Statement) con.createStatement()) {
          assertTrue(other.executeQuery("SELECT 1").next());
        }
        total = 1000;
        while ((len = is.read(buf)) > 0) total += len;
        assertEquals(17L * 1024 * 1024, total);
        assertEquals(1, rs.getInt(4));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertFalse(rs.next());
        stmt.execute("DROP TABLE seqBig");
      }
      con.close();
    }
  }
}
