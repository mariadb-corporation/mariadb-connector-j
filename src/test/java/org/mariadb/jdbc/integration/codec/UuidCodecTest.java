// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.*;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.integration.Common;
import org.mariadb.jdbc.util.constants.Capabilities;

public class UuidCodecTest extends CommonCodecTest {

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS UuidCodec");
    stmt.execute("DROP TABLE IF EXISTS UuidCodec2");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Assumptions.assumeTrue(
        isMariaDBServer()
            && minVersion(10, 7, 0)
            && hasCapability(Capabilities.EXTENDED_TYPE_INFO));
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE UuidCodec (t1 UUID, t2 UUID, t3 UUID, t4 UUID)");
    stmt.execute("CREATE TABLE UuidCodec2 (t1 UUID)");
    stmt.execute(
        "INSERT INTO UuidCodec VALUES ('123e4567-e89b-12d3-a456-426655440000',"
            + " '93aac041-1a14-11ec-ab4e-f859713e4be4', 'ffffffff-ffff-ffff-ffff-fffffffffffe',"
            + " null)");
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    stmt.closeOnCompletion();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from UuidCodec");
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private ResultSet getPrepare(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement preparedStatement =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from UuidCodec"
                + " WHERE 1 > ?");
    preparedStatement.closeOnCompletion();
    preparedStatement.setInt(1, 0);
    ResultSet rs = preparedStatement.executeQuery();
    assertTrue(rs.next());
    con.commit();
    return rs;
  }

  @Test
  public void getObject() throws Exception {
    getObject(get());
  }

  @Test
  public void getObjectPrepare() throws Exception {
    getObject(getPrepare(sharedConn));
    getObject(getPrepare(sharedConnBinary));
    try (Connection con = createCon("&uuidAsString=1")) {
      getObjectString(getPrepare(con));
    }
  }

  public void getObject(ResultSet rs) throws Exception {
    assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426655440000"), rs.getObject(1));
    assertFalse(rs.wasNull());

    assertEquals(UUID.fromString("93aac041-1a14-11ec-ab4e-f859713e4be4"), rs.getObject(2));
    assertEquals(UUID.fromString("93aac041-1a14-11ec-ab4e-f859713e4be4"), rs.getObject("t2alias"));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4));
    assertEquals(
        UUID.fromString("ffffffff-ffff-ffff-ffff-fffffffffffe"), rs.getObject(3, UUID.class));
    assertEquals(UUID.class, rs.getObject(3, UUID.class).getClass());
  }

  public void getObjectString(ResultSet rs) throws Exception {
    assertEquals("123e4567-e89b-12d3-a456-426655440000", rs.getObject(1));
    assertFalse(rs.wasNull());

    assertEquals("93aac041-1a14-11ec-ab4e-f859713e4be4", rs.getObject(2));
    assertEquals("93aac041-1a14-11ec-ab4e-f859713e4be4", rs.getObject("t2alias"));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4));
    assertEquals("ffffffff-ffff-ffff-ffff-fffffffffffe", rs.getObject(3));
    assertEquals(UUID.class, rs.getObject(3, UUID.class).getClass());
  }

  @Test
  public void getObjectType() throws Exception {
    getObjectType(get());
  }

  @Test
  public void getObjectTypePrepare() throws Exception {
    getObjectType(getPrepare(sharedConn));
    getObjectType(getPrepare(sharedConnBinary));
  }

  public void getObjectType(ResultSet rs) throws Exception {
    testErrObject(rs, Integer.class);
    testObject(rs, String.class, "123e4567-e89b-12d3-a456-426655440000");
    testErrObject(rs, Long.class);
    testErrObject(rs, Short.class);
    testErrObject(rs, BigDecimal.class);
    testErrObject(rs, BigInteger.class);
    testErrObject(rs, Double.class);
    testErrObject(rs, Float.class);
    testErrObject(rs, Byte.class);
    testObject(rs, Reader.class, new StringReader("123e4567-e89b-12d3-a456-426655440000"));
    testErrObject(rs, LocalDate.class);
    testErrObject(rs, LocalDateTime.class);
    testErrObject(rs, LocalTime.class);
    testErrObject(rs, Time.class);
    testErrObject(rs, Date.class);
    testErrObject(rs, ZonedDateTime.class);
    testErrObject(rs, OffsetDateTime.class);
    testErrObject(rs, OffsetTime.class);
    testErrObject(rs, java.util.Date.class);
  }

  @Test
  public void getString() throws SQLException {
    getString(get());
  }

  @Test
  public void getStringPrepare() throws SQLException {
    getString(getPrepare(sharedConn));
    getString(getPrepare(sharedConnBinary));
    try (Connection con = createCon("&uuidAsString=1")) {
      getString(getPrepare(con));
    }
  }

  public void getString(ResultSet rs) throws SQLException {
    assertEquals("123e4567-e89b-12d3-a456-426655440000", rs.getObject(1, String.class));
    assertFalse(rs.wasNull());

    assertEquals("93aac041-1a14-11ec-ab4e-f859713e4be4", rs.getString(2));
    assertEquals("93aac041-1a14-11ec-ab4e-f859713e4be4", rs.getString("t2alias"));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4));
    assertEquals("ffffffff-ffff-ffff-ffff-fffffffffffe", rs.getString(3));
    assertEquals(String.class, rs.getString(3).getClass());
  }

  @Test
  public void getUuid() throws SQLException {
    getUuid(get());
  }

  @Test
  public void getUuidPrepare() throws SQLException {
    getUuid(getPrepare(sharedConn));
    getUuid(getPrepare(sharedConnBinary));
  }

  public void getUuid(ResultSet rs) throws SQLException {
    assertEquals(
        UUID.fromString("123e4567-e89b-12d3-a456-426655440000"), rs.getObject(1, UUID.class));
    assertFalse(rs.wasNull());

    assertEquals(
        UUID.fromString("93aac041-1a14-11ec-ab4e-f859713e4be4"), rs.getObject(2, UUID.class));
    assertEquals(
        UUID.fromString("93aac041-1a14-11ec-ab4e-f859713e4be4"),
        rs.getObject("t2alias", UUID.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4, UUID.class));
    assertEquals(
        UUID.fromString("ffffffff-ffff-ffff-ffff-fffffffffffe"), rs.getObject(3, UUID.class));
    assertEquals(UUID.class, rs.getObject(3, UUID.class).getClass());
  }

  @Test
  public void getInt() throws SQLException {
    getInt(get());
  }

  @Test
  public void getIntPrepare() throws SQLException {
    getInt(getPrepare(sharedConn));
    getInt(getPrepare(sharedConnBinary));
  }

  public void getInt(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getInt(1), "Data type UUID cannot be decoded as Integer");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getLong() throws SQLException {
    getLong(get());
  }

  @Test
  public void getLongPrepare() throws SQLException {
    getLong(getPrepare(sharedConn));
    getLong(getPrepare(sharedConnBinary));
  }

  public void getLong(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getLong(1), "Data type UUID cannot be decoded as Long");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getFloat() throws SQLException {
    getFloat(get());
  }

  @Test
  public void getFloatPrepare() throws SQLException {
    getFloat(getPrepare(sharedConn));
    getFloat(getPrepare(sharedConnBinary));
  }

  public void getFloat(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getFloat(1), "Data type UUID cannot be decoded as Float");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getDouble() throws SQLException {
    getDouble(get());
  }

  @Test
  public void getDoublePrepare() throws SQLException {
    getDouble(getPrepare(sharedConn));
    getDouble(getPrepare(sharedConnBinary));
  }

  public void getDouble(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getDouble(1),
        "Data type UUID cannot be decoded as Double");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getBigDecimal() throws SQLException {
    getBigDecimal(get());
  }

  @Test
  public void getBigDecimalPrepare() throws SQLException {
    getBigDecimal(getPrepare(sharedConn));
    getBigDecimal(getPrepare(sharedConnBinary));
  }

  public void getBigDecimal(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class,
        () -> rs.getBigDecimal(1),
        "value '123e4567-e89b-12d3-a456-426655440000' cannot be decoded as BigDecimal");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getDate() throws SQLException {
    getDate(get());
  }

  @Test
  public void getDatePrepare() throws SQLException {
    getDate(getPrepare(sharedConn));
    getDate(getPrepare(sharedConnBinary));
  }

  public void getDate(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getDate(1), "Data type UUID cannot be decoded as Date");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getTime() throws SQLException {
    getTime(get());
  }

  @Test
  public void getTimePrepare() throws SQLException {
    getTime(getPrepare(sharedConn));
    getTime(getPrepare(sharedConnBinary));
  }

  public void getTime(ResultSet rs) throws SQLException {
    Common.assertThrowsContains(
        SQLDataException.class, () -> rs.getTime(1), "Data type UUID cannot be decoded as Time");
    assertFalse(rs.wasNull());
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = get();
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("uuid", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.util.UUID", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.OTHER, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(36, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(36, meta.getColumnDisplaySize(1));

    try (Connection con = createCon("&uuidAsString=True")) {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
      stmt.closeOnCompletion();
      rs =
          stmt.executeQuery(
              "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from UuidCodec");
      assertTrue(rs.next());
      con.commit();
    }

    meta = rs.getMetaData();
    assertEquals("uuid", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals("java.lang.String", meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.CHAR, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(36, meta.getPrecision(1));
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
    assertEquals(36, meta.getColumnDisplaySize(1));
  }

  @Test
  public void sendParam() throws Exception {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws Exception {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE UuidCodec2");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep = con.prepareStatement("INSERT INTO UuidCodec2(t1) VALUES (?)")) {
      prep.setObject(1, UUID.fromString("123e4567-e89b-12d3-a456-426655440000"));
      prep.execute();
      prep.setString(1, "123e4568-e89b-12d3-a456-426655440000");
      prep.execute();
      prep.setObject(1, null);
      prep.execute();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM UuidCodec2");

    assertTrue(rs.next());
    assertEquals("123e4567-e89b-12d3-a456-426655440000", rs.getString(1));

    assertTrue(rs.next());
    assertEquals("123e4568-e89b-12d3-a456-426655440000", rs.getString(1));

    assertTrue(rs.next());
    assertNull(rs.getString(1));

    con.commit();
  }
}
