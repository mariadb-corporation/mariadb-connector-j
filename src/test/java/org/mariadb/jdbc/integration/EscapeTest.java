// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbClob;
import org.mariadb.jdbc.Statement;

/**
 * Values containing the characters the text protocol must escape (quote, backslash, NUL) and the
 * ones it must not touch (double quote, newline, %, _, ^Z, multi-byte), round-tripped through every
 * string and binary encoding path, under each relevant sql_mode.
 */
public class EscapeTest extends Common {

  private static final String[] VALUES = {
    "",
    "plain",
    "it's",
    "back\\slash",
    "dbl\"quote",
    "nul\0char",
    "new\nline\r\ntab\t^Z\u001a",
    "wild%card_and\\%\\_",
    "\\'\\\"\\\\\\0",
    "'\"\\",
    "'''",
    "\"\"\"",
    "\\\\\\",
    "café 'crème' \"brûlée\" \\ 日本語 😀",
    // longer than the 32 chars threshold: JDK encoder path when nothing to escape
    "select col_name from table where id = 12345 and name like 'abc' or x = \"y\" or z = 'a\\b'",
    "no escaping needed in this long ascii string at all, just plain text over 32 chars",
    "日本語テキスト 😀 long multi-byte string without any quote at all here",
    "日本語 'quoted' \"double\" back\\slash 😀 long multi-byte string with escapes"
  };

  @AfterAll
  public static void drop() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE IF EXISTS escapeTest");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    sharedConn
        .createStatement()
        .execute(
            "CREATE TABLE escapeTest (id int not null primary key, t TEXT, b BLOB, c TEXT)"
                + " CHARACTER SET utf8mb4");
  }

  @Test
  public void defaultMode() throws Exception {
    roundTrip(sharedConn);
    roundTrip(sharedConnBinary);
  }

  @Test
  public void noBackslashEscapes() throws Exception {
    try (Connection con = createCon("sessionVariables=sql_mode='NO_BACKSLASH_ESCAPES'")) {
      roundTrip(con);
    }
    try (Connection con =
        createCon("sessionVariables=sql_mode='NO_BACKSLASH_ESCAPES'&useServerPrepStmts=true")) {
      roundTrip(con);
    }
  }

  @Test
  public void ansiQuotes() throws Exception {
    // double quotes delimit identifiers: a raw " inside a single-quoted literal must still be fine
    try (Connection con = createCon("sessionVariables=sql_mode='ANSI_QUOTES'")) {
      roundTrip(con);
    }
    try (Connection con =
        createCon("sessionVariables=sql_mode='ANSI_QUOTES,NO_BACKSLASH_ESCAPES'")) {
      roundTrip(con);
    }
  }

  private void roundTrip(Connection con) throws Exception {
    Statement stmt = (Statement) con.createStatement();
    stmt.execute("TRUNCATE TABLE escapeTest");
    // setString, setBytes, setCharacterStream
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO escapeTest(id, t, b, c) VALUES (?, ?, ?, ?)")) {
      for (int i = 0; i < VALUES.length; i++) {
        prep.setInt(1, i);
        prep.setString(2, VALUES[i]);
        prep.setBytes(3, VALUES[i].getBytes(StandardCharsets.UTF_8));
        prep.setCharacterStream(4, new StringReader(VALUES[i]));
        prep.addBatch();
      }
      prep.executeBatch();
    }
    check(con, "setString/setBytes/setCharacterStream");

    stmt.execute("TRUNCATE TABLE escapeTest");
    // setClob, setBinaryStream, setObject
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO escapeTest(id, t, b, c) VALUES (?, ?, ?, ?)")) {
      for (int i = 0; i < VALUES.length; i++) {
        prep.setInt(1, i);
        prep.setClob(2, new MariaDbClob(VALUES[i].getBytes(StandardCharsets.UTF_8)));
        prep.setBinaryStream(
            3, new ByteArrayInputStream(VALUES[i].getBytes(StandardCharsets.UTF_8)));
        prep.setObject(4, VALUES[i]);
        prep.execute();
      }
    }
    check(con, "setClob/setBinaryStream/setObject");

    // values in a WHERE clause must compare equal to the stored ones
    try (PreparedStatement prep =
        con.prepareStatement("SELECT id FROM escapeTest WHERE t = ? AND b = ? AND c = ?")) {
      for (int i = 0; i < VALUES.length; i++) {
        prep.setString(1, VALUES[i]);
        prep.setBytes(2, VALUES[i].getBytes(StandardCharsets.UTF_8));
        prep.setString(3, VALUES[i]);
        try (ResultSet rs = prep.executeQuery()) {
          assertTrue(rs.next(), "value " + i + " not found by equality: " + VALUES[i]);
          assertEquals(i, rs.getInt(1));
        }
      }
    }
  }

  private void check(Connection con, String label) throws SQLException {
    try (ResultSet rs =
        con.createStatement().executeQuery("SELECT id, t, b, c FROM escapeTest ORDER BY id")) {
      for (int i = 0; i < VALUES.length; i++) {
        assertTrue(rs.next(), label + " row " + i);
        assertEquals(VALUES[i], rs.getString(2), label + " t " + i);
        assertArrayEquals(
            VALUES[i].getBytes(StandardCharsets.UTF_8), rs.getBytes(3), label + " b " + i);
        assertEquals(VALUES[i], rs.getString(4), label + " c " + i);
      }
      assertFalse(rs.next());
    }
  }
}
