// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Common;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import org.junit.jupiter.api.Assertions;

public class CommonCodecTest extends Common {

  void assertReaderEquals(Reader expected, Reader result) throws IOException {
    int res1 = 0;
    int res2 = 0;
    while (res1 != -1 && res2 != -1) {
      res1 = expected.read();
      res2 = result.read();
      Assertions.assertEquals(res1, res2);
    }
  }

  void assertStreamEquals(InputStream expected, InputStream result) throws IOException {
    int res1 = 0;
    int res2 = 0;
    while (res1 != -1 && res2 != -1) {
      res1 = expected.read();
      res2 = result.read();
      Assertions.assertEquals(res1, res2);
    }
  }

  void assertStreamEquals(Blob expected, Blob result) throws Exception {
    assertStreamEquals(expected.getBinaryStream(), result.getBinaryStream());
  }

  void assertStreamEquals(Clob expected, Clob result) throws Exception {
    assertReaderEquals(expected.getCharacterStream(), result.getCharacterStream());
  }

  void testObject(ResultSet rs, Class<?> objClass, Object exp) throws Exception {
    testObject(rs, objClass, exp, 1);
  }

  void testObject(ResultSet rs, Class<?> objClass, Object exp, int idx) throws Exception {
    if (exp instanceof Blob) {
      assertStreamEquals((Blob) exp, (Blob) rs.getObject(idx, objClass));
      assertStreamEquals((Blob) exp, (Blob) rs.getObject("t" + idx + "alias", objClass));
      assertNull(rs.getObject(4, objClass));
      assertNull(rs.getObject("t4alias", objClass));
    } else if (exp instanceof Clob) {
      assertStreamEquals((Clob) exp, (Clob) rs.getObject(idx, objClass));
      assertStreamEquals((Clob) exp, (Clob) rs.getObject("t" + idx + "alias", objClass));
      assertNull(rs.getObject(4, objClass));
      assertNull(rs.getObject("t4alias", objClass));
    } else if (exp instanceof InputStream) {
      assertStreamEquals((InputStream) exp, (InputStream) rs.getObject(idx, objClass));
      //      assertStreamEquals((InputStream) exp, (InputStream) rs.getObject("t1alias",
      // objClass));
      assertNull(rs.getObject(4, objClass));
      assertNull(rs.getObject("t4alias", objClass));
    } else if (exp instanceof Reader) {
      assertReaderEquals((Reader) exp, (Reader) rs.getObject(idx, objClass));
      //      assertReaderEquals((Reader) exp, (Reader) rs.getObject("t1alias", objClass));
      assertNull(rs.getObject(4, objClass));
      assertNull(rs.getObject("t4alias", objClass));
    } else if (exp instanceof Time) {
      assertEquals(((Time) exp).getTime(), ((Time) rs.getObject(idx, objClass)).getTime());
      assertEquals(
          ((Time) exp).getTime(), ((Time) rs.getObject("t" + idx + "alias", objClass)).getTime());
      assertNull(rs.getObject(4, objClass));
      assertNull(rs.getObject("t4alias", objClass));
    } else if (exp instanceof Date) {
      assertEquals(((Date) exp).getTime(), ((Date) rs.getObject(idx, objClass)).getTime());
      assertEquals(
          ((Date) exp).getTime(), ((Date) rs.getObject("t" + idx + "alias", objClass)).getTime());
      assertNull(rs.getObject(4, objClass));
      assertNull(rs.getObject("t4alias", objClass));
    } else {
      assertEquals(exp, rs.getObject(idx, objClass));
      assertEquals(exp, rs.getObject("t" + idx + "alias", objClass));
      assertNull(rs.getObject(4, objClass));
      assertNull(rs.getObject("t4alias", objClass));
    }
  }

  void testErrObject(ResultSet rs, Class<?> objClass) throws SQLException {
    Assertions.assertThrows(SQLException.class, () -> rs.getObject(1, objClass));
    Assertions.assertThrows(SQLException.class, () -> rs.getObject("t1alias", objClass));
    assertNull(rs.getObject(4, objClass));
    assertNull(rs.getObject("t4alias", objClass));
  }

  void testArrObject(ResultSet rs, Class<?> objClass, byte[] exp) throws SQLException {
    assertArrayEquals(exp, (byte[]) rs.getObject(1, objClass));
    assertArrayEquals(exp, (byte[]) rs.getObject("t1alias", objClass));
    assertNull(rs.getObject(4, objClass));
    assertNull(rs.getObject("t4alias", objClass));
  }
}
