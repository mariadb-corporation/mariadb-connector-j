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
 *
 */

package org.mariadb.jdbc;

import org.junit.Test;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import static org.junit.Assert.*;

public class ScalarFunctionsTest extends BaseTest {

  @Test
  public void nativeSqlTest() throws SQLException {
    String exp;
    if (isMariadbServer() || minVersion(8, 0, 17)) {
      exp =
          "SELECT convert(foo(a,b,c), SIGNED INTEGER)"
              + ", convert(convert(?, CHAR), SIGNED INTEGER)"
              + ", 1=?"
              + ", 1=?"
              + ", convert(?,   SIGNED INTEGER   )"
              + ",  convert (?,   SIGNED INTEGER   )"
              + ", convert(?, UNSIGNED INTEGER)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, DOUBLE)"
              + ", convert(?, DOUBLE)"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DATETIME)"
              + ", convert(?, DATETIME)";
    } else {
      exp =
          "SELECT convert(foo(a,b,c), SIGNED INTEGER)"
              + ", convert(convert(?, CHAR), SIGNED INTEGER)"
              + ", 1=?"
              + ", 1=?"
              + ", convert(?,   SIGNED INTEGER   )"
              + ",  convert (?,   SIGNED INTEGER   )"
              + ", convert(?, UNSIGNED INTEGER)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, BINARY)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", convert(?, CHAR)"
              + ", 0.0+?"
              + ", 0.0+?"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DECIMAL)"
              + ", convert(?, DATETIME)"
              + ", convert(?, DATETIME)";
    }

    assertEquals(
        exp,
        sharedConnection.nativeSQL(
            "SELECT {fn convert(foo(a,b,c), SQL_BIGINT)}"
                + ", {fn convert({fn convert(?, SQL_VARCHAR)}, SQL_BIGINT)}"
                + ", {fn convert(?, SQL_BOOLEAN )}"
                + ", {fn convert(?, BOOLEAN)}"
                + ", {fn convert(?,   SMALLINT   )}"
                + ", {fn  convert (?,   TINYINT   )}"
                + ", {fn convert(?, SQL_BIT)}"
                + ", {fn convert(?, SQL_BLOB)}"
                + ", {fn convert(?, SQL_VARBINARY)}"
                + ", {fn convert(?, SQL_LONGVARBINARY)}"
                + ", {fn convert(?, SQL_ROWID)}"
                + ", {fn convert(?, SQL_BINARY)}"
                + ", {fn convert(?, SQL_NCHAR)}"
                + ", {fn convert(?, SQL_CLOB)}"
                + ", {fn convert(?, SQL_NCLOB)}"
                + ", {fn convert(?, SQL_DATALINK)}"
                + ", {fn convert(?, SQL_VARCHAR)}"
                + ", {fn convert(?, SQL_NVARCHAR)}"
                + ", {fn convert(?, SQL_LONGVARCHAR)}"
                + ", {fn convert(?, SQL_LONGNVARCHAR)}"
                + ", {fn convert(?, SQL_SQLXML)}"
                + ", {fn convert(?, SQL_LONGNCHAR)}"
                + ", {fn convert(?, SQL_CHAR)}"
                + ", {fn convert(?, SQL_FLOAT)}"
                + ", {fn convert(?, SQL_DOUBLE)}"
                + ", {fn convert(?, SQL_DECIMAL)}"
                + ", {fn convert(?, SQL_REAL)}"
                + ", {fn convert(?, SQL_NUMERIC)}"
                + ", {fn convert(?, SQL_TIMESTAMP)}"
                + ", {fn convert(?, SQL_DATETIME)}"));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void scalarFctTest() throws SQLException {
    if (!isMariadbServer()) {
      cancelForVersion(5, 5);
    }
    queryScalar("SELECT {fn convert(?, SQL_BIGINT)}", 2147483648L, 2147483648L);
    queryScalar("SELECT {fn convert(?, SQL_BIGINT)}", BigInteger.valueOf(2147483648L), 2147483648L);
    queryScalar("SELECT {fn convert(?, SQL_BIGINT)}", 20, new Object[] {20, 20L});
    queryScalar("SELECT {fn convert(?, SQL_BOOLEAN)}", true, new Object[] {1, 1L});
    queryScalar("SELECT {fn convert(?, SQL_SMALLINT)}", 5000, new Object[] {5000, 5000L});
    queryScalar("SELECT {fn convert(?, SQL_TINYINT)}", 5000, new Object[] {5000, 5000L});
    queryScalar(
        "SELECT {fn convert(?, SQL_BIT)}", 255, new Object[] {255L, BigInteger.valueOf(255L)});
    queryScalar("SELECT {fn convert(?, SQL_BINARY)}", "test", "test".getBytes());
    queryScalar(
        "SELECT {fn convert(?, SQL_DATETIME)}",
        "2020-12-31 12:13.15.12",
        new Timestamp(2020 - 1900, 11, 31, 12, 13, 15, 0));
  }

  private void queryScalar(String sql, Object val, Object res) throws SQLException {
    try (PreparedStatement prep = sharedConnection.prepareStatement(sql)) {
      prep.setObject(1, val);
      ResultSet rs = prep.executeQuery();
      assertTrue(rs.next());
      Object obj = rs.getObject(1);
      if (obj instanceof byte[]) {
        byte[] arr = (byte[]) obj;
        assertArrayEquals((byte[]) res, arr);
      } else if (res instanceof Object[]) {
        Object[] resArr = (Object[]) res;
        for (int i = 0; i < resArr.length; i++) {
          if (resArr[i].equals(obj)) {
            return;
          }
        }
        fail("not expected result");
      } else {
        assertEquals(res, rs.getObject(1));
      }
    }
  }
}
