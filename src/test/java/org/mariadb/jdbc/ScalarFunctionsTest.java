/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.junit.Test;

public class ScalarFunctionsTest extends BaseTest {

  @Test
  public void nativeSqlTest() throws SQLException {
    assertEquals(
        "SELECT convert(foo(a,b,c), INTEGER)"
            + ", convert(convert(?, CHAR), INTEGER)"
            + ", convert(?, INTEGER )"
            + ", convert(?, INTEGER)"
            + ", convert(?,   INTEGER   )"
            + ",  convert (?,   INTEGER   )"
            + ", convert(?, INTEGER)"
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
            + ", convert(?, DATETIME)",
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
  public void scalarFctTest() throws SQLException {
    queryScalar("SELECT {fn convert(?, SQL_BIGINT)}", 2147483648L, 2147483648L);
    queryScalar("SELECT {fn convert(?, SQL_BIGINT)}", BigInteger.valueOf(2147483648L), 2147483648L);
    queryScalar("SELECT {fn convert(?, SQL_BIGINT)}", 20, 20);
    queryScalar("SELECT {fn convert(?, SQL_BOOLEAN)}", true, 1);
    queryScalar("SELECT {fn convert(?, SQL_SMALLINT)}", 5000, 5000);
    queryScalar("SELECT {fn convert(?, SQL_TINYINT)}", 5000, 5000);
    queryScalar("SELECT {fn convert(?, SQL_BIT)}", 255, 255);
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
      } else {
        assertEquals(res, rs.getObject(1));
      }
    }
  }
}
