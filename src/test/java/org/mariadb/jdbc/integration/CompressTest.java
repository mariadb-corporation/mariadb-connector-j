/*
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
 */

package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Statement;

public class CompressTest extends Common {
  private static Connection shareCompressCon;

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS compressTest");
    stmt.execute(
        "CREATE TABLE compressTest (t1 int not null primary key auto_increment, t2 LONGTEXT)");
    shareCompressCon = createCon("useCompression=true");
  }

  @AfterAll
  public static void drop() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE IF EXISTS compressTest");
    shareCompressCon.close();
  }

  @Test
  public void bigSend() throws SQLException {
    char[] arr = new char[Math.min(16 * 1024 * 1024, getMaxAllowedPacket() / 2)];
    for (int pos = 0; pos < arr.length; pos++) {
      arr[pos] = (char) ('A' + (pos % 60));
    }
    Statement stmt = shareCompressCon.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    String st = new String(arr);
    try (PreparedStatement prep =
        shareCompressCon.prepareStatement("INSERT INTO compressTest VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setString(2, st);
      prep.execute();
    }

    ResultSet rs = stmt.executeQuery("SELECT t2 from compressTest WHERE t1 = 1");
    assertTrue(rs.next());
    assertEquals(st, rs.getString(1));
    stmt.execute("COMMIT");
  }

  private int getMaxAllowedPacket() throws SQLException {
    java.sql.Statement st = sharedConn.createStatement();
    ResultSet rs = st.executeQuery("select @@max_allowed_packet");
    assertTrue(rs.next());
    return rs.getInt(1);
  }
}
