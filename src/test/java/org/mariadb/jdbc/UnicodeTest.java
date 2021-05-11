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

import static org.junit.Assert.assertEquals;

import java.sql.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class UnicodeTest extends BaseTest {

  @BeforeClass()
  public static void initClass() throws SQLException {
    drop();
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute(
          "CREATE TABLE unicode_test(id int not null primary key auto_increment, test_text varchar(100)) charset utf8");
      stmt.execute(
          "CREATE TABLE umlaut_test(id varchar(100), test_text varchar(100), t int) charset utf8");
      stmt.execute(
          "CREATE TABLE unicode_test2(id int not null primary key auto_increment, test_text varchar(100)) charset utf8");
      stmt.execute(
          "CREATE TABLE unicode_test3(id int not null primary key auto_increment, test_text varchar(100)) charset utf8mb4");
      stmt.execute("FLUSH TABLES");
    }
  }

  @AfterClass
  public static void drop() throws SQLException {
    try (Statement stmt = sharedConnection.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS unicode_test");
      stmt.execute("DROP TABLE IF EXISTS umlaut_test");
      stmt.execute("DROP TABLE IF EXISTS unicode_test2");
      stmt.execute("DROP TABLE IF EXISTS unicode_test3");
    }
  }

  @Test
  public void firstTest() throws SQLException {
    String jaString = "\u65e5\u672c\u8a9e\u6587\u5b57\u5217"; // hmm wonder what this means...
    Statement stmt = sharedConnection.createStatement();
    PreparedStatement ps =
        sharedConnection.prepareStatement("insert into unicode_test (test_text) values (?)");
    ps.setString(1, jaString);
    ps.executeUpdate();
    ResultSet rs = stmt.executeQuery("select test_text from unicode_test");
    assertEquals(true, rs.next());
    assertEquals(jaString, rs.getString(1));
  }

  @Test
  public void testGermanUmlauts() throws SQLException {
    String query =
        "insert into umlaut_test values('tax-1273608028038--5546415852995205209-13', "
            + "'MwSt. 7% Bücher & Lebensmittel', 7)";
    Statement stmt = sharedConnection.createStatement();
    stmt.executeUpdate(query);

    ResultSet rs = stmt.executeQuery("select * from umlaut_test");
    assertEquals(true, rs.next());
    assertEquals("MwSt. 7% Bücher & Lebensmittel", rs.getString(2));
    assertEquals(false, rs.next());
  }

  @Test
  public void mysqlTest() throws SQLException {
    String jaString = "\u65e5\u672c\u8a9e\u6587\u5b57\u5217"; // hmm wonder what this means...
    Statement stmt = sharedConnection.createStatement();
    PreparedStatement ps =
        sharedConnection.prepareStatement("insert into unicode_test2 (test_text) values (?)");
    ps.setString(1, jaString);
    ps.executeUpdate();
    ResultSet rs = stmt.executeQuery("select test_text from unicode_test2");
    assertEquals(true, rs.next());
    assertEquals(jaString, rs.getString(1));
  }

  @Test
  public void unicodeTests() throws SQLException {
    String unicodeString = "";
    unicodeString += "\uD83D\uDE0E"; // 😎 unicode 6 smiling face with sunglasses
    unicodeString += "\uD83C\uDF36"; // 🌶 unicode 7 hot pepper
    unicodeString += "\uD83C\uDFA4"; // 🎤 unicode 8 no microphones
    unicodeString += "\uD83E\uDD42"; // 🥂 unicode 9 clinking glasses

    // test binary protocol
    try (Connection connection = setConnection("")) {
      connection.createStatement().execute("SET NAMES utf8mb4");
      checkSendAndRetrieve(connection, unicodeString);
    }

    // test prepare text protocol
    try (Connection connection = setConnection("&rewriteBatchedStatements=true")) {
      connection.createStatement().execute("SET NAMES utf8mb4");
      checkSendAndRetrieve(connection, unicodeString);
    }
  }

  private void checkSendAndRetrieve(Connection connection, String unicodeString)
      throws SQLException {
    Statement stmt = connection.createStatement();
    PreparedStatement ps =
        connection.prepareStatement("insert into unicode_test3 (test_text) values (?)");
    ps.setString(1, unicodeString);
    ps.executeUpdate();
    ResultSet rs = stmt.executeQuery("select test_text from unicode_test3");
    assertEquals(true, rs.next());
    String returnString = rs.getString(1);
    assertEquals(unicodeString, returnString);
  }
}
