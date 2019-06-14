/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mariadb.jdbc.internal.util.DefaultOptions;
import org.mariadb.jdbc.internal.util.constant.HaMode;

public class ParserTest extends BaseTest {

  /**
   * Initialisation.
   *
   * @throws SQLException exception
   */
  @BeforeClass()
  public static void initClass() throws SQLException {
    createTable("table1", "id1 int auto_increment primary key");
    createTable("table2", "id2 int auto_increment primary key");
  }

  @Test
  public void poolVerification() throws Exception {
    ArrayList<HostAddress> hostAddresses = new ArrayList<>();
    hostAddresses.add(new HostAddress(hostname, port));
    UrlParser urlParser = new UrlParser(database, hostAddresses,
        DefaultOptions.defaultValues(HaMode.NONE), HaMode.NONE);
    urlParser.setUsername("USER");
    urlParser.setPassword("PWD");
    urlParser.parseUrl("jdbc:mariadb://localhost:3306/db");
    assertEquals("USER", urlParser.getUsername());
    assertEquals("PWD", urlParser.getPassword());

    MariaDbDataSource datasource = new MariaDbDataSource();
    datasource.setUser("USER");
    datasource.setPassword("PWD");
    datasource.setUrl("jdbc:mariadb://localhost:3306/db");
  }

  @Test
  public void isMultiMaster() throws Exception {
    Properties emptyProps = new Properties();

    assertFalse(UrlParser.parse("jdbc:mariadb:replication://host1/", emptyProps).isMultiMaster());
    assertFalse(UrlParser.parse("jdbc:mariadb:failover://host1/", emptyProps).isMultiMaster());
    assertFalse(UrlParser.parse("jdbc:mariadb:aurora://host1/", emptyProps).isMultiMaster());
    assertFalse(UrlParser.parse("jdbc:mariadb:sequential://host1/", emptyProps).isMultiMaster());
    assertFalse(UrlParser.parse("jdbc:mariadb:loadbalance://host1/", emptyProps).isMultiMaster());

    assertFalse(
        UrlParser.parse("jdbc:mariadb:replication://host1,host2/", emptyProps).isMultiMaster());
    assertTrue(UrlParser.parse("jdbc:mariadb:failover://host1,host2/", emptyProps).isMultiMaster());
    assertFalse(UrlParser.parse("jdbc:mariadb:aurora://host1,host2/", emptyProps).isMultiMaster());
    assertTrue(
        UrlParser.parse("jdbc:mariadb:sequential://host1,host2/", emptyProps).isMultiMaster());
    assertTrue(
        UrlParser.parse("jdbc:mariadb:loadbalance://host1,host2/", emptyProps).isMultiMaster());

  }

  @Test
  public void mysqlDatasourceVerification() throws Exception {
    MariaDbDataSource datasource = new MariaDbDataSource();
    datasource.setUser(username);
    datasource.setPassword(password);
    datasource.setUrl("jdbc:mysql://" + hostname + ":" + port + "/" + database);
    try (Connection connection = datasource.getConnection()) {
      Statement stmt = connection.createStatement();
      assertTrue(stmt.execute("SELECT 10"));
    }
  }

  @Test
  public void libreOfficeBase() {
    String sql;
    try {
      Statement statement = sharedConnection.createStatement();
      sql = "INSERT INTO table1 VALUES (1),(2),(3),(4),(5),(6)";
      statement.execute(sql);
      sql = "INSERT INTO table2 VALUES (1),(2),(3),(4),(5),(6)";
      statement.execute(sql);
      // uppercase OJ
      sql = "SELECT table1.id1, table2.id2 FROM { OJ table1 LEFT OUTER JOIN table2 ON table1.id1 = table2.id2 }";
      ResultSet rs = statement.executeQuery(sql);
      for (int count = 1; count <= 6; count++) {
        assertTrue(rs.next());
        assertEquals(count, rs.getInt(1));
        assertEquals(count, rs.getInt(2));
      }
      // mixed oJ
      sql = "SELECT table1.id1, table2.id2 FROM { oJ table1 LEFT OUTER JOIN table2 ON table1.id1 = table2.id2 }";
      rs = statement.executeQuery(sql);
      for (int count = 1; count <= 6; count++) {
        assertTrue(rs.next());
        assertEquals(count, rs.getInt(1));
        assertEquals(count, rs.getInt(2));
      }
    } catch (SQLException e) {
      fail();
    }
  }

  @Test
  public void auroraClusterVerification() {
    try {
      DriverManager.getConnection("jdbc:mariadb:aurora://"
          + "1.somehex.us-east-1.rds.amazonaws.com,"
          + "2.someOtherHex.us-east-1.rds.amazonaws.com/testj");
      fail("must have fail since not same cluster");
    } catch (Exception e) {
      assertEquals("Connection string must contain only one aurora cluster. "
              + "'2.someOtherHex.us-east-1.rds.amazonaws.com' doesn't correspond to DNS prefix "
              + "'somehex.us-east-1.rds.amazonaws.com'",
          e.getMessage());
    }
  }
}
