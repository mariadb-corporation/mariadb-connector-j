/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2018 MariaDB Ab.
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

package org.mariadb.jdbc.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mariadb.jdbc.BaseTest;
import org.mariadb.jdbc.MariaDbConnection;

public class AllowMasterDownTest extends BaseTest {

  private String masterDownUrl;

  /**
   * Initialisation.
   */
  @Before
  public void init() {
    Assume.assumeTrue(testSingleHost);
    if (testSingleHost) {
      masterDownUrl = "jdbc:mariadb:replication//" + hostname + ":9999"
          + "," + hostname + ((port == 0) ? "" : ":" + port)
          + "/" + database + "?user=" + username
          + ((password != null) ? "&password=" + password : "")
          + "&retriesAllDown=10&allowMasterDownConnection";
    }
  }

  @Test
  public void masterDownReadOnlyAvailable() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager.getConnection(masterDownUrl);
      Assert.assertFalse(connection.isReadOnly());
      connection.isValid(0);
      connection.setReadOnly(true);
      PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1");
      preparedStatement.executeQuery();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void masterDownThrowError() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager.getConnection(masterDownUrl);
      Statement statement = connection.createStatement();
      try {
        statement.executeQuery("SELECT 1");
        Assert.fail("Must have thrown a connectionException");
      } catch (SQLException sqle) {
        Assert.assertTrue(sqle instanceof SQLNonTransientConnectionException);
      }
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void masterDownAutoCommit() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager.getConnection(masterDownUrl);
      connection.getAutoCommit();
    } catch (SQLException sqle) {
      Assert.assertTrue(sqle instanceof SQLNonTransientConnectionException);
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void masterDownGetMeta() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager.getConnection(masterDownUrl);
      createTable("checkMetaData",
          "xx tinyint(1) primary key auto_increment, yy year(4), zz bit, uu smallint");
      DatabaseMetaData meta = connection.getMetaData();
      meta.getColumns(null, null, "checkMetaData", null);
      Assert.fail("Must have thrown a connectionException");
    } catch (SQLException sqle) {
      Assert.assertTrue(sqle instanceof SQLNonTransientConnectionException);
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void masterDownGetMetaRead() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager.getConnection(masterDownUrl);
      createTable("checkMetaData",
          "xx tinyint(1) primary key auto_increment, yy year(4), zz bit, uu smallint");
      connection.setReadOnly(true);
      DatabaseMetaData meta = connection.getMetaData();
      ResultSet rs = meta.getColumns(null, null, "checkMetaData", null);
      assertTrue(rs.next());
      assertEquals("BIT", rs.getString(6));
      assertTrue(rs.next());
      assertEquals("YEAR", rs.getString(6));
      assertEquals(null, rs.getString(7)); // column size
      assertEquals(null, rs.getString(9)); // decimal digit
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void masterDownCreateStatement() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager.getConnection(masterDownUrl);
      Statement stmt = connection.createStatement();
      try {
        stmt.executeQuery("SELECT 1");
      } catch (SQLException sqle) {
        Assert.assertTrue(sqle instanceof SQLNonTransientConnectionException);
      }
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void masterDownCreatePrepareStatement() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager.getConnection(masterDownUrl);
      PreparedStatement prepare = connection.prepareStatement("SELECT ?");
      prepare.setString(1, "1");
      try {
        prepare.executeQuery();
      } catch (SQLException sqle) {
        Assert.assertTrue(sqle instanceof SQLNonTransientConnectionException);
      }
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void masterDownReadProperties() throws SQLException {
    MariaDbConnection connection = null;
    try {
      connection = (MariaDbConnection) DriverManager.getConnection(masterDownUrl);
      String db = connection.getCatalog();
      Assert.assertFalse(connection.isReadOnly());
      Assert.assertFalse(connection.isClosed());
      connection.getNetworkTimeout();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void masterDownTransactionIsolation() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager.getConnection(masterDownUrl);
      try {
        connection.getTransactionIsolation();
      } catch (SQLException sqle) {
        Assert.assertTrue(sqle instanceof SQLNonTransientConnectionException);
      }
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void masterDownRollback() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager.getConnection(masterDownUrl);
      try {
        connection.rollback();
      } catch (SQLException sqle) {
        Assert.assertTrue(sqle instanceof SQLNonTransientConnectionException);
      }
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

}
