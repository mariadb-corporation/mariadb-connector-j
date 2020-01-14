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

import org.junit.Assume;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

public class ClientPreparedStatementTest extends BaseTest {

  @Test
  public void closedStatement() throws SQLException {

    PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT ?");
    preparedStatement.setString(1, "1");
    preparedStatement.execute();

    preparedStatement.setString(1, "1");
    preparedStatement.executeQuery();

    preparedStatement.setString(1, "1");
    preparedStatement.executeUpdate();

    preparedStatement.close();

    try {
      preparedStatement.setString(1, "1");
      preparedStatement.execute();
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("is called on closed statement"));
    }

    try {
      preparedStatement.setString(1, "1");
      preparedStatement.executeQuery();
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("is called on closed statement"));
    }

    try {
      preparedStatement.setString(1, "1");
      preparedStatement.executeUpdate();
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("is called on closed statement"));
    }
  }

  @Test
  public void timeoutStatement() throws SQLException {

    PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT ?");
    preparedStatement.setQueryTimeout(10);
    preparedStatement.setString(1, "1");
    preparedStatement.execute();

    preparedStatement.setString(1, "1");
    preparedStatement.executeQuery();

    preparedStatement.setString(1, "1");
    preparedStatement.executeUpdate();
  }

  @Test
  public void paramNumber() throws SQLException {
    PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT ?, ?");
    try {
      preparedStatement.setString(1, "1");
      preparedStatement.execute();
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Parameter at position 2 is not set"));
    }
    try {
      preparedStatement.setString(1, "1");
      preparedStatement.executeQuery();
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Parameter at position 2 is not set"));
    }
    try {
      preparedStatement.setString(1, "1");
      preparedStatement.executeUpdate();
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Parameter at position 2 is not set"));
    }
  }

  @Test
  public void batchParamNumber() throws SQLException {
    PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT ?, ?");
    // batch with no parameter
    assertEquals(0, preparedStatement.executeBatch().length);
    assertEquals(0, preparedStatement.executeLargeBatch().length);

    try {
      preparedStatement.setString(1, "1");
      preparedStatement.addBatch();
      fail();
    } catch (SQLException e) {
      assertTrue(
          e.getMessage().contains("You need to set exactly 2 parameters on the prepared statement")
              || e.getMessage().contains("Parameter at position 2 is not set"));
    }

    try {
      preparedStatement.addBatch("SOME SQL");
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot do addBatch(String) on preparedStatement"));
    }

    preparedStatement.setString(1, "1");
    preparedStatement.setString(2, "2");
    preparedStatement.addBatch();
    preparedStatement.setString(2, "2");
    preparedStatement.addBatch();
    try {
      preparedStatement.clearParameters();
      preparedStatement.setString(2, "2");
      preparedStatement.addBatch();
      fail();
    } catch (SQLException e) {
      assertTrue(
          e.getMessage().contains("You need to set exactly 2 parameters on the prepared statement")
              || e.getMessage().contains("Parameter at position 1 is not set"));
    }
  }

  @Test
  public void setParameterError() throws SQLException {
    Assume.assumeFalse(sharedOptions().useServerPrepStmts);
    PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT ?, ?");
    preparedStatement.setString(1, "a");
    preparedStatement.setString(2, "a");

    try {
      preparedStatement.setString(3, "a");
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Could not set parameter at position 3 (values was 'a')"));
    }

    try {
      preparedStatement.setString(0, "a");
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Could not set parameter at position 0 (values was 'a')"));
    }
  }

  @Test
  public void closedBatchError() throws SQLException {
    PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT ?, ?");
    preparedStatement.setString(1, "1");
    preparedStatement.setString(2, "1");
    preparedStatement.addBatch();
    preparedStatement.executeBatch();

    preparedStatement.close();

    try {
      preparedStatement.setString(1, "2");
      preparedStatement.setString(2, "2");
      preparedStatement.addBatch();
      preparedStatement.executeBatch();
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Cannot do an operation on a closed statement"));
    }
  }

  @Test
  public void executeLargeBatchError() throws SQLException {
    PreparedStatement preparedStatement =
        sharedConnection.prepareStatement("INSERT INTO unknownTable values (?, ?)");
    preparedStatement.setString(1, "1");
    preparedStatement.setString(2, "1");
    preparedStatement.addBatch();

    try {
      preparedStatement.executeLargeBatch();
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("doesn't exist"));
    }
  }

  @Test
  public void executeBatchOneByOne() throws SQLException {
    try (Connection connection =
        setConnection(
            "&rewriteBatchedStatements=false&useBulkStmts=false&useBatchMultiSend=false")) {
      Statement stmt = connection.createStatement();
      stmt.execute("CREATE TEMPORARY TABLE executeBatchOneByOne (c1 varchar(16), c2 varchar(16))");
      PreparedStatement preparedStatement =
          connection.prepareStatement("INSERT INTO executeBatchOneByOne values (?, ?)");
      preparedStatement.setString(1, "1");
      preparedStatement.setString(2, "1");
      preparedStatement.addBatch();
      preparedStatement.setQueryTimeout(10);
      assertEquals(1, preparedStatement.executeBatch().length);
    }
  }

  @Test
  public void metaDataForWrongQuery() throws SQLException {
    PreparedStatement preparedStatement = sharedConnection.prepareStatement("WRONG QUERY");
    try {
      preparedStatement.getMetaData();
      fail();
    } catch (SQLSyntaxErrorException e) {
      assertTrue(e.getMessage().contains("You have an error in your SQL syntax"));
    }
  }

  @Test
  public void getMultipleMetaData() throws SQLException {
    PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT 1 as a");
    ResultSetMetaData meta = preparedStatement.getMetaData();
    assertEquals("a", meta.getColumnName(1));

    preparedStatement.execute();

    meta = preparedStatement.getMetaData();
    assertEquals("a", meta.getColumnName(1));
  }

  @Test
  public void prepareToString() throws SQLException {
    PreparedStatement preparedStatement =
        sharedConnection.prepareStatement("SELECT ? as a, ? as b");
    preparedStatement.setString(1, "a");
    preparedStatement.setNull(2, Types.VARCHAR);
    assertEquals(
        "sql : 'SELECT ? as a, ? as b', parameters : ['a',<null>]", preparedStatement.toString());
  }
}
