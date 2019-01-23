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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionTest extends BaseTest {

  /**
   * Tables initialisation.
   *
   * @throws SQLException exception
   */
  @Before
  public void before() throws SQLException {
    if (testSingleHost) {
      Statement stmt = sharedConnection.createStatement();
      stmt.execute("drop table if exists tx_fore_key");
      stmt.execute("drop table if exists tx_prim_key");
      createTable("tx_prim_key", "id int not null primary key", "engine=innodb");
      createTable("tx_fore_key",
          "id int not null primary key, id_ref int not null, "
              + "foreign key (id_ref) references tx_prim_key(id) on delete restrict on update restrict",
          "engine=innodb");
    }
  }

  /**
   * Clean up created tables.
   *
   * @throws SQLException exception
   */
  @After
  public void after() throws SQLException {
    if (testSingleHost) {
      Statement stmt = sharedConnection.createStatement();
      stmt.execute("drop table if exists tx_fore_key");
      stmt.execute("drop table if exists tx_prim_key");
    }
  }

  @Test
  public void testProperRollback() throws Exception {
    try (Statement st = sharedConnection.createStatement()) {
      st.executeUpdate("insert into tx_prim_key(id) values(32)");
      st.executeUpdate("insert into tx_fore_key(id, id_ref) values(42, 32)");
    }

    // 2. try to delete entry in Primary table in a transaction - which will fail due
    // foreign key.
    sharedConnection.setAutoCommit(false);
    try (Statement st = sharedConnection.createStatement()) {
      st.executeUpdate("delete from tx_prim_key where id = 32");
      sharedConnection.commit();
      fail("Expected SQLException");
    } catch (SQLException e) {
      // This exception is expected
      assertTrue(e.getMessage().contains("a foreign key constraint fails"));
      sharedConnection.rollback();
    }

    try (Connection conn2 = openNewConnection(connUri); Statement st = conn2.createStatement()) {
      st.setQueryTimeout(30000);
      st.executeUpdate("delete from tx_fore_key where id = 42");
      st.executeUpdate("delete from tx_prim_key where id = 32");
    }
  }

}