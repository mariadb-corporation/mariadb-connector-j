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

import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class CatalogTest extends BaseTest {

  @Test
  public void catalogTest() throws SQLException {
    Statement stmt = sharedConnection.createStatement();
    stmt.executeUpdate("drop database if exists cattest1");
    stmt.executeUpdate("create database cattest1");
    sharedConnection.setCatalog("cattest1");
    assertEquals("cattest1", sharedConnection.getCatalog());
    stmt.executeUpdate("drop database if exists cattest1");
    sharedConnection.setCatalog(database);
  }

  @Test(expected = SQLException.class)
  public void catalogTest2() throws SQLException {
    sharedConnection.setCatalog(null);
  }

  @Test(expected = SQLException.class)
  public void catalogTest3() throws SQLException {
    sharedConnection.setCatalog("Non-existent catalog");
  }

  @Test(expected = SQLException.class)
  public void catalogTest4() throws SQLException {
    sharedConnection.setCatalog("");
  }

  @Test
  public void catalogTest5() throws SQLException {
    requireMinimumVersion(5, 1);

    String[] weirdDbNames = new String[] {"abc 123", "\"", "`"};
    for (String name : weirdDbNames) {
      try (Statement stmt = sharedConnection.createStatement()) {
        stmt.execute("drop database if exists " + MariaDbConnection.quoteIdentifier(name));
        stmt.execute("create database " + MariaDbConnection.quoteIdentifier(name));
        sharedConnection.setCatalog(name);
        assertEquals(name, sharedConnection.getCatalog());
        stmt.execute("drop database if exists " + MariaDbConnection.quoteIdentifier(name));
      }
      sharedConnection.setCatalog(database);
    }
  }
}
