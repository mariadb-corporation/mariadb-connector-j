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

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;

public class ResultSetMetaDataTest extends BaseTest {
    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("test_rsmd", "id_col int not null primary key auto_increment, "
                + "nullable_col varchar(20),unikey_col int unique, char_col char(10), us  smallint unsigned");
        createTable("t1", "id int, name varchar(20)");
        createTable("t2", "id int, name varchar(20)");
        createTable("t3", "id int, name varchar(20)");
    }

    @Test
    public void metaDataTest() throws SQLException {
        requireMinimumVersion(5, 0);
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("insert into test_rsmd (id_col,nullable_col,unikey_col) values (null, 'hej', 9)");
        ResultSet rs = stmt
                .executeQuery("select id_col, nullable_col, unikey_col as something, char_col,us from test_rsmd");
        assertEquals(true, rs.next());
        ResultSetMetaData rsmd = rs.getMetaData();

        assertEquals(true, rsmd.isAutoIncrement(1));
        assertEquals(5, rsmd.getColumnCount());
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(2));
        assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(1));
        assertEquals(String.class.getName(), rsmd.getColumnClassName(2));
        assertEquals(Integer.class.getName(), rsmd.getColumnClassName(1));
        assertEquals(Integer.class.getName(), rsmd.getColumnClassName(3));
        assertEquals("id_col", rsmd.getColumnLabel(1));
        assertEquals("nullable_col", rsmd.getColumnLabel(2));
        assertEquals("something", rsmd.getColumnLabel(3));
        assertEquals("unikey_col", rsmd.getColumnName(3));
        assertEquals(Types.CHAR, rsmd.getColumnType(4));
        assertEquals(Types.SMALLINT, rsmd.getColumnType(5));

        DatabaseMetaData md = sharedConnection.getMetaData();
        ResultSet cols = md.getColumns(null, null, "test\\_rsmd", null);
        cols.next();
        assertEquals("id_col", cols.getString("COLUMN_NAME"));
        assertEquals(Types.INTEGER, cols.getInt("DATA_TYPE"));
        cols.next(); /* nullable_col */
        cols.next(); /* unikey_col */
        cols.next(); /* char_col */
        assertEquals("char_col", cols.getString("COLUMN_NAME"));
        assertEquals(Types.CHAR, cols.getInt("DATA_TYPE"));
        cols.next(); /* us */// CONJ-96: SMALLINT UNSIGNED gives Types.SMALLINT
        assertEquals("us", cols.getString("COLUMN_NAME"));
        assertEquals(Types.SMALLINT, cols.getInt("DATA_TYPE"));
    }

    @Test
    public void conj17() throws Exception {
        requireMinimumVersion(5, 0);
        ResultSet rs = sharedConnection.createStatement().executeQuery(
                "select count(*),1 from information_schema.tables");
        rs.next();
        assertEquals(rs.getMetaData().getColumnName(1), "count(*)");
        assertEquals(rs.getMetaData().getColumnName(2), "1");
    }

    @Test
    public void conj84() throws Exception {
        requireMinimumVersion(5, 0);
        Statement stmt = sharedConnection.createStatement();

        stmt.execute("INSERT INTO t1 VALUES (1, 'foo')");
        stmt.execute("INSERT INTO t2 VALUES (2, 'bar')");
        ResultSet rs = sharedConnection.createStatement().executeQuery(
                "select t1.*, t2.* FROM t1 join t2");
        rs.next();
        assertEquals(rs.findColumn("id"), 1);
        assertEquals(rs.findColumn("name"), 2);
        assertEquals(rs.findColumn("t1.id"), 1);
        assertEquals(rs.findColumn("t1.name"), 2);
        assertEquals(rs.findColumn("t2.id"), 3);
        assertEquals(rs.findColumn("t2.name"), 4);
    }

    /*
     * CONJ-149: ResultSetMetaData.getTableName returns table alias instead of real table name
     *
     * @throws SQLException
     */
    @Test
    public void tableNameTest() throws Exception {
        ResultSet rs = sharedConnection.createStatement().executeQuery(
                "SELECT id AS id_alias FROM t3 AS t1_alias");
        ResultSetMetaData rsmd = rs.getMetaData();

        // this should return the original name of the table, not the alias
        logInfo(rsmd.getTableName(1));
        assertEquals(rsmd.getTableName(1), "t3");

        assertEquals(rsmd.getColumnLabel(1), "id_alias");
        assertEquals(rsmd.getColumnName(1), "id");

        // add useOldAliasMetadataBehavior to get the alias instead of the real
        // table name
        Connection connection = null;
        try {
            connection = setConnection("&useOldAliasMetadataBehavior=true");
            rs = connection.createStatement().executeQuery(
                    "SELECT id AS id_alias FROM t3 AS t1_alias");
            rsmd = rs.getMetaData();

            // this should return the alias name of the table, i.e. old behavior
            logInfo(rsmd.getTableName(1));
            assertEquals(rsmd.getTableName(1), "t1_alias");
            assertEquals(rsmd.getColumnLabel(1), "id_alias");
            assertEquals(rsmd.getColumnName(1), "id_alias");
        } finally {
            connection.close();
        }
    }

}
