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


import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;


public class CheckDataTest extends BaseTest {

    @Test
    public void testStatementExecuteAutoincrement() throws SQLException {
        createTable("CheckDataTest1", "id int not null primary key auto_increment, test varchar(10)");
        Statement stmt = sharedConnection.createStatement();
        int insert = stmt.executeUpdate("INSERT INTO CheckDataTest1 (test) VALUES ('test1')", Statement.RETURN_GENERATED_KEYS);
        assertEquals(1, insert);

        setAutoInc();

        ResultSet rs = stmt.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(autoIncOffset + autoInc, rs.getInt(1));
        assertFalse(rs.next());

        rs = stmt.executeQuery("SELECT * FROM CheckDataTest1");
        assertTrue(rs.next());
        assertEquals(autoIncOffset + autoInc, rs.getInt(1));
        assertEquals("test1", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testStatementBatch() throws SQLException {
        createTable("CheckDataTest2", "id int not null primary key auto_increment, test varchar(10)");
        Statement stmt = sharedConnection.createStatement();
        stmt.addBatch("INSERT INTO CheckDataTest2 (id, test) VALUES (2, 'test1')");
        stmt.addBatch("INSERT INTO CheckDataTest2 (test) VALUES ('test2')");
        stmt.addBatch("UPDATE CheckDataTest2 set test = CONCAT(test, 'tt')");
        stmt.addBatch("INSERT INTO CheckDataTest2 (id, test) VALUES (9, 'test3')");
        int[] res = stmt.executeBatch();

        assertEquals(4, res.length);
        assertEquals(1, res[0]);
        assertEquals(1, res[1]);
        assertEquals(2, res[2]);
        assertEquals(1, res[3]);

        setAutoInc();

        ResultSet rs = stmt.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2 + autoIncOffset + autoInc, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(9, rs.getInt(1));
        assertFalse(rs.next());

        rs = stmt.executeQuery("SELECT * FROM CheckDataTest2");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("test1tt", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2 + autoIncOffset + autoInc, rs.getInt(1));
        assertEquals("test2tt", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(9, rs.getInt(1));
        assertEquals("test3", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testPrepareStatementExecuteAutoincrement() throws SQLException {
        createTable("CheckDataTest3", "id int not null primary key auto_increment, test varchar(10)");
        PreparedStatement stmt = sharedConnection.prepareStatement("INSERT INTO CheckDataTest3 (test) VALUES (?)",
                Statement.RETURN_GENERATED_KEYS);
        stmt.setString(1, "test1");

        setAutoInc();

        int insert = stmt.executeUpdate();
        assertEquals(1, insert);

        ResultSet rs = stmt.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(autoIncOffset + autoInc, rs.getInt(1));
        assertFalse(rs.next());

        //without addBatch -> no execution
        int[] noBatch = stmt.executeBatch();
        assertEquals(0, noBatch.length);

        //with addBatch
        stmt.addBatch();
        int[] nbBatch = stmt.executeBatch();
        assertEquals(1, nbBatch.length);
        assertEquals(1, nbBatch[0]);

        rs = stmt.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(autoIncOffset + 2 * autoInc, rs.getInt(1));
        assertFalse(rs.next());

        rs = stmt.executeQuery("SELECT * FROM CheckDataTest3");
        assertTrue(rs.next());
        assertEquals(autoIncOffset + autoInc, rs.getInt(1));
        assertEquals("test1", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(autoIncOffset + 2 * autoInc, rs.getInt(1));
        assertEquals("test1", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testPrepareStatementBatch() throws SQLException {
        createTable("CheckDataTest4", "id int not null primary key auto_increment, test varchar(10)");
        PreparedStatement stmt = sharedConnection.prepareStatement("INSERT INTO CheckDataTest4 (test) VALUES (?)",
                Statement.RETURN_GENERATED_KEYS);
        stmt.setString(1, "test1");
        stmt.addBatch();
        stmt.setString(1, "test2");
        stmt.addBatch();
        stmt.addBatch();

        int[] res = stmt.executeBatch();

        assertEquals(3, res.length);
        assertEquals(1, res[0]);
        assertEquals(1, res[1]);
        assertEquals(1, res[2]);

        setAutoInc();

        ResultSet rs = stmt.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(autoIncOffset + autoInc, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(autoIncOffset + 2 * autoInc, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(autoIncOffset + 3 * autoInc, rs.getInt(1));
        assertFalse(rs.next());

        rs = stmt.executeQuery("SELECT * FROM CheckDataTest4");
        assertTrue(rs.next());
        assertEquals(autoIncOffset + autoInc, rs.getInt(1));
        assertEquals("test1", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(autoIncOffset + 2 * autoInc, rs.getInt(1));
        assertEquals("test2", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(autoIncOffset + 3 * autoInc, rs.getInt(1));
        assertEquals("test2", rs.getString(2));
        assertFalse(rs.next());
    }
}
