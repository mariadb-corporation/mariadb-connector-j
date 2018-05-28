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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.*;

public class ReconnectionStateMaxAllowedStatement extends BaseTest {

    @Test
    public void isolationLevelResets() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection();
            long max = maxPacket(connection);
            if (max > Integer.MAX_VALUE - 10) {
                fail("max_allowed_packet too high for this test");
            }
            connection.prepareStatement("create table if not exists foo (x longblob)").execute();
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

            assertEquals("READ-UNCOMMITTED", level((MariaDbConnection) connection));
            PreparedStatement st = connection.prepareStatement("insert into foo (?)");
            try {
                st.setBytes(1, data((int) (max + 10)));
                st.execute();
                fail();
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("max_allowed_packet"));
                // we still have a working connection
                assertTrue(connection.isValid(0));
                // our isolation level must have stay the same
                assertEquals("READ-UNCOMMITTED", level((MariaDbConnection) connection));
            }
        } finally {
            if (connection != null) connection.close();
        }
    }

    private String level(MariaDbConnection connection) throws SQLException {
        String sql = "SELECT @@tx_isolation";
        if (!connection.isServerMariaDb() && connection.versionGreaterOrEqual(8,0,3)) {
            sql = "SELECT @@transaction_isolation";
        }
        ResultSet rs = connection.prepareStatement(sql).executeQuery();
        assertTrue(rs.next());
        return rs.getString(1);

    }

    private long maxPacket(Connection connection) throws SQLException {
        ResultSet rs = connection.prepareStatement("select @@max_allowed_packet").executeQuery();
        assertTrue(rs.next());
        return rs.getLong(1);
    }

    private byte[] data(int size) {
        byte[] data = new byte[size];
        Arrays.fill(data, (byte) 'a');
        return data;
    }
}
