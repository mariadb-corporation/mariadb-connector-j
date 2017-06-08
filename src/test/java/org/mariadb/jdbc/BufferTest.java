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


import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;


public class BufferTest extends BaseTest {

    static char[] array8m;
    static char[] array20m;
    static char[] array40m;

    static {
        array8m = new char[8000000];
        for (int i = 0; i < array8m.length; i++) {
            array8m[i] = (char) (0x30 + (i % 10));
        }
        array20m = new char[20000000];
        for (int i = 0; i < array20m.length; i++) {
            array20m[i] = (char) (0x30 + (i % 10));
        }
        array40m = new char[40000000];
        for (int i = 0; i < array40m.length; i++) {
            array40m[i] = (char) (0x30 + (i % 10));
        }
    }

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("BufferTest", "test longText");
    }

    @Test
    public void send8mTextData() throws SQLException {
        Assume.assumeTrue(checkMaxAllowedPacketMore8m("send8mTextData"));
        sendSqlData(false, array8m);
        sendSqlData(true, array8m);
    }

    @Test
    public void send20mTextData() throws SQLException {
        Assume.assumeTrue(checkMaxAllowedPacketMore20m("send20mTextData"));
        sendSqlData(false, array20m);
        sendSqlData(true, array20m);
    }

    @Test
    public void send40mTextData() throws SQLException {
        Assume.assumeTrue(checkMaxAllowedPacketMore40m("send40mTextData"));
        sendSqlData(false, array40m);
        sendSqlData(true, array40m);
    }

    @Test
    public void send8mByteBufferData() throws SQLException {
        Assume.assumeTrue(checkMaxAllowedPacketMore8m("send8mByteBufferData"));
        sendByteBufferData(false, array8m);
        sendByteBufferData(true, array8m);
    }

    @Test
    public void send20mByteBufferData() throws SQLException {
        Assume.assumeTrue(checkMaxAllowedPacketMore20m("send20mByteBufferData"));
        sendByteBufferData(false, array20m);
        sendByteBufferData(true, array20m);
    }

    @Test
    public void send40mByteBufferData() throws SQLException {
        Assume.assumeTrue(checkMaxAllowedPacketMore40m("send40mByteBufferData"));
        sendByteBufferData(false, array40m);
        sendByteBufferData(true, array40m);
    }


    @Test
    public void send20mSqlNotCompressDataException() throws SQLException {
        try {
            Assume.assumeTrue(!checkMaxAllowedPacketMore20m("send20mSqlNotCompressDataException", false));
            sendSqlData(false, array20m);
            fail("must have thrown exception");
        } catch (SQLException sqlexception) {
            assertTrue("not the expected exception. was " + sqlexception.getMessage(),
                    sqlexception.getMessage().contains("is >= to max_allowed_packet"));
        }
    }

    @Test
    public void send20mSqlCompressDataException() throws SQLException {
        try {
            Assume.assumeTrue(!checkMaxAllowedPacketMore20m("send20mSqlCompressDataException", false));
            sendSqlData(true, array20m);
            fail("must have thrown exception");
        } catch (SQLException sqlexception) {
            assertTrue("not the expected exception. was " + sqlexception.getMessage(),
                    sqlexception.getMessage().contains("is >= to max_allowed_packet"));
        }
    }

    @Test
    public void send40mSqlNotCompressDataException() throws SQLException {
        try {
            Assume.assumeTrue(!checkMaxAllowedPacketMore40m("send40mSqlNotCompressDataException", false));
            sendSqlData(false, array40m);
            fail("must have thrown exception");
        } catch (SQLException sqlexception) {
            assertTrue("not the expected exception. was " + sqlexception.getMessage(),
                    sqlexception.getMessage().contains("is >= to max_allowed_packet"));
        }
    }

    @Test
    public void send40mSqlCompressDataException() throws SQLException {
        try {
            Assume.assumeTrue(!checkMaxAllowedPacketMore40m("send40mSqlCompressDataException", false));
            sendSqlData(true, array40m);
            fail("must have thrown exception");
        } catch (SQLException sqlexception) {
            assertTrue("not the expected exception. was " + sqlexception.getMessage(),
                    sqlexception.getMessage().contains("is >= to max_allowed_packet"));
        }
    }


    @Test
    public void send20mByteBufferNotCompressDataException() throws SQLException {
        try {
            Assume.assumeTrue(!checkMaxAllowedPacketMore20m("send20mByteBufferNotCompressDataException", false));
            sendByteBufferData(false, array20m);
            fail("must have thrown exception");
        } catch (SQLException sqlexception) {
            assertTrue("not the expected exception. was " + sqlexception.getCause().getCause().getMessage(),
                    sqlexception.getCause().getMessage().contains("is >= to max_allowed_packet"));
        }
    }

    @Test
    public void send20mByteBufferCompressDataException() throws SQLException {
        try {
            Assume.assumeTrue(!checkMaxAllowedPacketMore20m("send20mByteBufferCompressDataException", false));
            sendByteBufferData(true, array20m);
            fail("must have thrown exception");
        } catch (SQLException sqlexception) {
            assertTrue("not the expected exception. was " + sqlexception.getCause().getCause().getMessage(),
                    sqlexception.getCause().getMessage().contains("is >= to max_allowed_packet"));
        }
    }

    @Test
    public void send40mByteBufferNotCompressDataException() throws SQLException {
        try {
            Assume.assumeTrue(!checkMaxAllowedPacketMore40m("send40mByteBufferNotCompressDataException", false));
            sendByteBufferData(false, array40m);
            fail("must have thrown exception");
        } catch (SQLException sqlexception) {
            assertTrue("not the expected exception. was " + sqlexception.getCause().getCause().getMessage(),
                    sqlexception.getCause().getMessage().contains("is >= to max_allowed_packet"));
        }
    }

    @Test
    public void send40mByteBufferCompressDataException() throws SQLException {
        try {
            Assume.assumeTrue(!checkMaxAllowedPacketMore40m("send40mByteBufferCompressDataException", false));
            sendByteBufferData(true, array40m);
            fail("must have thrown exception");
        } catch (SQLException sqlexception) {
            assertTrue("not the expected exception. was " + sqlexception.getCause().getCause().getMessage(),
                    sqlexception.getCause().getMessage().contains("is >= to max_allowed_packet"));
        }
    }

    /**
     * Insert data using bytebuffer implementation on PacketOutputStream.
     *
     * @param compression use packet compression
     * @param arr         data to insert
     * @throws SQLException if anything wrong append
     */
    private void sendByteBufferData(boolean compression, char[] arr) throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection("&useCompression=" + compression);
            Statement stmt = connection.createStatement();
            stmt.execute("TRUNCATE BufferTest");
            PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO BufferTest VALUES (?)");
            preparedStatement.setString(1, new String(arr));
            preparedStatement.execute();
            checkResult(arr);
        } finally {
            connection.close();
        }
    }

    /**
     * Insert data using sql buffer implementation on PacketOutputStream.
     *
     * @param compression use packet compression
     * @param arr         data to insert
     * @throws SQLException if anything wrong append
     */
    private void sendSqlData(boolean compression, char[] arr) throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection("&useCompression=" + compression);
            Statement stmt = connection.createStatement();
            stmt.execute("TRUNCATE BufferTest");
            stmt.execute("INSERT INTO BufferTest VALUES ('" + new String(arr) + "')");
            checkResult(arr);
        } finally {
            connection.close();
        }
    }

    private void checkResult(char[] arr) throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM BufferTest");
        if (rs.next()) {
            String resString = rs.getString(1);
            char[] cc = resString.toCharArray();
            assertEquals("error in data : length not equal", cc.length, arr.length);
            assertEquals(String.valueOf(cc), resString);

        } else {
            fail("Error, must have result");
        }
    }

}
